/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.Version;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Collections.singleton;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

/**
 * A StreamTask is associated with a {@link PartitionGroup}, and is assigned to a StreamThread for processing.
 */
public class StreamTask extends AbstractTask implements ProcessorNodePunctuator, Task {

    private static final ConsumerRecord<Object, Object> DUMMY_RECORD = new ConsumerRecord<>(ProcessorContextImpl.NONEXIST_TOPIC, -1, -1L, null, null);
    // visible for testing
    static final byte LATEST_MAGIC_BYTE = 1;

    private final TaskId id;
    private final ProcessorTopology topology;
    private final ProcessorStateManager stateMgr;
    private final Set<TopicPartition> partitions;
    private final Consumer<byte[], byte[]> consumer;
    private final String logPrefix;
    private final Logger log;
    private final StateDirectory stateDirectory;

    private final Time time;
    private final String threadId;

    // we want to abstract eos logic out of StreamTask, however
    // there's still an optimization that requires this info to be
    // leaked into this class, which is to checkpoint after committing if EOS is not enabled.
    private final boolean eosDisabled;

    private final long maxTaskIdleMs;
    private final int maxBufferedSize;
    private final StreamsMetricsImpl streamsMetrics;
    private final PartitionGroup partitionGroup;
    private final RecordCollector recordCollector;
    private final PartitionGroup.RecordInfo recordInfo;
    private final Map<TopicPartition, Long> consumedOffsets;
    private final PunctuationQueue streamTimePunctuationQueue;
    private final PunctuationQueue systemTimePunctuationQueue;

    private final Sensor closeTaskSensor;
    private final Sensor processLatencySensor;
    private final Sensor punctuateLatencySensor;
    private final Sensor commitSensor;
    private final Sensor enforcedProcessingSensor;
    private final InternalProcessorContext processorContext;

    private long idleStartTime;
    private boolean commitNeeded = false;
    private boolean commitRequested = false;

    public StreamTask(final TaskId id,
                      final Set<TopicPartition> partitions,
                      final ProcessorTopology topology,
                      final Consumer<byte[], byte[]> consumer,
                      final StreamsConfig config,
                      final StreamsMetricsImpl streamsMetrics,
                      final StateDirectory stateDirectory,
                      final ThreadCache cache,
                      final Time time,
                      final ProcessorStateManager stateMgr,
                      final RecordCollector recordCollector) {
        this.id = id;
        this.partitions = new HashSet<>(partitions);
        this.topology = topology;
        this.consumer = consumer;
        this.stateDirectory = stateDirectory;

        this.stateMgr = stateMgr;

        final String threadIdPrefix = format("stream-thread [%s] ", Thread.currentThread().getName());
        logPrefix = threadIdPrefix + format("%s [%s] ", "task", id);
        final LogContext logContext = new LogContext(logPrefix);
        log = logContext.logger(getClass());

        this.time = time;
        this.streamsMetrics = streamsMetrics;
        this.recordCollector = recordCollector;
        eosDisabled = !StreamsConfig.EXACTLY_ONCE.equals(config.getString(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));

        threadId = Thread.currentThread().getName();
        closeTaskSensor = ThreadMetrics.closeTaskSensor(threadId, streamsMetrics);
        final String taskId = id.toString();
        if (streamsMetrics.version() == Version.FROM_0100_TO_24) {
            final Sensor parent = ThreadMetrics.commitOverTasksSensor(threadId, streamsMetrics);
            commitSensor = TaskMetrics.commitSensor(threadId, taskId, streamsMetrics, parent);
            enforcedProcessingSensor = TaskMetrics.enforcedProcessingSensor(threadId, taskId, streamsMetrics, parent);
        } else {
            commitSensor = TaskMetrics.commitSensor(threadId, taskId, streamsMetrics);
            enforcedProcessingSensor = TaskMetrics.enforcedProcessingSensor(threadId, taskId, streamsMetrics);
        }
        processLatencySensor = TaskMetrics.processLatencySensor(threadId, taskId, streamsMetrics);
        punctuateLatencySensor = TaskMetrics.punctuateSensor(threadId, taskId, streamsMetrics);

        streamTimePunctuationQueue = new PunctuationQueue();
        systemTimePunctuationQueue = new PunctuationQueue();
        maxTaskIdleMs = config.getLong(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
        maxBufferedSize = config.getInt(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG);

        // initialize the consumed and committed offset cache
        consumedOffsets = new HashMap<>();

        // create queues for each assigned partition and associate them
        // to corresponding source nodes in the processor topology
        final Map<TopicPartition, RecordQueue> partitionQueues = new HashMap<>();

        // initialize the topology with its own context
        processorContext = new ProcessorContextImpl(id, this, config, this.recordCollector, stateMgr, streamsMetrics, cache);

        final TimestampExtractor defaultTimestampExtractor = config.defaultTimestampExtractor();
        final DeserializationExceptionHandler defaultDeserializationExceptionHandler = config.defaultDeserializationExceptionHandler();
        for (final TopicPartition partition : partitions) {
            final SourceNode source = topology.source(partition.topic());
            final TimestampExtractor timestampExtractor = source.getTimestampExtractor();
            final TimestampExtractor sourceTimestampExtractor = timestampExtractor != null ? timestampExtractor : defaultTimestampExtractor;
            final RecordQueue queue = new RecordQueue(
                partition,
                source,
                sourceTimestampExtractor,
                defaultDeserializationExceptionHandler,
                processorContext,
                logContext
            );
            partitionQueues.put(partition, queue);
        }

        recordInfo = new PartitionGroup.RecordInfo();
        partitionGroup = new PartitionGroup(partitionQueues,
                                            TaskMetrics.recordLatenessSensor(threadId, taskId, streamsMetrics));

        stateMgr.registerGlobalStateStores(topology.globalStateStores());
    }

    @Override
    public boolean isActive() {
        return true;
    }

    @Override
    public void initializeIfNeeded() {
        if (state() == State.CREATED) {
            initializeMetadata();
            initializeStateStores();
            transitionTo(State.RESTORING);
        }
    }

    @Override
    public void completeInitializationAfterRestore() {
        if (state() == State.RESTORING) {
            initTopology();
            processorContext.initialize();
            idleStartTime = RecordQueue.UNKNOWN;
            transitionTo(State.RUNNING);
        } else {
            throw new IllegalStateException("This method is only for transitioning from restoring to running. But was in state " + state());
        }
    }

    public void initializeMetadata() {
        try {
            final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = consumer.committed(partitions).entrySet().stream()
                                                                                      .filter(e -> e.getValue() != null)
                                                                                      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            initializeTaskTime(offsetsAndMetadata);
        } catch (final AuthorizationException e) {
            throw new ProcessorStateException(format("task [%s] AuthorizationException when initializing offsets for %s", id, partitions), e);
        } catch (final WakeupException e) {
            throw e;
        } catch (final KafkaException e) {
            throw new ProcessorStateException(format("task [%s] Failed to initialize offsets for %s", id, partitions), e);
        }
    }

    private void initializeTaskTime(final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata) {
        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetsAndMetadata.entrySet()) {
            final TopicPartition partition = entry.getKey();
            final OffsetAndMetadata metadata = entry.getValue();

            if (metadata != null) {
                final long committedTimestamp = decodeTimestamp(metadata.metadata());
                partitionGroup.setPartitionTime(partition, committedTimestamp);
                log.debug("A committed timestamp was detected: setting the partition time of partition {}"
                              + " to {} in stream task {}", partition, committedTimestamp, id);
            } else {
                log.debug("No committed timestamp was found in metadata for partition {}", partition);
            }
        }

        final Set<TopicPartition> nonCommitted = new HashSet<>(partitions);
        nonCommitted.removeAll(offsetsAndMetadata.keySet());
        for (final TopicPartition partition : nonCommitted) {
            log.debug("No committed offset for partition {}, therefore no timestamp can be found for this partition", partition);
        }
    }


    public void initializeStateStores() {
        TaskUtils.registerStateStores(topology, stateDirectory, id, logPrefix, log, processorContext, stateMgr);
    }

    /**
     * <pre>
     * - resume the task
     * </pre>
     */
    public void resume() {
        log.debug("Resuming");
        if (state() == State.SUSPENDED) {
            initializeMetadata();
            transitionTo(State.RUNNING);
        } else {
            throw new IllegalStateException("Expected only to resume from SUSPENDED state, but was " + state());
        }
    }

    /**
     * An active task is processable if its buffer contains data for all of its input
     * source topic partitions, or if it is enforced to be processable
     */
    private boolean isProcessable(final long wallClockTime) {
        if (partitionGroup.allPartitionsBuffered()) {
            idleStartTime = RecordQueue.UNKNOWN;
            return true;
        } else if (partitionGroup.numBuffered() > 0) {
            if (idleStartTime == RecordQueue.UNKNOWN) {
                idleStartTime = wallClockTime;
            }

            if (wallClockTime - idleStartTime >= maxTaskIdleMs) {
                enforcedProcessingSensor.record();
                return true;
            } else {
                return false;
            }
        } else {
            // there's no data in any of the topics; we should reset the enforced
            // processing timer
            idleStartTime = RecordQueue.UNKNOWN;
            return false;
        }
    }

    /**
     * Process one record.
     *
     * @return true if this method processes a record, false if it does not process a record.
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean process(final long wallClockTime) {
        if (!isProcessable(wallClockTime)) {
            return false;
        }

        // get the next record to process
        final StampedRecord record = partitionGroup.nextRecord(recordInfo);

        // if there is no record to process, return immediately
        if (record == null) {
            return false;
        }

        try {
            // process the record by passing to the source node of the topology
            final ProcessorNode currNode = recordInfo.node();
            final TopicPartition partition = recordInfo.partition();

            log.trace("Start processing one record [{}]", record);

            updateProcessorContext(record, currNode);
            maybeMeasureLatency(() -> currNode.process(record.key(), record.value()), time, processLatencySensor);

            log.trace("Completed processing one record [{}]", record);

            // update the consumed offset map after processing is done
            consumedOffsets.put(partition, record.offset());
            commitNeeded = true;

            // after processing this record, if its partition queue's buffered size has been
            // decreased to the threshold, we can then resume the consumption on this partition
            if (recordInfo.queue().size() == maxBufferedSize) {
                consumer.resume(singleton(partition));
            }
        } catch (final RecoverableClientException e) {
            throw new TaskMigratedException(id, e);
        } catch (final KafkaException e) {
            final String stackTrace = getStacktraceString(e);
            throw new StreamsException(format("Exception caught in process. taskId=%s, " +
                                                  "processor=%s, topic=%s, partition=%d, offset=%d, stacktrace=%s",
                                              id(),
                                              processorContext.currentNode().name(),
                                              record.topic(),
                                              record.partition(),
                                              record.offset(),
                                              stackTrace
            ), e);
        } finally {
            processorContext.setCurrentNode(null);
        }

        return true;
    }

    private String getStacktraceString(final KafkaException e) {
        String stacktrace = null;
        try (final StringWriter stringWriter = new StringWriter();
             final PrintWriter printWriter = new PrintWriter(stringWriter)) {
            e.printStackTrace(printWriter);
            stacktrace = stringWriter.toString();
        } catch (final IOException ioe) {
            log.error("Encountered error extracting stacktrace from this exception", ioe);
        }
        return stacktrace;
    }

    /**
     * @throws IllegalStateException if the current node is not null
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    @Override
    public void punctuate(final ProcessorNode node, final long timestamp, final PunctuationType type, final Punctuator punctuator) {
        if (processorContext.currentNode() != null) {
            throw new IllegalStateException(format("%sCurrent node is not null", logPrefix));
        }

        updateProcessorContext(new StampedRecord(DUMMY_RECORD, timestamp), node);

        if (log.isTraceEnabled()) {
            log.trace("Punctuating processor {} with timestamp {} and punctuation type {}", node.name(), timestamp, type);
        }

        try {
            maybeMeasureLatency(() -> node.punctuate(timestamp, punctuator), time, punctuateLatencySensor);
        } catch (final RecoverableClientException e) {
            throw new TaskMigratedException(id, e);
        } catch (final KafkaException e) {
            throw new StreamsException(format("%sException caught while punctuating processor '%s'", logPrefix, node.name()), e);
        } finally {
            processorContext.setCurrentNode(null);
        }
    }

    private void updateProcessorContext(final StampedRecord record, final ProcessorNode currNode) {
        processorContext.setRecordContext(
            new ProcessorRecordContext(
                record.timestamp,
                record.offset(),
                record.partition(),
                record.topic(),
                record.headers()));
        processorContext.setCurrentNode(currNode);
    }

    private Map<TopicPartition, Long> extractPartitionTimes() {
        final Map<TopicPartition, Long> partitionTimes = new HashMap<>();
        for (final TopicPartition partition : partitionGroup.partitions()) {
            partitionTimes.put(partition, partitionGroup.partitionTimestamp(partition));
        }
        return partitionTimes;
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    @Override
    public void commit() {
        log.debug("Committing");

        commitState();

        // this is an optimization for non-EOS only
        if (eosDisabled) {
            stateMgr.checkpoint(checkpointableOffsets());
        }
    }

    /**
     * <pre>
     * the following order must be followed:
     *  1. flush the state, send any left changelog records
     *  2. then flush the record collector
     *  3. then commit the record collector -- for EOS this is the synchronization barrier
     * </pre>
     *
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    private void commitState() {
        final long startNs = time.nanoseconds();

        stateMgr.flush();

        recordCollector.flush();

        // we need to preserve the original partitions times before calling commit
        // because all partition times are reset to -1 during close
        final Map<TopicPartition, Long> partitionTimes = extractPartitionTimes();
        final Map<TopicPartition, OffsetAndMetadata> consumedOffsetsAndMetadata = new HashMap<>(consumedOffsets.size());
        for (final Map.Entry<TopicPartition, Long> entry : consumedOffsets.entrySet()) {
            final TopicPartition partition = entry.getKey();
            final long offset = entry.getValue() + 1L;
            final long partitionTime = partitionTimes.get(partition);
            consumedOffsetsAndMetadata.put(partition, new OffsetAndMetadata(offset, encodeTimestamp(partitionTime)));
        }
        recordCollector.commit(consumedOffsetsAndMetadata);

        commitNeeded = false;
        commitRequested = false;
        commitSensor.record(time.nanoseconds() - startNs);
    }

    /**
     * Return all the checkpointable offsets(written + consumed) to the state manager.
     * Currently only changelog topic offsets need to be checkpointed.
     */
    private Map<TopicPartition, Long> checkpointableOffsets() {
        final Map<TopicPartition, Long> checkpointableOffsets = new HashMap<>(recordCollector.offsets());
        for (final Map.Entry<TopicPartition, Long> entry : consumedOffsets.entrySet()) {
            checkpointableOffsets.putIfAbsent(entry.getKey(), entry.getValue());
        }
        return checkpointableOffsets;
    }

    @Override
    public Map<TopicPartition, Long> purgableOffsets() {
        final Map<TopicPartition, Long> purgableConsumedOffsets = new HashMap<>();
        for (final Map.Entry<TopicPartition, Long> entry : consumedOffsets.entrySet()) {
            final TopicPartition tp = entry.getKey();
            if (topology.isRepartitionTopic(tp.topic())) {
                purgableConsumedOffsets.put(tp, entry.getValue() + 1);
            }
        }

        return purgableConsumedOffsets;
    }

    private void initTopology() {
        // initialize the task by initializing all its processor nodes in the topology
        log.trace("Initializing processor nodes of the topology");
        for (final ProcessorNode node : topology.processors()) {
            processorContext.setCurrentNode(node);
            try {
                node.init(processorContext);
            } finally {
                processorContext.setCurrentNode(null);
            }
        }
    }

    /**
     * <pre>
     * - close topology
     * - {@link #commit()}
     *   - flush state and producer
     *   - if (!eos) write checkpoint
     *   - commit offsets
     * </pre>
     *
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    public void suspend() {
        log.debug("Suspending");
        if (state() == State.SUSPENDED) {
            return;
        } else {
            suspend(true);
            transitionTo(State.SUSPENDED);
        }
    }

    /**
     * <pre>
     * the following order must be followed:
     *  1. first close topology to make sure all cached records in the topology are processed
     *  2. then flush the state, send any left changelog records
     *  3. then flush the record collector
     *  4. then commit the record collector -- for EOS this is the synchronization barrier
     *  5. then checkpoint the state manager -- even if we crash before this step, EOS is still guaranteed
     * </pre>
     *
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    private void suspend(final boolean clean) {
        try {
            // If the suspension is from unclean shutdown, then only need to close topology and flush state to make sure that when later
            // closing the states, there's no records triggering any processing anymore; also swallow all caught exceptions
            closeTopology(clean);

            if (clean) {
                commitState();
                // whenever we have successfully committed state during suspension, it is safe to checkpoint
                // the state as well no matter if EOS is enabled or not
                stateMgr.checkpoint(checkpointableOffsets());
            } else {
                stateMgr.flush();
            }
        } catch (final RuntimeException error) {
            if (clean) {
                throw error;
            }
        }

        // we should also clear any buffered records of a task when suspending it
        partitionGroup.clear();
    }

    private void closeTopology(final boolean clean) {
        log.trace("Closing processor topology");

        // close the processors
        // make sure close() is called for each node even when there is a RuntimeException
        RuntimeException exception = null;
        if (state().hasBeenRunning()) {
            for (final ProcessorNode node : topology.processors()) {
                processorContext.setCurrentNode(node);
                try {
                    node.close();
                } catch (final RuntimeException e) {
                    exception = e;
                } finally {
                    processorContext.setCurrentNode(null);
                }
            }
        }

        if (exception != null && clean) {
            throw exception;
        }
    }

    @Override
    public void closeClean() {
        close(true);
    }

    @Override
    public void closeDirty() {
        close(false);
    }

    /**
     * <pre>
     * the following order must be followed:
     *  1. first close topology to make sure all cached records in the topology are processed
     *  2. then flush the state, send any left changelog records
     *  3. then flush the record collector
     *  4. then commit the record collector -- for EOS this is the synchronization barrier
     *  5. then checkpoint the state manager -- even if we crash before this step, EOS is still guaranteed
     * </pre>
     *
     * @param clean    shut down cleanly (ie, incl. flush and commit) if {@code true} --
     *                 otherwise, just close open resources
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    private void close(final boolean clean) {
        log.debug("Closing");
        // Once we start closing, we have to complete it.
        transitionTo(State.CLOSING);

        try {

            closeTopology(clean);

            // If from unclean shutdown, then only need to close topology and flush state to make sure that when later
            // closing the states, there's no records triggering any processing anymore; also swallow all caught exceptions
            // However, for a _clean_ shutdown, we try to commit and checkpoint. If there are any exceptions, they become
            // fatal for the "closeClean()" call, and the caller can try again with closeDirty() to complete the shutdown.
            if (clean) {
                commitState();
                // whenever we have successfully committed state during suspension, it is safe to checkpoint
                // the state as well no matter if EOS is enabled or not
                stateMgr.checkpoint(checkpointableOffsets());
            } else {
                try {
                    stateMgr.flush();
                } catch (final RuntimeException error) {
                    log.debug("Ignoring flush error in unclean close.", error);
                }
            }

            // we should also clear any buffered records of a task when suspending it
            partitionGroup.clear();

            TaskUtils.closeStateManager(log, logPrefix, stateMgr, stateDirectory, id);
        } finally {
            partitionGroup.close();
            closeTaskSensor.record();
            streamsMetrics.removeAllTaskLevelSensors(threadId, id.toString());

            // this is last because it might throw
            closeRecordCollector(clean);
        }
        transitionTo(State.CLOSED);
    }

    private void closeRecordCollector(final boolean clean) {
        try {
            recordCollector.close();
        } catch (final RuntimeException e) {
            if (clean) {
                throw e;
            }
        }
    }

    /**
     * Adds records to queues. If a record has an invalid (i.e., negative) timestamp, the record is skipped
     * and not added to the queue for processing
     *
     * @param partition the partition
     * @param records   the records
     */
    public void addRecords(final TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> records) {
        final int newQueueSize = partitionGroup.addRawRecords(partition, records);

        if (log.isTraceEnabled()) {
            log.trace("Added records into the buffered queue of partition {}, new queue size is {}", partition, newQueueSize);
        }

        // if after adding these records, its partition queue's buffered size has been
        // increased beyond the threshold, we can then pause the consumption for this partition
        if (newQueueSize > maxBufferedSize) {
            consumer.pause(singleton(partition));
        }
    }

    /**
     * Schedules a punctuation for the processor
     *
     * @param interval the interval in milliseconds
     * @param type     the punctuation type
     * @throws IllegalStateException if the current node is not null
     */
    public Cancellable schedule(final long interval, final PunctuationType type, final Punctuator punctuator) {
        switch (type) {
            case STREAM_TIME:
                // align punctuation to 0L, punctuate as soon as we have data
                return schedule(0L, interval, type, punctuator);
            case WALL_CLOCK_TIME:
                // align punctuation to now, punctuate after interval has elapsed
                return schedule(time.milliseconds() + interval, interval, type, punctuator);
            default:
                throw new IllegalArgumentException("Unrecognized PunctuationType: " + type);
        }
    }

    /**
     * Schedules a punctuation for the processor
     *
     * @param startTime time of the first punctuation
     * @param interval  the interval in milliseconds
     * @param type      the punctuation type
     * @throws IllegalStateException if the current node is not null
     */
    private Cancellable schedule(final long startTime, final long interval, final PunctuationType type, final Punctuator punctuator) {
        if (processorContext.currentNode() == null) {
            throw new IllegalStateException(format("%sCurrent node is null", logPrefix));
        }

        final PunctuationSchedule schedule = new PunctuationSchedule(processorContext.currentNode(), startTime, interval, punctuator);

        switch (type) {
            case STREAM_TIME:
                // STREAM_TIME punctuation is data driven, will first punctuate as soon as stream-time is known and >= time,
                // stream-time is known when we have received at least one record from each input topic
                return streamTimePunctuationQueue.schedule(schedule);
            case WALL_CLOCK_TIME:
                // WALL_CLOCK_TIME is driven by the wall clock time, will first punctuate when now >= time
                return systemTimePunctuationQueue.schedule(schedule);
            default:
                throw new IllegalArgumentException("Unrecognized PunctuationType: " + type);
        }
    }

    /**
     * @return The number of records left in the buffer of this task's partition group
     */
    int numBuffered() {
        return partitionGroup.numBuffered();
    }

    /**
     * Possibly trigger registered stream-time punctuation functions if
     * current partition group timestamp has reached the defined stamp
     * Note, this is only called in the presence of new records
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    public boolean maybePunctuateStreamTime() {
        final long streamTime = partitionGroup.streamTime();

        // if the timestamp is not known yet, meaning there is not enough data accumulated
        // to reason stream partition time, then skip.
        if (streamTime == RecordQueue.UNKNOWN) {
            return false;
        } else {
            final boolean punctuated = streamTimePunctuationQueue.mayPunctuate(streamTime, PunctuationType.STREAM_TIME, this);

            if (punctuated) {
                commitNeeded = true;
            }

            return punctuated;
        }
    }

    /**
     * Possibly trigger registered system-time punctuation functions if
     * current system timestamp has reached the defined stamp
     * Note, this is called irrespective of the presence of new records
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    public boolean maybePunctuateSystemTime() {
        final long systemTime = time.milliseconds();

        final boolean punctuated = systemTimePunctuationQueue.mayPunctuate(systemTime, PunctuationType.WALL_CLOCK_TIME, this);

        if (punctuated) {
            commitNeeded = true;
        }

        return punctuated;
    }

    /**
     * Request committing the current task's state
     */
    void requestCommit() {
        commitRequested = true;
    }

    /**
     * Whether or not a request has been made to commit the current state
     */
    @Override
    public boolean commitRequested() {
        return commitRequested;
    }

    static String encodeTimestamp(final long partitionTime) {
        final ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.put(LATEST_MAGIC_BYTE);
        buffer.putLong(partitionTime);
        return Base64.getEncoder().encodeToString(buffer.array());
    }

    long decodeTimestamp(final String encryptedString) {
        if (encryptedString.isEmpty()) {
            return RecordQueue.UNKNOWN;
        }
        final ByteBuffer buffer = ByteBuffer.wrap(Base64.getDecoder().decode(encryptedString));
        final byte version = buffer.get();
        switch (version) {
            case LATEST_MAGIC_BYTE:
                return buffer.getLong();
            default:
                log.warn("Unsupported offset metadata version found. Supported version {}. Found version {}.",
                         LATEST_MAGIC_BYTE, version);
                return RecordQueue.UNKNOWN;
        }
    }

    RecordCollector recordCollector() {
        return recordCollector;
    }

    @Override
    public TaskId id() {
        return id;
    }

    @Override
    public Set<TopicPartition> inputPartitions() {
        return partitions;
    }

    public ProcessorContext context() {
        return processorContext;
    }

    @Override
    public StateStore getStore(final String name) {
        return stateMgr.getStore(name);
    }

    /**
     * Produces a string representation containing useful information about a Task.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the StreamTask instance.
     */
    @Override
    public String toString() {
        return toString("");
    }

    /**
     * Produces a string representation containing useful information about a Task starting with the given indent.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the Task instance.
     */
    public String toString(final String indent) {
        final StringBuilder sb = new StringBuilder();
        sb.append(indent);
        sb.append("TaskId: ");
        sb.append(id);
        sb.append("\n");

        // print topology
        if (topology != null) {
            sb.append(indent).append(topology.toString(indent + "\t"));
        }

        // print assigned partitions
        if (partitions != null && !partitions.isEmpty()) {
            sb.append(indent).append("Partitions [");
            for (final TopicPartition topicPartition : partitions) {
                sb.append(topicPartition).append(", ");
            }
            sb.setLength(sb.length() - 2);
            sb.append("]\n");
        }
        return sb.toString();
    }

    public boolean isClosed() {
        return state() == State.CLOSED;
    }

    public boolean commitNeeded() {
        return commitNeeded;
    }

    public Collection<TopicPartition> changelogPartitions() {
        return stateMgr.changelogPartitions();
    }

    public Map<TopicPartition, Long> changelogOffsets() {
        if (state() == State.RUNNING) {
            // if we are in running state, just return the latest offset sentinel indicating
            // we should be at the end of the changelog
            return changelogPartitions().stream()
                                        .collect(Collectors.toMap(Function.identity(), tp -> Task.LATEST_OFFSET));
        } else {
            return Collections.unmodifiableMap(stateMgr.changelogOffsets());
        }
    }

    // visible for testing
    InternalProcessorContext processorContext() {
        return processorContext;
    }
}
