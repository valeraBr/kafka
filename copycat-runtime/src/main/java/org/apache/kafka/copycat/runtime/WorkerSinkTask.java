/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.runtime;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.copycat.cli.WorkerConfig;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.errors.CopycatRuntimeException;
import org.apache.kafka.copycat.sink.SinkRecord;
import org.apache.kafka.copycat.sink.SinkTask;
import org.apache.kafka.copycat.sink.SinkTaskContext;
import org.apache.kafka.copycat.storage.Converter;
import org.apache.kafka.copycat.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * WorkerTask that uses a SinkTask to export data from Kafka.
 */
public class WorkerSinkTask implements WorkerTask {
    private static final Logger log = LoggerFactory.getLogger(WorkerSinkTask.class);

    private final ConnectorTaskId id;
    private final SinkTask task;
    private final WorkerConfig workerConfig;
    private final Time time;
    private final Converter converter;
    private WorkerSinkTaskThread workThread;
    private KafkaConsumer<Object, Object> consumer;
    private final SinkTaskContext context;

    public WorkerSinkTask(ConnectorTaskId id, SinkTask task, WorkerConfig workerConfig,
                          Converter converter, Time time) {
        this.id = id;
        this.task = task;
        this.workerConfig = workerConfig;
        this.converter = converter;
        context = new SinkTaskContextImpl();
        this.time = time;
    }

    @Override
    public void start(Properties props) {
        task.initialize(context);
        task.start(props);
        consumer = createConsumer(props);
        workThread = createWorkerThread();
        workThread.start();
    }

    @Override
    public void stop() throws CopycatException {
        // Offset commit is handled upon exit in work thread
        task.stop();
        if (workThread != null)
            workThread.startGracefulShutdown();
        consumer.wakeup();
    }

    @Override
    public boolean awaitStop(long timeoutMs) {
        if (workThread != null) {
            try {
                boolean success = workThread.awaitShutdown(timeoutMs, TimeUnit.MILLISECONDS);
                if (!success)
                    workThread.forceShutdown();
                return success;
            } catch (InterruptedException e) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        // FIXME Kafka needs to add a timeout parameter here for us to properly obey the timeout
        // passed in
        if (consumer != null)
            consumer.close();
    }

    /** Poll for new messages with the given timeout. Should only be invoked by the worker thread. */
    public void poll(long timeoutMs) {
        try {
            log.trace("{} polling consumer with timeout {} ms", id, timeoutMs);
            ConsumerRecords<Object, Object> msgs = consumer.poll(timeoutMs);
            log.trace("{} polling returned {} messages", id, msgs.count());
            deliverMessages(msgs);
        } catch (ConsumerWakeupException we) {
            log.trace("{} consumer woken up", id);
        }
    }

    /**
     * Starts an offset commit by flushing outstanding messages from the task and then starting
     * the write commit. This should only be invoked by the WorkerSinkTaskThread.
     **/
    public void commitOffsets(long now, boolean sync, final int seqno, boolean flush) {
        HashMap<TopicPartition, Long> offsets = new HashMap<>();
        for (TopicPartition tp : consumer.subscriptions()) {
            offsets.put(tp, consumer.position(tp));
        }
        // We only don't flush the task in one case: when shutting down, the task has already been
        // stopped and all data should have already been flushed
        if (flush) {
            try {
                task.flush(offsets);
            } catch (Throwable t) {
                log.error("Commit of {} offsets failed due to exception while flushing: {}", this, t);
                workThread.onCommitCompleted(t, seqno);
                return;
            }
        }

        ConsumerCommitCallback cb = new ConsumerCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, Long> offsets, Exception error) {
                workThread.onCommitCompleted(error, seqno);
            }
        };
        consumer.commit(offsets, sync ? CommitType.SYNC : CommitType.ASYNC, cb);
    }

    public Time getTime() {
        return time;
    }

    public WorkerConfig getWorkerConfig() {
        return workerConfig;
    }

    private KafkaConsumer<Object, Object> createConsumer(Properties taskProps) {
        String topicsStr = taskProps.getProperty(SinkTask.TOPICS_CONFIG);
        if (topicsStr == null || topicsStr.isEmpty())
            throw new CopycatRuntimeException("Sink tasks require a list of topics.");
        String[] topics = topicsStr.split(",");

        // Include any unknown worker configs so consumer configs can be set globally on the worker
        // and through to the task
        Properties props = workerConfig.getUnusedProperties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "copycat-" + id.toString());
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                Utils.join(workerConfig.getList(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG), ","));
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                workerConfig.getClass(WorkerConfig.KEY_DESERIALIZER_CLASS_CONFIG).getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                workerConfig.getClass(WorkerConfig.VALUE_DESERIALIZER_CLASS_CONFIG).getName());

        KafkaConsumer<Object, Object> newConsumer;
        try {
            newConsumer = new KafkaConsumer<>(props);
        } catch (Throwable t) {
            throw new CopycatRuntimeException("Failed to create consumer", t);
        }

        log.debug("Task {} subscribing to topics {}", id, topics);
        newConsumer.subscribe(topics);

        // Seek to any user-provided offsets. This is useful if offsets are tracked in the downstream system (e.g., to
        // enable exactly once delivery to that system).
        //
        // To do this correctly, we need to first make sure we have been assigned partitions, which poll() will guarantee.
        // We ask for offsets after this poll to make sure any offsets committed before the rebalance are picked up correctly.
        newConsumer.poll(0);
        Map<TopicPartition, Long> offsets = context.getOffsets();
        for (TopicPartition tp : newConsumer.subscriptions()) {
            Long offset = offsets.get(tp);
            if (offset != null)
                newConsumer.seek(tp, offset);
        }
        return newConsumer;
    }

    private WorkerSinkTaskThread createWorkerThread() {
        return new WorkerSinkTaskThread(this, "WorkerSinkTask-" + id, time, workerConfig);
    }

    private void deliverMessages(ConsumerRecords<Object, Object> msgs) {
        // Finally, deliver this batch to the sink
        if (msgs.count() > 0) {
            List<SinkRecord> records = new ArrayList<>();
            for (ConsumerRecord<Object, Object> msg : msgs) {
                log.trace("Consuming message with key {}, value {}", msg.key(), msg.value());
                records.add(
                        new SinkRecord(msg.topic(), msg.partition(),
                                converter.toCopycatData(msg.key()),
                                converter.toCopycatData(msg.value()),
                                msg.offset())
                );
            }

            try {
                task.put(records);
            } catch (CopycatException e) {
                log.error("Exception from SinkTask {}: ", id, e);
            } catch (Throwable t) {
                log.error("Unexpected exception from SinkTask {}: ", id, t);
            }
        }
    }
}
