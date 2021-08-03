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

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.TopicsInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptySet;

public class TopologyMetadata {
    private final Logger log = LoggerFactory.getLogger(TopologyMetadata.class);

    // the "__" (double underscore) string is not allowed for topology names, so it's safe to use to indicate
    // that it's not a named topology
    private static final String UNNAMED_TOPOLOGY = "__UNNAMED_TOPOLOGY__";
    private static final Pattern EMPTY_ZERO_LENGTH_PATTERN = Pattern.compile("");

    private final StreamsConfig config;
    private final TopologyVersion version;

    private final ConcurrentNavigableMap<String, InternalTopologyBuilder> builders; // Keep sorted by topology name for readability

    private ProcessorTopology globalTopology;
    private final Map<String, StateStore> globalStateStores = new HashMap<>();
    private final Set<String> allInputTopics = new HashSet<>();

    public static class TopologyVersion {
        public AtomicLong topologyVersion = new AtomicLong(0L); // the local topology version
        public Set<String> assignedNamedTopologies = new HashSet<>(); // the named topologies whose tasks are actively assigned
        public ReentrantLock topologyLock = new ReentrantLock();
        public Condition topologyCV = topologyLock.newCondition();
    }

    public TopologyMetadata(final InternalTopologyBuilder builder,
                            final StreamsConfig config) {
        version = new TopologyVersion();
        this.config = config;
        builders = new ConcurrentSkipListMap<>();
        if (builder.hasNamedTopology()) {
            builders.put(builder.topologyName(), builder);
        } else {
            builders.put(UNNAMED_TOPOLOGY, builder);
        }
    }

    public TopologyMetadata(final ConcurrentNavigableMap<String, InternalTopologyBuilder> builders,
                            final StreamsConfig config) {
        version = new TopologyVersion();
        this.config = config;

        this.builders = builders;
        if (builders.isEmpty()) {
            log.debug("Starting up empty KafkaStreams app with no topology");
        }
    }

    public void updateCurrentAssignmentTopology(final Set<String> assignedNamedTopologies) {
        version.assignedNamedTopologies = assignedNamedTopologies;
    }

    /**
     * @return the set of named topologies that the assignor distributed tasks for during the last rebalance
     */
    public Set<String> assignmentNamedTopologies() {
        return version.assignedNamedTopologies;
    }

    public long topologyVersion() {
        return version.topologyVersion.get();
    }

    public void lock() {
        version.topologyLock.lock();
    }

    public void unlock() {
        version.topologyLock.unlock();
    }

    public InternalTopologyBuilder getBuilderForTopologyName(final String name) {
        return builders.get(name);
    }

    /**
     * @throws IllegalStateException if the thread is not already holding the lock via TopologyMetadata#lock
     */
    public void maybeWaitForNonEmptyTopology() {
        if (!version.topologyLock.isHeldByCurrentThread()) {
            throw new IllegalStateException("Must call lock() before attempting to wait on non-empty topology");
        }
        while (isEmpty()) {
            try {
                log.debug("Detected that the topology is currently empty, going to wait for something to be added");
                version.topologyCV.await();
            } catch (final InterruptedException e) {
                log.debug("StreamThread was interrupted while waiting on empty topology", e);
            }
        }
    }

    public void registerAndBuildNewTopology(final InternalTopologyBuilder newTopologyBuilder) {
        try {
            lock();
            version.topologyVersion.incrementAndGet();
            log.info("Adding NamedTopology {}, latest topology version is {}", newTopologyBuilder.topologyName(), version.topologyVersion.get());
            builders.put(newTopologyBuilder.topologyName(), newTopologyBuilder);
            buildAndVerifyTopology(newTopologyBuilder);
            version.topologyCV.signalAll();
        } finally {
            unlock();
        }
    }

    /**
     * Removes the topology and blocks until all threads on the older version have ack'ed this removal.
     * IT is guaranteed that no more tasks from this removed topology will be processed
     */
    public void unregisterTopology(final String topologyName) {
        try {
            lock();
            version.topologyVersion.incrementAndGet();
            log.info("Removing NamedTopology {}, latest topology version is {}", topologyName, version.topologyVersion.get());
            final InternalTopologyBuilder removedBuilder = builders.remove(topologyName);
            removedBuilder.fullSourceTopicNames().forEach(allInputTopics::remove);
            removedBuilder.allSourcePatternStrings().forEach(allInputTopics::remove);
            version.topologyCV.signalAll();
        } finally {
            unlock();
        }
    }

    public void buildAndRewriteTopology() {
        applyToEachBuilder(this::buildAndVerifyTopology);
    }

    private void buildAndVerifyTopology(final InternalTopologyBuilder builder) {
        builder.rewriteTopology(config);
        builder.buildTopology();

        // As we go, check each topology for overlap in the set of input topics/patterns
        final int numInputTopics = allInputTopics.size();
        final List<String> inputTopics = builder.fullSourceTopicNames();
        final Collection<String> inputPatterns = builder.allSourcePatternStrings();

        final int numNewInputTopics = inputTopics.size() + inputPatterns.size();
        allInputTopics.addAll(inputTopics);
        allInputTopics.addAll(inputPatterns);
        if (allInputTopics.size() != numInputTopics + numNewInputTopics) {
            inputTopics.retainAll(allInputTopics);
            inputPatterns.retainAll(allInputTopics);
            inputTopics.addAll(inputPatterns);
            log.error("Tried to add the NamedTopology {} but it had overlap with other input topics: {}", builder.topologyName(), inputTopics);
            throw new TopologyException("Named Topologies may not subscribe to the same input topics or patterns");
        }

        final ProcessorTopology globalTopology = builder.buildGlobalStateTopology();
        if (globalTopology != null) {
            if (builder.topologyName() != null) {
                throw new IllegalStateException("Global state stores are not supported with Named Topologies");
            } else if (this.globalTopology == null) {
                this.globalTopology = globalTopology;
            } else {
                throw new IllegalStateException("Topology builder had global state, but global topology has already been set");
            }
        }
        globalStateStores.putAll(builder.globalStateStores());
    }

    public int getNumStreamThreads(final StreamsConfig config) {
        final int configuredNumStreamThreads = config.getInt(StreamsConfig.NUM_STREAM_THREADS_CONFIG);

        // If there are named topologies but some are empty, this indicates a bug in user code
        if (hasNamedTopologies()) {
            if (hasNoLocalTopology()) {
                log.error("Detected a named topology with no input topics, a named topology may not be empty.");
                throw new TopologyException("Topology has no stream threads and no global threads, " +
                                                "must subscribe to at least one source topic or pattern.");
            }
        } else {
            // If both the global and non-global topologies are empty, this indicates a bug in user code
            if (hasNoLocalTopology() && !hasGlobalTopology()) {
                log.error("Topology with no input topics will create no stream threads and no global thread.");
                throw new TopologyException("Topology has no stream threads and no global threads, " +
                                                "must subscribe to at least one source topic or global table.");
            }
        }

        // Lastly we check for an empty non-global topology and override the threads to zero if set otherwise
        if (configuredNumStreamThreads != 0 && hasNoLocalTopology()) {
            log.info("Overriding number of StreamThreads to zero for global-only topology");
            return 0;
        }

        return configuredNumStreamThreads;
    }

    /**
     * @return true iff the app is using named topologies, or was started up with no topology at all
     */
    public boolean hasNamedTopologies() {
        return !builders.containsKey(UNNAMED_TOPOLOGY);
    }

    Set<String> namedTopologiesView() {
        return hasNamedTopologies() ? Collections.unmodifiableSet(builders.keySet()) : emptySet();
    }

    /**
     * @return true iff any of the topologies have a global topology
     */
    public boolean hasGlobalTopology() {
        return evaluateConditionIsTrueForAnyBuilders(InternalTopologyBuilder::hasGlobalStores);
    }

    /**
     * @return true iff any of the topologies have no local (aka non-global) topology
     */
    public boolean hasNoLocalTopology() {
        return evaluateConditionIsTrueForAnyBuilders(InternalTopologyBuilder::hasNoLocalTopology);
    }

    public boolean hasPersistentStores() {
        // If the app is using named topologies, there may not be any persistent state when it first starts up
        // but a new NamedTopology may introduce it later, so we must return true
        if (hasNamedTopologies()) {
            return true;
        }
        return evaluateConditionIsTrueForAnyBuilders(InternalTopologyBuilder::hasPersistentStores);
    }

    public boolean hasStore(final String name) {
        return evaluateConditionIsTrueForAnyBuilders(b -> b.hasStore(name));
    }

    public boolean hasOffsetResetOverrides() {
        return evaluateConditionIsTrueForAnyBuilders(InternalTopologyBuilder::hasOffsetResetOverrides);
    }

    public OffsetResetStrategy offsetResetStrategy(final String topic) {
        for (final InternalTopologyBuilder builder : builders.values()) {
            final OffsetResetStrategy resetStrategy = builder.offsetResetStrategy(topic);
            if (resetStrategy != null) {
                return resetStrategy;
            }
        }
        return null;
    }

    Collection<String> sourceTopicCollection() {
        final List<String> sourceTopics = new ArrayList<>();
        applyToEachBuilder(b -> sourceTopics.addAll(b.sourceTopicCollection()));
        return sourceTopics;
    }

    Pattern sourceTopicPattern() {
        final StringBuilder patternBuilder = new StringBuilder();

        applyToEachBuilder(b -> {
            final String patternString = b.sourceTopicsPatternString();
            if (patternString.length() > 0) {
                patternBuilder.append(patternString).append("|");
            }
        });

        if (patternBuilder.length() > 0) {
            patternBuilder.setLength(patternBuilder.length() - 1);
            return Pattern.compile(patternBuilder.toString());
        } else {
            return EMPTY_ZERO_LENGTH_PATTERN;
        }
    }

    public boolean usesPatternSubscription() {
        return evaluateConditionIsTrueForAnyBuilders(InternalTopologyBuilder::usesPatternSubscription);
    }

    // Can be empty if app is started up with no Named Topologies, in order to add them on later
    public boolean isEmpty() {
        return builders.isEmpty();
    }

    public String topologyDescriptionString() {
        if (isEmpty()) {
            return "";
        }
        final StringBuilder sb = new StringBuilder();

        applyToEachBuilder(b -> {
            sb.append(b.describe().toString());
        });

        return sb.toString();
    }

    /**
     * @return the subtopology built for this task, or null if the corresponding NamedTopology does not (yet) exist
     */
    public ProcessorTopology buildSubtopology(final TaskId task) {
        final InternalTopologyBuilder builder = lookupBuilderForTask(task);
        return builder == null ? null : builder.buildSubtopology(task.subtopology());
    }

    public ProcessorTopology globalTaskTopology() {
        if (hasNamedTopologies()) {
            throw new IllegalStateException("Global state stores are not supported with Named Topologies");
        }
        return globalTopology;
    }

    public Map<String, StateStore> globalStateStores() {
        return globalStateStores;
    }

    public Map<String, List<String>> stateStoreNameToSourceTopics() {
        final Map<String, List<String>> stateStoreNameToSourceTopics = new HashMap<>();
        applyToEachBuilder(b -> stateStoreNameToSourceTopics.putAll(b.stateStoreNameToSourceTopics()));
        return stateStoreNameToSourceTopics;
    }

    public String getStoreForChangelogTopic(final String topicName) {
        for (final InternalTopologyBuilder builder : builders.values()) {
            final String store = builder.getStoreForChangelogTopic(topicName);
            if (store != null) {
                return store;
            }
        }
        log.warn("Unable to locate any store for topic {}", topicName);
        return "";
    }

    public Collection<String> sourceTopicsForStore(final String storeName) {
        final List<String> sourceTopics = new ArrayList<>();
        applyToEachBuilder(b -> sourceTopics.addAll(b.sourceTopicsForStore(storeName)));
        return sourceTopics;
    }

    public Map<Subtopology, TopicsInfo> topicGroups() {
        final Map<Subtopology, TopicsInfo> topicGroups = new HashMap<>();
        applyToEachBuilder(b -> topicGroups.putAll(b.topicGroups()));
        return topicGroups;
    }

    public Map<String, List<String>> nodeToSourceTopics(final TaskId task) {
        return lookupBuilderForTask(task).nodeToSourceTopics();
    }

    void addSubscribedTopicsFromMetadata(final Set<String> topics, final String logPrefix) {
        applyToEachBuilder(b -> b.addSubscribedTopicsFromMetadata(topics, logPrefix));
    }

    void addSubscribedTopicsFromAssignment(final List<TopicPartition> partitions, final String logPrefix) {
        applyToEachBuilder(b -> b.addSubscribedTopicsFromAssignment(partitions, logPrefix));
    }

    public Collection<Set<String>> copartitionGroups() {
        final List<Set<String>> copartitionGroups = new ArrayList<>();
        applyToEachBuilder(b -> copartitionGroups.addAll(b.copartitionGroups()));
        return copartitionGroups;
    }

    private InternalTopologyBuilder lookupBuilderForTask(final TaskId task) {
        return task.namedTopology() == null ? builders.get(UNNAMED_TOPOLOGY) : builders.get(task.namedTopology());
    }

    private boolean evaluateConditionIsTrueForAnyBuilders(final Function<InternalTopologyBuilder, Boolean> condition) {
        for (final InternalTopologyBuilder builder : builders.values()) {
            if (condition.apply(builder)) {
                return true;
            }
        }
        return false;
    }

    private void applyToEachBuilder(final Consumer<InternalTopologyBuilder> function) {
        for (final InternalTopologyBuilder builder : builders.values()) {
            function.accept(builder);
        }
    }

    public static class Subtopology {
        final int nodeGroupId;
        final String namedTopology;

        public Subtopology(final int nodeGroupId, final String namedTopology) {
            this.nodeGroupId = nodeGroupId;
            this.namedTopology = namedTopology;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Subtopology that = (Subtopology) o;
            return nodeGroupId == that.nodeGroupId &&
                    Objects.equals(namedTopology, that.namedTopology);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeGroupId, namedTopology);
        }
    }
}
