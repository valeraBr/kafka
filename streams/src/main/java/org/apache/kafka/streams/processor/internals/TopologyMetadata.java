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
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.TopicsInfo;
import org.apache.kafka.streams.processor.internals.namedtopology.TopologyConfig.TaskConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
    private final Map<String, Long> threadVersions = new ConcurrentHashMap<>();

    public static class TopologyVersion {
        public AtomicLong topologyVersion = new AtomicLong(0L); // the local topology version
        public ReentrantLock topologyLock = new ReentrantLock();
        public Condition topologyCV = topologyLock.newCondition();
        public List<TopologyVersionWaiters> activeTopologyWaiters = new LinkedList<>();
    }

    public static class TopologyVersionWaiters {
        final long topologyVersion; // the (minimum) version to wait for these threads to cross
        final KafkaFutureImpl<Void> future; // the future waiting on all threads to be updated

        public TopologyVersionWaiters(final long topologyVersion, final KafkaFutureImpl<Void> future) {
            this.topologyVersion = topologyVersion;
            this.future = future;
        }
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

    public long topologyVersion() {
        return version.topologyVersion.get();
    }

    private void lock() {
        version.topologyLock.lock();
    }

    private void unlock() {
        version.topologyLock.unlock();
    }

    public Collection<String> sourceTopicsForTopology(final String name) {
        return builders.get(name).fullSourceTopicNames();
    }

    public boolean needsUpdate(final String threadName) {
        return threadVersions.get(threadName) < topologyVersion();
    }

    public void registerThread(final String threadName) {
        threadVersions.put(threadName, 0L);
    }

    public void unregisterThread(final String threadName) {
        threadVersions.remove(threadName);
    }

    public void maybeNotifyTopologyVersionWaiters(final String threadName) {
        try {
            lock();
            final Iterator<TopologyVersionWaiters> iterator = version.activeTopologyWaiters.listIterator();
            TopologyVersionWaiters topologyVersionWaiters;
            threadVersions.put(threadName, topologyVersion());
            while (iterator.hasNext()) {
                topologyVersionWaiters = iterator.next();
                final long topologyVersionWaitersVersion = topologyVersionWaiters.topologyVersion;
                if (topologyVersionWaitersVersion <= threadVersions.get(threadName)) {
                    if (threadVersions.values().stream().allMatch(t -> t >= topologyVersionWaitersVersion)) {
                        topologyVersionWaiters.future.complete(null);
                        iterator.remove();
                        log.info("Thread {} is now on topology version {}", threadName, topologyVersionWaiters.topologyVersion);
                    }
                }
            }
        } finally {
            unlock();
        }
    }

    public void wakeupThreads() {
        try {
            lock();
            version.topologyCV.signalAll();
        } finally {
            unlock();
        }
    }

    public void maybeWaitForNonEmptyTopology(final Supplier<StreamThread.State> threadState) {
        if (isEmpty() && threadState.get().isAlive()) {
            try {
                lock();
                while (isEmpty() && threadState.get().isAlive()) {
                    try {
                        log.debug("Detected that the topology is currently empty, waiting for something to process");
                        version.topologyCV.await();
                    } catch (final InterruptedException e) {
                        log.debug("StreamThread was interrupted while waiting on empty topology", e);
                    }
                }
            } finally {
                unlock();
            }
        }
    }

    /**
     * Adds the topology and registers a future that listens for all threads on the older version to see the update
     */
    public KafkaFuture<Void> registerAndBuildNewTopology(final InternalTopologyBuilder newTopologyBuilder) {
        final KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        try {
            lock();
            version.topologyVersion.incrementAndGet();
            log.info("Adding NamedTopology {}, latest topology version is {}", newTopologyBuilder.topologyName(), version.topologyVersion.get());
            version.activeTopologyWaiters.add(new TopologyVersionWaiters(topologyVersion(), future));
            builders.put(newTopologyBuilder.topologyName(), newTopologyBuilder);
            buildAndVerifyTopology(newTopologyBuilder);
            wakeupThreads();
        } finally {
            unlock();
        }
        return future;
    }

    /**
     * Removes the topology and registers a future that listens for all threads on the older version to see the update
     */
    public KafkaFuture<Void> unregisterTopology(final String topologyName) {
        final KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        try {
            lock();
            version.topologyVersion.incrementAndGet();
            log.info("Removing NamedTopology {}, latest topology version is {}", topologyName, version.topologyVersion.get());
            version.activeTopologyWaiters.add(new TopologyVersionWaiters(topologyVersion(), future));
            final InternalTopologyBuilder removedBuilder = builders.remove(topologyName);
            removedBuilder.fullSourceTopicNames().forEach(allInputTopics::remove);
            removedBuilder.allSourcePatternStrings().forEach(allInputTopics::remove);
        } finally {
            unlock();
        }
        return future;
    }

    public TaskConfig getTaskConfigFor(final TaskId taskId) {
        final InternalTopologyBuilder builder = lookupBuilderForTask(taskId);
        return builder.topologyConfigs().getTaskConfig();
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
            log.error("Tried to add the NamedTopology {} but it had overlap with other input topics {} or patterns {}",
                      builder.topologyName(), inputTopics, inputPatterns);
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
        return anyBuildersMatch(InternalTopologyBuilder::hasGlobalStores);
    }

    /**
     * @return true iff any of the topologies have no local (aka non-global) topology
     */
    public boolean hasNoLocalTopology() {
        return anyBuildersMatch(InternalTopologyBuilder::hasNoLocalTopology);
    }

    public boolean hasPersistentStores() {
        // If the app is using named topologies, there may not be any persistent state when it first starts up
        // but a new NamedTopology may introduce it later, so we must return true
        if (hasNamedTopologies()) {
            return true;
        }
        return anyBuildersMatch(InternalTopologyBuilder::hasPersistentStores);
    }

    public boolean hasStore(final String name) {
        return anyBuildersMatch(b -> b.hasStore(name));
    }

    public boolean hasOffsetResetOverrides() {
        // Return true if using named topologies, as there may be named topologies added later which do have overrides
        return hasNamedTopologies() || anyBuildersMatch(InternalTopologyBuilder::hasOffsetResetOverrides);
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

    /**
     * NOTE: this should not be invoked until all topologies have been built via
     * {@link #buildAndVerifyTopology(InternalTopologyBuilder)}, otherwise the
     * {@link InternalTopologyBuilder#fullSourceTopicNames()}
     */
    public Set<String> sourceTopicCollection() {
        final Set<String> sourceTopics = new HashSet<>();
        applyToEachBuilder(b -> sourceTopics.addAll(b.fullSourceTopicNames()));
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
        return anyBuildersMatch(InternalTopologyBuilder::usesPatternSubscription);
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

        applyToEachBuilder(b -> sb.append(b.describe().toString()));

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

    private String getTopologyNameOrElseUnnamed(final String topologyName) {
        return topologyName == null ? UNNAMED_TOPOLOGY : topologyName;
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
        if (usesPatternSubscription()) {
            final Map<String, Set<String>> newTopicsByTopology = new HashMap<>();
            final Set<String> duplicateInputTopics = new HashSet<>();
            final Set<String> subscriptionTopicsWithUnknownSource = new HashSet<>();
            for (final String topic : topics) {
                final Set<String> subscribingTopologies = new HashSet<>();
                applyToEachBuilder(b -> {
                    if (b.fullSourceTopicNames().contains(topic) || b.matchesSubscribedPattern(topic)) {
                        subscribingTopologies.add(getTopologyNameOrElseUnnamed(b.topologyName()));
                    }
                });
                if (subscribingTopologies.size() > 1) {
                    log.error("{}Subscribed topic {} matches more than one topology: {}",
                        logPrefix, topic, subscribingTopologies);
                    duplicateInputTopics.add(topic);
                } else if (subscribingTopologies.isEmpty()) {
                    log.error("{}Topic {} is subscribed to by the consumer, but no topology can be identified " +
                        "that is subscribing to that topic or to a matching Pattern", logPrefix, topic);
                    subscriptionTopicsWithUnknownSource.add(topic);
                }
                for (final String topology : subscribingTopologies) {
                    if (!allInputTopics.contains(topic)) {
                        newTopicsByTopology
                            .computeIfAbsent(topology, t -> new HashSet<>())
                            .add(topic);
                    }
                }
            }

            if (!duplicateInputTopics.isEmpty()) {
                throw new IllegalStateException("The following topics are subscribed to " +
                    "by multiple topologies: " + duplicateInputTopics);
            } else if (!subscriptionTopicsWithUnknownSource.isEmpty()) {
                throw new IllegalStateException("The following topics appear in the subscription " +
                    "but can't be located in the topology: " + subscriptionTopicsWithUnknownSource);
            }

            updateAndVerifyNewInputTopics(newTopicsByTopology);
            for (final Map.Entry<String, Set<String>> topology : newTopicsByTopology.entrySet()) {
                lookupBuilderForNamedTopology(topology.getKey())
                    .addSubscribedTopicsFromMetadata(topology.getValue(), logPrefix);
            }
        } else {
            if (!topics.equals(sourceTopicCollection())) {
                log.error("{}Consumer's subscription does not match the known source topics.\n" +
                        "consumer subscription: {}\n" +
                        "topics in topology metadata: {}",
                    logPrefix, topics, sourceTopicCollection());
                throw new IllegalStateException("Consumer subscribed topics and topology's known topics do not match.");
            }
        }
    }

    void addSubscribedTopicsFromAssignment(final Map<TaskId, Set<TopicPartition>> tasks, final String logPrefix) {
        final Map<String, Set<String>> assignedTopicsByTopology = new HashMap<>();

        for (final Map.Entry<TaskId, Set<TopicPartition>> task : tasks.entrySet()) {
            final String topologyName = getTopologyNameOrElseUnnamed(task.getKey().topologyName());
            // Skip updating subscription with topics if their topology is not yet known to this client,
            // the subscription will be updated when the topology is added
            if (builders.containsKey(topologyName)) {
                assignedTopicsByTopology
                    .computeIfAbsent(topologyName, t -> new HashSet<>())
                    .addAll(task.getValue().stream().map(TopicPartition::topic).collect(Collectors.toSet()));
            }
        }
        for (final Map.Entry<String, Set<String>> assignedTopics : assignedTopicsByTopology.entrySet()) {
            final Set<String> newTopics =
                lookupBuilderForNamedTopology(assignedTopics.getKey())
                    .addSubscribedTopicsFromAssignment(assignedTopics.getValue(), logPrefix);
            assignedTopics.getValue().retainAll(newTopics);
        }
        updateAndVerifyNewInputTopics(assignedTopicsByTopology);
    }

    private void updateAndVerifyNewInputTopics(final Map<String, Set<String>> newTopicsByTopology) {
        final Set<String> duplicateInputTopics = new HashSet<>();

        for (final Map.Entry<String, Set<String>> newTopics : newTopicsByTopology.entrySet()) {
            final String topologyName = newTopics.getKey();
            for (final String newTopic : newTopics.getValue()) {
                if (allInputTopics.contains(newTopic)) {
                    duplicateInputTopics.add(newTopic);
                    final String namedTopologyErrorMessagePrefix = UNNAMED_TOPOLOGY.equals(topologyName) ?
                        "" :
                        "Topology {}: ";
                    log.error("{}Cannot add topic {} to the subscription as the application is " +
                        "already consuming from this topic elsewhere in the topology",
                        namedTopologyErrorMessagePrefix, newTopic);
                } else {
                    allInputTopics.add(newTopic);
                }
            }
        }
        if (!duplicateInputTopics.isEmpty()) {
            throw new IllegalStateException("Found overlapping input topics when updating subscription, you may " +
                "not consume from the same source topic more than once per application. Please make sure there are " +
                "no topics that are subscribed to in more than one place, whether directly or by matching Pattern " +
                "subscriptions. The following topics were found to be duplicates: " + duplicateInputTopics);
        }
    }

    public Collection<Set<String>> copartitionGroups() {
        final List<Set<String>> copartitionGroups = new ArrayList<>();
        applyToEachBuilder(b -> copartitionGroups.addAll(b.copartitionGroups()));
        return copartitionGroups;
    }

    private InternalTopologyBuilder lookupBuilderForTask(final TaskId task) {
        return task.topologyName() == null ? builders.get(UNNAMED_TOPOLOGY) : builders.get(task.topologyName());
    }

    /**
     * @return the InternalTopologyBuilder for a Topology, or null if no such Topology exists
     */
    public InternalTopologyBuilder lookupBuilderForNamedTopology(final String name) {
        return builders.get(name);
    }

    private boolean anyBuildersMatch(final Function<InternalTopologyBuilder, Boolean> condition) {
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
