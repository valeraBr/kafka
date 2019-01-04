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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A class encapsulating some of the logic around metadata.
 * <p>
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 *
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metadata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 * <p>
 * If topic expiry is enabled for the metadata, any topic that has not been used within the expiry interval
 * is removed from the metadata refresh set after an update. Consumers disable topic expiry since they explicitly
 * manage topics while producers rely on topic expiry to limit the refresh set.
 */
public class Metadata implements Closeable {

    /**
     * A mutable cache of nodes, topics, and partitions in the Kafka cluster
     */
    static class ClusterMetadataCache {
        final boolean bootstrapped;
        final String clusterId;
        final List<Node> nodes;
        final Set<String> unauthorizedTopics;
        final Set<String> invalidTopics;
        final Set<String> internalTopics;
        final Node controller;
        final Map<TopicPartition, PartitionInfo> partitionsByTopicPartition;
        final Map<Integer, Node> nodesById;

        ClusterMetadataCache(boolean bootstrapped,
                             String clusterId,
                             List<Node> nodes,
                             Collection<PartitionInfo> partitions,
                             Set<String> unauthorizedTopics,
                             Set<String> invalidTopics,
                             Set<String> internalTopics,
                             Node controller) {
            this.bootstrapped = bootstrapped;
            this.clusterId = clusterId;
            this.nodes = nodes;
            this.unauthorizedTopics = unauthorizedTopics;
            this.invalidTopics = invalidTopics;
            this.internalTopics = internalTopics;
            this.controller = controller;
            this.nodesById = new HashMap<>();
            for (Node node : nodes) {
                this.nodesById.put(node.id(), node);
            }
            // index the partitions by topic/partition for quick lookup
            this.partitionsByTopicPartition = new HashMap<>(partitions.size());
            for (PartitionInfo p : partitions) {
                this.partitionsByTopicPartition.put(new TopicPartition(p.topic(), p.partition()), p);
            }
        }

        /**
         * Remove a partition from this cache.
         * @param topicPartition
         * @return
         */
        boolean removePartition(TopicPartition topicPartition) {
            return partitionsByTopicPartition.remove(topicPartition) != null;
        }

        /**
         * Convert this to a Cluster instance
         * @return
         */
        Cluster toCluster() {
            return new Cluster(clusterId, nodes, partitionsByTopicPartition.values(), unauthorizedTopics,
                    internalTopics, internalTopics, controller);
        }

        static ClusterMetadataCache bootstrap(Collection<InetSocketAddress> addresses) {
            List<Node> nodes = new ArrayList<>();
            int nodeId = -1;
            for (InetSocketAddress address : addresses)
                nodes.add(new Node(nodeId--, address.getHostString(), address.getPort()));
            return new ClusterMetadataCache(true, null, nodes, new ArrayList<>(0),
                    Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null);
        }

        static ClusterMetadataCache empty() {
            return new ClusterMetadataCache(false, null, new ArrayList<>(0), new ArrayList<>(0),
                    Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Metadata.class);

    public static final long TOPIC_EXPIRY_MS = 5 * 60 * 1000;
    private static final long TOPIC_EXPIRY_NEEDS_UPDATE = -1L;

    private final long refreshBackoffMs;
    private final long metadataExpireMs;
    private int version;
    private long lastRefreshMs;
    private long lastSuccessfulRefreshMs;
    private AuthenticationException authenticationException;
    private ClusterMetadataCache cache = ClusterMetadataCache.empty();
    private Set<String> unavailableTopics = Collections.emptySet();
    private boolean needUpdate;
    /* Topics with expiry time */
    private final Map<String, Long> topics;
    private final List<Listener> listeners;
    private final ClusterResourceListeners clusterResourceListeners;
    private boolean needMetadataForAllTopics;
    private final boolean allowAutoTopicCreation;
    private final boolean topicExpiryEnabled;
    private boolean isClosed;
    private final Map<TopicPartition, Integer> lastSeenLeaderEpochs;

    public Metadata(long refreshBackoffMs,
                    long metadataExpireMs,
                    boolean allowAutoTopicCreation) {
        this(refreshBackoffMs, metadataExpireMs, allowAutoTopicCreation, false, new ClusterResourceListeners());
    }

    /**
     * Create a new Metadata instance
     * @param refreshBackoffMs The minimum amount of time that must expire between metadata refreshes to avoid busy
     *        polling
     * @param metadataExpireMs The maximum amount of time that metadata can be retained without refresh
     * @param allowAutoTopicCreation If this and the broker config 'auto.create.topics.enable' are true, topics that
     *                               don't exist will be created by the broker when a metadata request is sent
     * @param topicExpiryEnabled If true, enable expiry of unused topics
     * @param clusterResourceListeners List of ClusterResourceListeners which will receive metadata updates.
     */
    public Metadata(long refreshBackoffMs,
                    long metadataExpireMs,
                    boolean allowAutoTopicCreation,
                    boolean topicExpiryEnabled,
                    ClusterResourceListeners clusterResourceListeners) {
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.allowAutoTopicCreation = allowAutoTopicCreation;
        this.topicExpiryEnabled = topicExpiryEnabled;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.version = 0;
        this.needUpdate = false;
        this.topics = new HashMap<>();
        this.listeners = new ArrayList<>();
        this.clusterResourceListeners = clusterResourceListeners;
        this.needMetadataForAllTopics = false;
        this.isClosed = false;
        this.lastSeenLeaderEpochs = new HashMap<>();
    }

    /**
     * Get the current cluster info without blocking
     */
    public synchronized Cluster fetch() {
        return cache.toCluster();
    }

    /**
     * Add the topic to maintain in the metadata. If topic expiry is enabled, expiry time
     * will be reset on the next update.
     */
    public synchronized void add(String topic) {
        Objects.requireNonNull(topic, "topic cannot be null");
        if (topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE) == null) {
            requestUpdateForNewTopics();
        }
    }

    /**
     * Return the next time when the current cluster info can be updated (i.e., backoff time has elapsed).
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till the cluster info can be updated again
     */
    public synchronized long timeToAllowUpdate(long nowMs) {
        return Math.max(this.lastRefreshMs + this.refreshBackoffMs - nowMs, 0);
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
     * is now
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till updating the cluster info
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        long timeToExpire = needUpdate ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        return Math.max(timeToExpire, timeToAllowUpdate(nowMs));
    }

    /**
     * Request an update of the current cluster metadata info, return the current version before the update
     */
    public synchronized int requestUpdate() {
        this.needUpdate = true;
        return this.version;
    }

    /**
     * Request an update for the partition metadata iff the given leader epoch is at newer than the last seen leader epoch
     */
    public synchronized boolean updateLastSeenEpochIfNewer(TopicPartition topicPartition, int leaderEpoch) {
        Objects.requireNonNull(topicPartition, "TopicPartition cannot be null");
        if (updateLastSeenEpoch(topicPartition, leaderEpoch, oldEpoch -> leaderEpoch > oldEpoch) == leaderEpoch) {
            this.needUpdate = true;
            return true;
        } else {
            return false;
        }
    }

    /**
     * Update the last seen leader epoch for a partition. Requests a metadata update if the given leader epoch differs
     * from what is in the cache.
     */
    public synchronized boolean updateLastSeenEpoch(TopicPartition topicPartition, int leaderEpoch) {
        Objects.requireNonNull(topicPartition, "TopicPartition cannot be null");
        if (updateLastSeenEpoch(topicPartition, leaderEpoch, oldEpoch -> leaderEpoch != oldEpoch) == leaderEpoch) {
            this.needUpdate = true;
            return true;
        } else {
            return false;
        }
    }

    /**
     * Unset the last seen leader epoch for a partition and remove its cached metadata
     */
    public synchronized void resetLastSeenEpoch(TopicPartition topicPartition) {
        Objects.requireNonNull(topicPartition, "TopicPartition cannot be null");
        if (lastSeenLeaderEpochs.remove(topicPartition) != null) {
            cache.removePartition(topicPartition);
        }
    }

    // Visible for testing
    Optional<Integer> lastSeenLeaderEpoch(TopicPartition topicPartition) {
        return Optional.ofNullable(lastSeenLeaderEpochs.get(topicPartition));
    }

    /**
     * Update the leader epoch for a partition if the given predicate passes. If updated, remove the stale metadata
     * record from {@link Cluster}.
     */
    private int updateLastSeenEpoch(TopicPartition topicPartition, int epoch, Predicate<Integer> epochTest) {
        Integer oldEpoch = lastSeenLeaderEpochs.get(topicPartition);
        if (oldEpoch == null || epochTest.test(oldEpoch)) {
            log.debug("Setting last seen epoch to {} for partition {}", epoch, topicPartition);
            lastSeenLeaderEpochs.put(topicPartition, epoch);
            cache.removePartition(topicPartition);
            return epoch;
        } else {
            return oldEpoch;
        }
    }

    /**
     * Check whether an update has been explicitly requested.
     * @return true if an update was requested, false otherwise
     */
    public synchronized boolean updateRequested() {
        return this.needUpdate;
    }

    /**
     * If any non-retriable authentication exceptions were encountered during
     * metadata update, clear and return the exception.
     */
    public synchronized AuthenticationException getAndClearAuthenticationException() {
        if (authenticationException != null) {
            AuthenticationException exception = authenticationException;
            authenticationException = null;
            return exception;
        } else
            return null;
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     */
    public synchronized void awaitUpdate(final int lastVersion, final long maxWaitMs) throws InterruptedException {
        if (maxWaitMs < 0)
            throw new IllegalArgumentException("Max time to wait for metadata updates should not be < 0 milliseconds");

        long begin = System.currentTimeMillis();
        long remainingWaitMs = maxWaitMs;
        while ((this.version <= lastVersion) && !isClosed()) {
            AuthenticationException ex = getAndClearAuthenticationException();
            if (ex != null)
                throw ex;
            if (remainingWaitMs != 0)
                wait(remainingWaitMs);
            long elapsed = System.currentTimeMillis() - begin;
            if (elapsed >= maxWaitMs)
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            remainingWaitMs = maxWaitMs - elapsed;
        }
        if (isClosed())
            throw new KafkaException("Requested metadata update after close");
    }

    /**
     * Replace the current set of topics maintained to the one provided.
     * If topic expiry is enabled, expiry time of the topics will be
     * reset on the next update.
     * @param topics
     */
    public synchronized void setTopics(Collection<String> topics) {
        if (!this.topics.keySet().containsAll(topics)) {
            requestUpdateForNewTopics();
        }
        this.topics.clear();
        for (String topic : topics)
            this.topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE);
    }

    /**
     * Get the list of topics we are currently maintaining metadata for
     */
    public synchronized Set<String> topics() {
        return new HashSet<>(this.topics.keySet());
    }

    /**
     * Check if a topic is already in the topic set.
     * @param topic topic to check
     * @return true if the topic exists, false otherwise
     */
    public synchronized boolean containsTopic(String topic) {
        return this.topics.containsKey(topic);
    }

    public synchronized void bootstrap(List<InetSocketAddress> addresses, long now) {
        this.needUpdate = true;
        this.lastRefreshMs = now;
        this.lastSuccessfulRefreshMs = now;
        this.version += 1;
        this.cache = ClusterMetadataCache.bootstrap(addresses);
    }

    /**
     * Updates the cluster metadata. If topic expiry is enabled, expiry time
     * is set for topics if required and expired topics are removed from the metadata.
     *
     * @param metadataResponse metadata response received from the broker
     * @param now current time in milliseconds
     */
    public synchronized void update(MetadataResponse metadataResponse, long now) {
        Objects.requireNonNull(metadataResponse, "Metadata response cannot be null");
        if (isClosed())
            throw new IllegalStateException("Update requested after metadata close");

        this.needUpdate = false;
        this.lastRefreshMs = now;
        this.lastSuccessfulRefreshMs = now;
        this.version += 1;

        if (topicExpiryEnabled) {
            // Handle expiry of topics from the metadata refresh set.
            for (Iterator<Map.Entry<String, Long>> it = topics.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Long> entry = it.next();
                long expireMs = entry.getValue();
                if (expireMs == TOPIC_EXPIRY_NEEDS_UPDATE)
                    entry.setValue(now + TOPIC_EXPIRY_MS);
                else if (expireMs <= now) {
                    it.remove();
                    log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", entry.getKey(), expireMs, now);
                }
            }
        }

        String previousClusterId = cache.clusterId;

        this.cache = handleMetadataResponse(metadataResponse, topic -> true);
        this.unavailableTopics = metadataResponse.unavailableTopics();
        Cluster clusterForListeners = this.cache.toCluster();
        fireListeners(clusterForListeners, unavailableTopics);

        if (this.needMetadataForAllTopics) {
            // the listener may change the interested topics, which could cause another metadata refresh.
            // If we have already fetched all topics, however, another fetch should be unnecessary.
            this.needUpdate = false;
            this.cache = handleMetadataResponse(metadataResponse, topics.keySet()::contains);
        }

        String newClusterId = cache.clusterId;
        if (newClusterId == null ? previousClusterId != null : !newClusterId.equals(previousClusterId))
            log.info("Cluster ID: {}", newClusterId);
        clusterResourceListeners.onUpdate(clusterForListeners.clusterResource());

        notifyAll();
        log.debug("Updated cluster metadata version {} to {}", this.version, this.cache);
    }

    /**
     * Transform a MetadataResponse into a new MetadataCache instance.
     * @param metadataResponse
     * @param topicsToRetain
     * @return
     */
    private ClusterMetadataCache handleMetadataResponse(MetadataResponse metadataResponse, Predicate<String> topicsToRetain) {
        Set<String> internalTopics = new HashSet<>();
        List<PartitionInfo> partitions = new ArrayList<>();
        for (MetadataResponse.TopicMetadata metadata : metadataResponse.topicMetadata()) {
            if (!topicsToRetain.test(metadata.topic()))
                continue;

            if (metadata.error() == Errors.NONE) {
                if (metadata.isInternal())
                    internalTopics.add(metadata.topic());
                for (MetadataResponse.PartitionMetadata partitionMetadata : metadata.partitionMetadata()) {
                    updatePartitionInfo(metadata.topic(), partitionMetadata, partitions::add);
                }
            }
        }

        return new ClusterMetadataCache(false, metadataResponse.clusterId(), new ArrayList<>(metadataResponse.brokers()), partitions,
                metadataResponse.topicsByError(Errors.TOPIC_AUTHORIZATION_FAILED),
                metadataResponse.topicsByError(Errors.INVALID_TOPIC_EXCEPTION),
                internalTopics, metadataResponse.controller());
    }

    /**
     * Compute the correct PartitionInfo to cache for a topic+partition.
     * @param topic
     * @param partitionMetadata
     * @param partitionInfoConsumer
     */
    private void updatePartitionInfo(String topic, MetadataResponse.PartitionMetadata partitionMetadata, Consumer<PartitionInfo> partitionInfoConsumer) {
        TopicPartition tp = new TopicPartition(topic, partitionMetadata.partition());
        if (partitionMetadata.leaderEpoch().isPresent()) {
            int newEpoch = partitionMetadata.leaderEpoch().get();
            if (updateLastSeenEpoch(tp, newEpoch, oldEpoch -> newEpoch >= oldEpoch) == newEpoch) {
                // If the received leader epoch is at least the same as the previous one, use the new partition info
                partitionInfoConsumer.accept(MetadataResponse.partitionMetaToInfo(topic, partitionMetadata));
            } else {
                // Otherwise ignore the new metadata and use the previously cached info
                PartitionInfo previousInfo = cache.partitionsByTopicPartition.get(tp);
                if (previousInfo != null) {
                    partitionInfoConsumer.accept(previousInfo);
                } else {
                    log.warn("Got an older epoch in partition metadata response for {}, but could not find previous partition " +
                            "info to use. Refusing to update metadata", tp);
                }
            }
        } else {
            partitionInfoConsumer.accept(MetadataResponse.partitionMetaToInfo(topic, partitionMetadata));
        }
    }

    private void fireListeners(Cluster newCluster, Set<String> unavailableTopics) {
        for (Listener listener: listeners)
            listener.onMetadataUpdate(newCluster, unavailableTopics);
    }

    /**
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    public synchronized void failedUpdate(long now, AuthenticationException authenticationException) {
        this.lastRefreshMs = now;
        this.authenticationException = authenticationException;
        if (authenticationException != null)
            this.notifyAll();
    }

    /**
     * @return The current metadata version
     */
    public synchronized int version() {
        return this.version;
    }

    /**
     * The last time metadata was successfully updated.
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    public boolean allowAutoTopicCreation() {
        return allowAutoTopicCreation;
    }

    /**
     * Set state to indicate if metadata for all topics in Kafka cluster is required or not.
     * @param needMetadataForAllTopics boolean indicating need for metadata of all topics in cluster.
     */
    public synchronized void needMetadataForAllTopics(boolean needMetadataForAllTopics) {
        if (needMetadataForAllTopics && !this.needMetadataForAllTopics) {
            requestUpdateForNewTopics();
        }
        this.needMetadataForAllTopics = needMetadataForAllTopics;
    }

    /**
     * Get whether metadata for all topics is needed or not
     */
    public synchronized boolean needMetadataForAllTopics() {
        return this.needMetadataForAllTopics;
    }

    /**
     * Add a Metadata listener that gets notified of metadata updates
     */
    public synchronized void addListener(Listener listener) {
        this.listeners.add(listener);
    }

    /**
     * Stop notifying the listener of metadata updates
     */
    public synchronized void removeListener(Listener listener) {
        this.listeners.remove(listener);
    }

    /**
     * "Close" this metadata instance to indicate that metadata updates are no longer possible. This is typically used
     * when the thread responsible for performing metadata updates is exiting and needs a way to relay this information
     * to any other thread(s) that could potentially wait on metadata update to come through.
     */
    @Override
    public synchronized void close() {
        this.isClosed = true;
        this.notifyAll();
    }

    /**
     * Check if this metadata instance has been closed. See {@link #close()} for more information.
     * @return True if this instance has been closed; false otherwise
     */
    public synchronized boolean isClosed() {
        return this.isClosed;
    }

    /**
     * MetadataUpdate Listener
     */
    public interface Listener {
        /**
         * Callback invoked on metadata update.
         *
         * @param cluster the cluster containing metadata for topics with valid metadata
         * @param unavailableTopics topics which are non-existent or have one or more partitions whose
         *        leader is not known
         */
        void onMetadataUpdate(Cluster cluster, Set<String> unavailableTopics);
    }

    private synchronized void requestUpdateForNewTopics() {
        // Override the timestamp of last refresh to let immediate update.
        this.lastRefreshMs = 0;
        requestUpdate();
    }

}
