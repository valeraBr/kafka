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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.PartitionStates;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * A class for tracking the topics, partitions, and offsets for the consumer. A partition
 * is "assigned" either directly with {@link #assignFromUser(Set)} (manual assignment)
 * or with {@link #assignFromSubscribed(Collection)} (automatic assignment from subscription).
 *
 * Once assigned, the partition is not considered "fetchable" until its initial position has
 * been set with {@link #seek(TopicPartition, FetchPosition)}. Fetchable partitions track a fetch
 * position which is used to set the offset of the next fetch, and a consumed position
 * which is the last offset that has been returned to the user. You can suspend fetching
 * from a partition through {@link #pause(TopicPartition)} without affecting the fetched/consumed
 * offsets. The partition will remain unfetchable until the {@link #resume(TopicPartition)} is
 * used. You can also query the pause state independently with {@link #isPaused(TopicPartition)}.
 *
 * Note that pause state as well as fetch/consumed positions are not preserved when partition
 * assignment is changed whether directly by the user or through a group rebalance.
 *
 * Thread Safety: this class is generally not thread-safe. It should only be accessed in the
 * consumer's calling thread. The only exception is {@link ConsumerMetadata} which accesses
 * the subscription state needed to build and handle Metadata requests. The thread-safe methods
 * are documented below.
 */
public class SubscriptionState {
    private static final String SUBSCRIPTION_EXCEPTION_MESSAGE =
            "Subscription to topics, partitions and pattern are mutually exclusive";

    private final Logger log;

    private enum SubscriptionType {
        NONE, AUTO_TOPICS, AUTO_PATTERN, USER_ASSIGNED
    }

    /* the type of subscription */
    private volatile SubscriptionType subscriptionType;

    /* the pattern user has requested */
    private volatile Pattern subscribedPattern;

    /* the list of topics the user has requested */
    private Set<String> subscription;

    /* The list of topics the group has subscribed to. This may include some topics which are not part
     * of `subscription` for the leader of a group since it is responsible for detecting metadata changes
     * which require a group rebalance. */
    private final Set<String> groupSubscription;

    /* the partitions that are currently assigned, note that the order of partition matters (see FetchBuilder for more details) */
    private final PartitionStates<TopicPartitionState> assignment;

    /* Default offset reset strategy */
    private final OffsetResetStrategy defaultResetStrategy;

    /* Listeners provide a hook for internal state cleanup (e.g. metrics) on assignment changes */
    private final List<Listener> listeners = new ArrayList<>();

    /* User-provided listener to be invoked when assignment changes */
    private ConsumerRebalanceListener rebalanceListener;

    public SubscriptionState(LogContext logContext, OffsetResetStrategy defaultResetStrategy) {
        this.log = logContext.logger(this.getClass());
        this.defaultResetStrategy = defaultResetStrategy;
        this.subscription = Collections.emptySet();
        this.assignment = new PartitionStates<>();
        this.groupSubscription = ConcurrentHashMap.newKeySet();
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }

    /**
     * This method sets the subscription type if it is not already set (i.e. when it is NONE),
     * or verifies that the subscription type is equal to the give type when it is set (i.e.
     * when it is not NONE)
     * @param type The given subscription type
     */
    private void setSubscriptionType(SubscriptionType type) {
        if (this.subscriptionType == SubscriptionType.NONE)
            this.subscriptionType = type;
        else if (this.subscriptionType != type)
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
    }

    public boolean subscribe(Set<String> topics, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        setSubscriptionType(SubscriptionType.AUTO_TOPICS);

        this.rebalanceListener = listener;

        return changeSubscription(topics);
    }

    public boolean subscribeFromPattern(Set<String> topics) {
        if (subscriptionType != SubscriptionType.AUTO_PATTERN)
            throw new IllegalArgumentException("Attempt to subscribe from pattern while subscription type set to " +
                    subscriptionType);

        return changeSubscription(topics);
    }

    private boolean changeSubscription(Set<String> topicsToSubscribe) {
        if (subscription.equals(topicsToSubscribe))
            return false;

        this.subscription = topicsToSubscribe;
        this.groupSubscription.addAll(topicsToSubscribe);
        return true;
    }

    /**
     * Add topics to the current group subscription. This is used by the group leader to ensure
     * that it receives metadata updates for all topics that the group is interested in.
     * @param topics The topics to add to the group subscription
     */
    public boolean groupSubscribe(Collection<String> topics) {
        if (!partitionsAutoAssigned())
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
        return this.groupSubscription.addAll(topics);
    }

    /**
     * Reset the group's subscription to only contain topics subscribed by this consumer.
     */
    public void resetGroupSubscription() {
        this.groupSubscription.retainAll(subscription);
    }

    /**
     * Change the assignment to the specified partitions provided by the user,
     * note this is different from {@link #assignFromSubscribed(Collection)}
     * whose input partitions are provided from the subscribed topics.
     */
    public boolean assignFromUser(Set<TopicPartition> partitions) {
        setSubscriptionType(SubscriptionType.USER_ASSIGNED);

        if (this.assignment.partitionSet().equals(partitions))
            return false;

        fireOnAssignment(partitions);

        Set<String> manualSubscribedTopics = new HashSet<>();
        Map<TopicPartition, TopicPartitionState> partitionToState = new HashMap<>();
        for (TopicPartition partition : partitions) {
            TopicPartitionState state = assignment.stateValue(partition);
            if (state == null)
                state = new TopicPartitionState();
            partitionToState.put(partition, state);
            manualSubscribedTopics.add(partition.topic());
        }
        this.assignment.set(partitionToState);
        return changeSubscription(manualSubscribedTopics);
    }

    /**
     * Change the assignment to the specified partitions returned from the coordinator, note this is
     * different from {@link #assignFromUser(Set)} which directly set the assignment from user inputs.
     *
     * @return true if assignments matches subscription, otherwise false
     */
    public boolean assignFromSubscribed(Collection<TopicPartition> assignments) {
        if (!this.partitionsAutoAssigned())
            throw new IllegalArgumentException("Attempt to dynamically assign partitions while manual assignment in use");

        Predicate<TopicPartition> predicate = topicPartition -> {
            if (this.subscribedPattern != null) {
                boolean match = this.subscribedPattern.matcher(topicPartition.topic()).matches();
                if (!match) {
                    log.info("Assigned partition {} for non-subscribed topic regex pattern; subscription pattern is {}",
                            topicPartition,
                            this.subscribedPattern);
                }
                return match;
            } else {
                boolean match = this.subscription.contains(topicPartition.topic());
                if (!match) {
                    log.info("Assigned partition {} for non-subscribed topic; subscription is {}", topicPartition, this.subscription);
                }
                return match;
            }
        };

        boolean assignmentMatchedSubscription = assignments.stream().allMatch(predicate);

        if (assignmentMatchedSubscription) {
            Map<TopicPartition, TopicPartitionState> assignedPartitionStates = partitionToStateMap(
                    assignments);
            fireOnAssignment(assignedPartitionStates.keySet());

            this.assignment.set(assignedPartitionStates);
        }

        return assignmentMatchedSubscription;
    }

    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        setSubscriptionType(SubscriptionType.AUTO_PATTERN);

        this.rebalanceListener = listener;
        this.subscribedPattern = pattern;
    }

    /**
     * Check whether pattern subscription is in use. This is thread-safe.
     *
     */
    public boolean hasPatternSubscription() {
        return this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public boolean hasNoSubscriptionOrUserAssignment() {
        return this.subscriptionType == SubscriptionType.NONE;
    }

    public void unsubscribe() {
        this.subscription = Collections.emptySet();
        this.groupSubscription.clear();
        this.assignment.clear();
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
        fireOnAssignment(Collections.emptySet());
    }

    /**
     * Check whether a topic matches a subscribed pattern.
     *
     * This is thread-safe, but it may not always reflect the most recent subscription pattern.
     *
     * @return true if pattern subscription is in use and the topic matches the subscribed pattern, false otherwise
     */
    public boolean matchesSubscribedPattern(String topic) {
        Pattern pattern = this.subscribedPattern;
        if (hasPatternSubscription() && pattern != null)
            return pattern.matcher(topic).matches();
        return false;
    }

    public Set<String> subscription() {
        if (partitionsAutoAssigned())
            return this.subscription;
        return Collections.emptySet();
    }

    public Set<TopicPartition> pausedPartitions() {
        return collectPartitions(TopicPartitionState::isPaused, Collectors.toSet());
    }

    /**
     * Get the subscription for the group. For the leader, this will include the union of the
     * subscriptions of all group members. For followers, it is just that member's subscription.
     * This is used when querying topic metadata to detect the metadata changes which would
     * require rebalancing. The leader fetches metadata for all topics in the group so that it
     * can do the partition assignment (which requires at least partition counts for all topics
     * to be assigned).
     *
     * Note this is thread-safe since the Set is backed by a ConcurrentMap.
     *
     * @return The union of all subscribed topics in the group if this member is the leader
     *   of the current generation; otherwise it returns the same set as {@link #subscription()}
     */
    public Set<String> groupSubscription() {
        return this.groupSubscription;
    }

    /**
     * Note this is thread-safe since the Set is backed by a ConcurrentMap.
     */
    public boolean isGroupSubscribed(String topic) {
        return groupSubscription.contains(topic);
    }

    private TopicPartitionState assignedState(TopicPartition tp) {
        TopicPartitionState state = this.assignment.stateValue(tp);
        if (state == null)
            throw new IllegalStateException("No current assignment for partition " + tp);
        return state;
    }

    public void seek(TopicPartition tp, FetchPosition position) {
        assignedState(tp).seek(position);
    }

    public void seek(TopicPartition tp, long offset) {
        seek(tp, new FetchPosition(offset, Optional.empty(), new Metadata.LeaderAndEpoch(Node.noNode(), Optional.empty())));
    }

    /**
     * @return an unmodifiable view of the currently assigned partitions
     */
    public Set<TopicPartition> assignedPartitions() {
        return this.assignment.partitionSet();
    }

    /**
     * Provides the number of assigned partitions in a thread safe manner.
     * @return the number of assigned partitions.
     */
    public int numAssignedPartitions() {
        return this.assignment.size();
    }

    public List<TopicPartition> fetchablePartitions(Predicate<TopicPartition> isAvailable) {
        return assignment.stream()
                .filter(tpState -> isAvailable.test(tpState.topicPartition()) && tpState.value().isFetchable())
                .map(PartitionStates.PartitionState::topicPartition)
                .collect(Collectors.toList());
    }

    public boolean partitionsAutoAssigned() {
        return this.subscriptionType == SubscriptionType.AUTO_TOPICS || this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public void position(TopicPartition tp, FetchPosition position) {
        assignedState(tp).position(position);
    }

    public boolean needsValidation(TopicPartition tp, Metadata.LeaderAndEpoch leaderAndEpoch) {
        return assignedState(tp).needsValidation(leaderAndEpoch);
    }

    public void validate(TopicPartition tp) {
        assignedState(tp).validate();
    }

    public FetchPosition position(TopicPartition tp) {
        return assignedState(tp).position;
    }

    public Long partitionLag(TopicPartition tp, IsolationLevel isolationLevel) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        if (isolationLevel == IsolationLevel.READ_COMMITTED)
            return topicPartitionState.lastStableOffset == null ? null : topicPartitionState.lastStableOffset - topicPartitionState.position.offset;
        else
            return topicPartitionState.highWatermark == null ? null : topicPartitionState.highWatermark - topicPartitionState.position.offset;
    }

    public Long partitionLead(TopicPartition tp) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        return topicPartitionState.logStartOffset == null ? null : topicPartitionState.position.offset - topicPartitionState.logStartOffset;
    }

    public void updateHighWatermark(TopicPartition tp, long highWatermark) {
        assignedState(tp).highWatermark = highWatermark;
    }

    public void updateLogStartOffset(TopicPartition tp, long logStartOffset) {
        assignedState(tp).logStartOffset = logStartOffset;
    }

    public void updateLastStableOffset(TopicPartition tp, long lastStableOffset) {
        assignedState(tp).lastStableOffset = lastStableOffset;
    }

    public Map<TopicPartition, OffsetAndMetadata> allConsumed() {
        Map<TopicPartition, OffsetAndMetadata> allConsumed = new HashMap<>();
        assignment.stream().forEach(state -> {
            TopicPartitionState partitionState = state.value();
            if (partitionState.hasValidPosition())
                allConsumed.put(state.topicPartition(), new OffsetAndMetadata(partitionState.position.offset,
                        partitionState.position.lastFetchEpoch, ""));
        });
        return allConsumed;
    }

    public void requestOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy) {
        assignedState(partition).reset(offsetResetStrategy);
    }

    public void requestOffsetReset(TopicPartition partition) {
        requestOffsetReset(partition, defaultResetStrategy);
    }

    public void setResetPending(Set<TopicPartition> partitions, long nextAllowResetTimeMs) {
        for (TopicPartition partition : partitions) {
            assignedState(partition).setNextAllowedRetry(nextAllowResetTimeMs);
        }
    }

    public boolean hasDefaultOffsetResetPolicy() {
        return defaultResetStrategy != OffsetResetStrategy.NONE;
    }

    public boolean isOffsetResetNeeded(TopicPartition partition) {
        return assignedState(partition).awaitingReset();
    }

    public OffsetResetStrategy resetStrategy(TopicPartition partition) {
        return assignedState(partition).resetStrategy;
    }

    public boolean hasAllFetchPositions() {
        return assignment.stream().allMatch(state -> state.value().hasValidPosition());
    }

    public Set<TopicPartition> missingFetchPositions() {
        return collectPartitions(state -> !state.hasPosition(), Collectors.toSet());
    }

    private <T extends Collection<TopicPartition>> T collectPartitions(Predicate<TopicPartitionState> filter, Collector<TopicPartition, ?, T> collector) {
        return assignment.stream()
                .filter(state -> filter.test(state.value()))
                .map(PartitionStates.PartitionState::topicPartition)
                .collect(collector);
    }


    public void resetMissingPositions() {
        final Set<TopicPartition> partitionsWithNoOffsets = new HashSet<>();
        assignment.stream().forEach(state -> {
            TopicPartition tp = state.topicPartition();
            TopicPartitionState partitionState = state.value();
            if (!partitionState.hasPosition()) {
                if (defaultResetStrategy == OffsetResetStrategy.NONE)
                    partitionsWithNoOffsets.add(tp);
                else
                    partitionState.reset(defaultResetStrategy);
            }
        });

        if (!partitionsWithNoOffsets.isEmpty())
            throw new NoOffsetForPartitionException(partitionsWithNoOffsets);
    }

    public Set<TopicPartition> partitionsNeedingReset(long nowMs) {
        return collectPartitions(state -> state.awaitingReset() && !state.awaitingRetryBackoff(nowMs),
                Collectors.toSet());
    }

    public boolean isAssigned(TopicPartition tp) {
        return assignment.contains(tp);
    }

    public boolean isPaused(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).paused;
    }

    public boolean isFetchable(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).isFetchable();
    }

    public boolean hasValidPosition(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).hasValidPosition();
    }

    public void pause(TopicPartition tp) {
        assignedState(tp).pause();
    }

    public void resume(TopicPartition tp) {
        assignedState(tp).resume();
    }

    public void resetFailed(Set<TopicPartition> partitions, long nextRetryTimeMs) {
        for (TopicPartition partition : partitions)
            assignedState(partition).resetFailed(nextRetryTimeMs);
    }

    public void movePartitionToEnd(TopicPartition tp) {
        assignment.moveToEnd(tp);
    }

    public ConsumerRebalanceListener rebalanceListener() {
        return rebalanceListener;
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public void fireOnAssignment(Set<TopicPartition> assignment) {
        for (Listener listener : listeners)
            listener.onAssignment(assignment);
    }

    private static Map<TopicPartition, TopicPartitionState> partitionToStateMap(Collection<TopicPartition> assignments) {
        Map<TopicPartition, TopicPartitionState> map = new HashMap<>(assignments.size());
        for (TopicPartition tp : assignments)
            map.put(tp, new TopicPartitionState());
        return map;
    }

    private static class TopicPartitionState {

        private enum FetchState {
            INITIALIZING,
            FETCHING,
            AWAIT_RESET,
            AWAIT_VALIDATION
        }

        private FetchState state;
        private FetchPosition position; // last consumed position

        private Long highWatermark; // the high watermark from last fetch
        private Long logStartOffset; // the log start offset
        private Long lastStableOffset;
        private boolean paused;  // whether this partition has been paused by the user
        private OffsetResetStrategy resetStrategy;  // the strategy to use if the offset needs resetting
        private Long nextRetryTimeMs;

        TopicPartitionState() {
            this.paused = false;
            this.state = FetchState.INITIALIZING;
            this.position = null;
            this.highWatermark = null;
            this.logStartOffset = null;
            this.lastStableOffset = null;
            this.resetStrategy = null;
            this.nextRetryTimeMs = null;
        }

        private void reset(OffsetResetStrategy strategy) {
            this.state = FetchState.AWAIT_RESET;
            this.resetStrategy = strategy;
            this.position = null;
            this.nextRetryTimeMs = null;
        }

        private boolean needsValidation(Metadata.LeaderAndEpoch currentLeader) {
            if (!hasPosition())
                throw new IllegalStateException("Cannot validate offset while partition is in state " + state);

            this.position = new FetchPosition(position.offset, position.lastFetchEpoch, currentLeader);
            // If we have no epoch information for the current position, then we can skip validation.
            if (position.lastFetchEpoch.isPresent()) {
                this.state = FetchState.AWAIT_VALIDATION;
                return true;
            } else {
                this.state = FetchState.FETCHING;
                return false;
            }
        }

        private void validate() {
            if (hasPosition() && state == FetchState.AWAIT_VALIDATION) {
                state = FetchState.FETCHING;
            }
        }

        private boolean awaitingRetryBackoff(long nowMs) {
            return nextRetryTimeMs != null && nowMs < nextRetryTimeMs;
        }

        private boolean awaitingReset() {
            return state == FetchState.AWAIT_RESET;
        }

        private void setNextAllowedRetry(long nextAllowedRetryTimeMs) {
            this.nextRetryTimeMs = nextAllowedRetryTimeMs;
        }

        private void resetFailed(long nextAllowedRetryTimeMs) {
            this.nextRetryTimeMs = nextAllowedRetryTimeMs;
        }

        private boolean hasValidPosition() {
            return state == FetchState.FETCHING;
        }

        private boolean hasPosition() {
            return state == FetchState.FETCHING || state == FetchState.AWAIT_VALIDATION || state == FetchState.AWAIT_RESET;
        }

        private boolean isPaused() {
            return paused;
        }

        private void seek(FetchPosition position) {
            this.state = FetchState.FETCHING;
            this.position = position;
            this.resetStrategy = null;
            this.nextRetryTimeMs = null;
        }

        private void position(FetchPosition position) {
            if (!hasValidPosition())
                throw new IllegalStateException("Cannot set a new position without a valid current position");
            this.position = position;
        }

        private void pause() {
            this.paused = true;
        }

        private void resume() {
            this.paused = false;
        }

        private boolean isFetchable() {
            return !paused && hasValidPosition();
        }

    }

    public interface Listener {
        /**
         * Fired after a new assignment is received (after a group rebalance or when the user manually changes the
         * assignment).
         *
         * @param assignment The topic partitions assigned to the consumer
         */
        void onAssignment(Set<TopicPartition> assignment);
    }

    public static class FetchPosition {
        public final long offset;
        public final Optional<Integer> lastFetchEpoch;
        public final Metadata.LeaderAndEpoch currentLeader;

        public FetchPosition(long offset, Optional<Integer> lastFetchEpoch, Metadata.LeaderAndEpoch currentLeader) {
            this.offset = offset;
            this.lastFetchEpoch = Objects.requireNonNull(lastFetchEpoch);
            this.currentLeader = Objects.requireNonNull(currentLeader);
        }

        public boolean safeToFetchFrom(Metadata.LeaderAndEpoch leaderAndEpoch) {
            // Should we check the if the epoch is present or do we get a benefit
            // for leader changes on older versions of the broker?

            // I think the answer is 'no' because the OffsetsForLeaderEpoch API was
            // not exposed in older versions anyway.
            if (currentLeader.leader.isEmpty() || leaderAndEpoch.leader.isEmpty()) {
                return false;
            } else {
                return currentLeader.equals(leaderAndEpoch);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FetchPosition that = (FetchPosition) o;

            if (offset != that.offset) return false;
            return lastFetchEpoch.equals(that.lastFetchEpoch);
        }

        @Override
        public int hashCode() {
            int result = (int) (offset ^ (offset >>> 32));
            result = 31 * result + lastFetchEpoch.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "FetchPosition(" +
                    "offset=" + offset +
                    ", lastFetchEpoch=" + lastFetchEpoch +
                    ')';
        }
    }
}
