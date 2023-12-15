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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.internals.CachedSupplier;
import org.apache.kafka.clients.consumer.internals.CommitRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.clients.consumer.internals.HeartbeatRequestManager;
import org.apache.kafka.clients.consumer.internals.MembershipManager;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * An {@link EventProcessor} that is created and executes in the {@link ConsumerNetworkThread network thread}
 * which processes {@link ApplicationEvent application events} generated by the application thread.
 */
public class ApplicationEventProcessor extends EventProcessor<ApplicationEvent> {

    private final Logger log;
    private final ConsumerMetadata metadata;
    private final RequestManagers requestManagers;

    public ApplicationEventProcessor(final LogContext logContext,
                                     final BlockingQueue<ApplicationEvent> applicationEventQueue,
                                     final RequestManagers requestManagers,
                                     final ConsumerMetadata metadata) {
        super(logContext, applicationEventQueue);
        this.log = logContext.logger(ApplicationEventProcessor.class);
        this.requestManagers = requestManagers;
        this.metadata = metadata;
    }

    /**
     * Process the events—if any—that were produced by the application thread. It is possible that when processing
     * an event generates an error. In such cases, the processor will log an exception, but we do not want those
     * errors to be propagated to the caller.
     */
    @Override
    public void process() {
        process((event, error) -> { });
    }

    @Override
    public void process(ApplicationEvent event) {
        switch (event.type()) {
            case COMMIT:
                process((CommitApplicationEvent) event);
                return;

            case POLL:
                process((PollApplicationEvent) event);
                return;

            case FETCH_COMMITTED_OFFSETS:
                process((FetchCommittedOffsetsApplicationEvent) event);
                return;

            case NEW_TOPICS_METADATA_UPDATE:
                process((NewTopicsMetadataUpdateRequestEvent) event);
                return;

            case ASSIGNMENT_CHANGE:
                process((AssignmentChangeApplicationEvent) event);
                return;

            case TOPIC_METADATA:
                process((TopicMetadataApplicationEvent) event);
                return;

            case LIST_OFFSETS:
                process((ListOffsetsApplicationEvent) event);
                return;

            case RESET_POSITIONS:
                process((ResetPositionsApplicationEvent) event);
                return;

            case VALIDATE_POSITIONS:
                process((ValidatePositionsApplicationEvent) event);
                return;

            case SUBSCRIPTION_CHANGE:
                process((SubscriptionChangeApplicationEvent) event);
                return;

            case UNSUBSCRIBE:
                process((UnsubscribeApplicationEvent) event);
                return;

            default:
                log.warn("Application event type " + event.type() + " was not expected");
        }
    }

    @Override
    protected Class<ApplicationEvent> getEventClass() {
        return ApplicationEvent.class;
    }

    private void process(final PollApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return;
        }

        requestManagers.commitRequestManager.ifPresent(m -> m.updateAutoCommitTimer(event.pollTimeMs()));
        requestManagers.heartbeatRequestManager.ifPresent(HeartbeatRequestManager::resetPollTimer);
    }

    private void process(final CommitApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            // Leaving this error handling here, but it is a bit strange as the commit API should enforce the group.id
            // upfront, so we should never get to this block.
            Exception exception = new KafkaException("Unable to commit offset. Most likely because the group.id wasn't set");
            event.future().completeExceptionally(exception);
            return;
        }

        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        Optional<Long> expirationTimeMs = event.retryTimeoutMs().map(this::getExpirationTimeForTimeout);
        event.chain(manager.addOffsetCommitRequest(event.offsets(), expirationTimeMs, false));
    }

    private void process(final FetchCommittedOffsetsApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            event.future().completeExceptionally(new KafkaException("Unable to fetch committed " +
                    "offset because the CommittedRequestManager is not available. Check if group.id was set correctly"));
            return;
        }
        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        long expirationTimeMs = getExpirationTimeForTimeout(event.timeout());
        event.chain(manager.addOffsetFetchRequest(event.partitions(), expirationTimeMs));
    }

    private void process(final NewTopicsMetadataUpdateRequestEvent ignored) {
        metadata.requestUpdateForNewTopics();
    }


    /**
     * Commit all consumed if auto-commit is enabled. Note this will trigger an async commit,
     * that will not be retried if the commit request fails.
     */
    private void process(final AssignmentChangeApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return;
        }
        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        manager.updateAutoCommitTimer(event.currentTimeMs());
        manager.maybeAutoCommitAllConsumedAsync();
    }

    private void process(final ListOffsetsApplicationEvent event) {
        final CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> future =
                requestManagers.offsetsRequestManager.fetchOffsets(event.timestampsToSearch(),
                        event.requireTimestamps());
        event.chain(future);
    }

    /**
     * Process event that indicates that the subscription changed. This will make the
     * consumer join the group if it is not part of it yet, or send the updated subscription if
     * it is already a member.
     */
    private void process(final SubscriptionChangeApplicationEvent event) {
        if (!requestManagers.membershipManager.isPresent()) {
            throw new RuntimeException("Group membership manager not present when processing a " +
                    "subscribe event");
        }
        MembershipManager membershipManager = requestManagers.membershipManager.get();
        membershipManager.onSubscriptionUpdated();
    }

    /**
     * Process event indicating that the consumer unsubscribed from all topics. This will make
     * the consumer release its assignment and send a request to leave the group.
     *
     * @param event Unsubscribe event containing a future that will complete when the callback
     *              execution for releasing the assignment completes, and the request to leave
     *              the group is sent out.
     */
    private void process(final UnsubscribeApplicationEvent event) {
        if (!requestManagers.membershipManager.isPresent()) {
            throw new RuntimeException("Group membership manager not present when processing an " +
                    "unsubscribe event");
        }
        MembershipManager membershipManager = requestManagers.membershipManager.get();
        CompletableFuture<Void> result = membershipManager.leaveGroup();
        event.chain(result);
    }

    private void process(final ResetPositionsApplicationEvent event) {
        CompletableFuture<Void> result = requestManagers.offsetsRequestManager.resetPositionsIfNeeded();
        event.chain(result);
    }

    private void process(final ValidatePositionsApplicationEvent event) {
        CompletableFuture<Void> result = requestManagers.offsetsRequestManager.validatePositionsIfNeeded();
        event.chain(result);
    }

    private void process(final TopicMetadataApplicationEvent event) {
        final CompletableFuture<Map<String, List<PartitionInfo>>> future;

        long expirationTimeMs =
            (event.getTimeoutMs() == Long.MAX_VALUE) ? Long.MAX_VALUE : System.currentTimeMillis() + event.getTimeoutMs();
        if (event.isAllTopics()) {
            future = requestManagers.topicMetadataRequestManager.requestAllTopicsMetadata(expirationTimeMs);
        } else {
            future = requestManagers.topicMetadataRequestManager.requestTopicMetadata(event.topic(), expirationTimeMs);
        }

        event.chain(future);
    }

    private long getExpirationTimeForTimeout(final long timeoutMs) {
        return (timeoutMs == Long.MAX_VALUE) ? Long.MAX_VALUE : System.currentTimeMillis() + timeoutMs;
    }

    /**
     * Creates a {@link Supplier} for deferred creation during invocation by
     * {@link ConsumerNetworkThread}.
     */
    public static Supplier<ApplicationEventProcessor> supplier(final LogContext logContext,
                                                               final ConsumerMetadata metadata,
                                                               final BlockingQueue<ApplicationEvent> applicationEventQueue,
                                                               final Supplier<RequestManagers> requestManagersSupplier) {
        return new CachedSupplier<ApplicationEventProcessor>() {
            @Override
            protected ApplicationEventProcessor create() {
                RequestManagers requestManagers = requestManagersSupplier.get();
                return new ApplicationEventProcessor(
                        logContext,
                        applicationEventQueue,
                        requestManagers,
                        metadata
                );
            }
        };
    }
}
