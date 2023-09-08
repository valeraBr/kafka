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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.AssignmentReconciler.ReconciliationResult;
import org.apache.kafka.clients.consumer.internals.events.AssignPartitionsEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData.Assignment;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData.TopicPartitions;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.AssignmentReconciler.ReconciliationResult.RECONCILING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AssignmentReconcilerTest {

    private SubscriptionState subscriptions;
    private BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private AssignmentReconciler reconciler;

    @Test
    public void testBasic() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "test-topic";
        setup(Collections.singletonMap(topicName, topicId));

        // Create our test partitions
        TopicPartitions testPartitions = newTopicPartitions(topicId, 0, 1, 2, 3);

        // Create our assignment with the partitions from set A
        Optional<Assignment> assignment = newAssignment(testPartitions);

        // Start the reconciliation process. At this point, since there are no partitions assigned to our
        // subscriptions, we don't need to revoke anything. Validate that after our initial step that we haven't
        // prematurely assigned anything to the SubscriptionState and that our result is RECONCILING.
        ReconciliationResult result = reconciler.maybeReconcile(assignment);
        assertEquals(Collections.emptySet(), subscriptions.assignedPartitions());
        assertEquals(result, RECONCILING);

        // This is intentionally superfluous. We're just checking that we're in the same state as
        // the last time we called maybeReconcile. Because we haven't executed the ConsumerRebalanceListener,
        // the state of the reconciliation is still in progress.
        result = reconciler.maybeReconcile(assignment);
        assertEquals(result, RECONCILING);

        // Grab the background event. Because we didn't remove any partitions, but only added them, jump
        // directly to the assign partitions. Let's verify that there's an appropriate event on the
        // background event queue, it has the correct partitions, and the future is there but not complete.
        AssignPartitionsEvent event = pollBackgroundEvent(AssignPartitionsEvent.class);
        assertEquals(newTopicPartitions(topicName, 0, 1, 2, 3),  event.partitions());
        CompletableFuture<Void> future = event.future();
        assertNotNull(future);
        assertFalse(future.isDone());

        // Complete the future to signal to the reconciler that the ConsumerRebalanceListener callback
        // has completed. This will trigger the "commit" of the partition assignment to the SubscriptionState.
        assertEquals(Collections.emptySet(), subscriptions.assignedPartitions());
        future.complete(null);
        assertEquals(newTopicPartitions(topicName, 0, 1, 2, 3), subscriptions.assignedPartitions());

        // Call the reconciler and verify that it did "commit" the partition assignment as expected.
        result = reconciler.maybeReconcile(assignment);
        assertEquals(result, ReconciliationResult.APPLIED_LOCALLY);
    }

    private TopicPartitions newTopicPartitions(Uuid topicId, Integer... partitions) {
        TopicPartitions topicPartitions = new TopicPartitions();

        if (topicId != null)
            topicPartitions.setTopicId(topicId);

        if (partitions != null)
            topicPartitions.setPartitions(Arrays.asList(partitions));

        return topicPartitions;
    }

    private SortedSet<TopicPartition> newTopicPartitions(String topicName, Integer... partitions) {
        SortedSet<TopicPartition> topicPartitions = new TreeSet<>(new Utils.TopicPartitionComparator());

        if (partitions != null) {
            for (int partition : partitions)
                topicPartitions.add(new TopicPartition(topicName, partition));
        }

        return topicPartitions;
    }

    private Optional<Assignment> newAssignment(TopicPartitions a) {
        Assignment assignment = new Assignment();
        assignment.setAssignedTopicPartitions(Collections.singletonList(a));
        return Optional.of(assignment);
    }

    private void setup(Map<String, Uuid> topics) {
        setup(topics, new NoOpConsumerRebalanceListener());
    }
    private void setup(Map<String, Uuid> topics, ConsumerRebalanceListener listener) {
        LogContext logContext = new LogContext();

        // Create our SubscriptionState and subscribe to the topics.
        subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        subscriptions.subscribe(topics.keySet(), listener);

        // Create our metadata and ensure at has our topic name to topic ID mapping.
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWithIds(
                "dummy",
                1,
                Collections.emptyMap(),
                topics.keySet().stream().collect(Collectors.toMap(t -> t, t -> 3)),
                tp -> 0,
                topics);
        Properties props = new Properties();
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        ConsumerConfig config = new ConsumerConfig(props);
        ConsumerMetadata metadata = new ConsumerMetadata(config, subscriptions, logContext, new ClusterResourceListeners());
        metadata.updateWithCurrentRequestVersion(metadataResponse, false, 0L);

        // We need the background event queue to check for the events from the network thread to the application thread
        // to signal the ConsumerRebalanceListener callbacks.
        backgroundEventQueue = new LinkedBlockingQueue<>();

        reconciler = new AssignmentReconciler(logContext, subscriptions, metadata, backgroundEventQueue);
    }

    private <T> T pollBackgroundEvent(Class<T> expectedEventType) {
        BackgroundEvent e = backgroundEventQueue.poll();
        assertNotNull(e);
        assertEquals(e.getClass(), expectedEventType);
        return expectedEventType.cast(e);
    }
}
