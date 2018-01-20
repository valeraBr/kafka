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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * The result of the {@link AdminClient#listGroupOffsets()} call.
 * <p>
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class ListGroupOffsetsResult {

    final KafkaFuture<Map<TopicPartition, GroupOffsetListing>> future;

    ListGroupOffsetsResult(KafkaFuture<Map<TopicPartition, GroupOffsetListing>> future) {
        this.future = future;
    }

    /**
     * Return a future which yields a map of topic partitions to GroupOffsetListing objects.
     */
    public KafkaFuture<Map<TopicPartition, GroupOffsetListing>> namesToListings() {
        return future;
    }

    /**
     * Return a future which yields a collection of GroupOffsetListing objects.
     */
    public KafkaFuture<Collection<GroupOffsetListing>> listings() {
        return future.thenApply(new KafkaFuture.Function<Map<TopicPartition, GroupOffsetListing>, Collection<GroupOffsetListing>>() {
            @Override
            public Collection<GroupOffsetListing> apply(Map<TopicPartition, GroupOffsetListing> namesToDescriptions) {
                return namesToDescriptions.values();
            }
        });
    }

    /**
     * Return a future which yields a collection of topic partitions.
     */
    public KafkaFuture<Set<TopicPartition>> topicPartitions() {
        return future.thenApply(new KafkaFuture.Function<Map<TopicPartition, GroupOffsetListing>, Set<TopicPartition>>() {
            @Override
            public Set<TopicPartition> apply(Map<TopicPartition, GroupOffsetListing> namesToListings) {
                return namesToListings.keySet();
            }
        });
    }
}
