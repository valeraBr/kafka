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
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Map;

/**
 * The result of the {@link AdminClient#listTopics()} call.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class ListTopicsResult {
    final KafkaFuture<Map<String, TopicListItem>> future;

    ListTopicsResult(KafkaFuture<Map<String, TopicListItem>> future) {
        this.future = future;
    }

    /**
     * Return a future which yields a map of topic names to TopicListing objects.
     */
    public KafkaFuture<Map<String, TopicListItem>> namesToItems() {
        return future;
    }

    /**
     * Return a future which yields a collection of TopicListing objects.
     */
    public KafkaFuture<Collection<TopicListItem>> items() {
        return future.thenApply(new KafkaFuture.Function<Map<String, TopicListItem>, Collection<TopicListItem>>() {
            @Override
            public Collection<TopicListItem> apply(Map<String, TopicListItem> namesToDescriptions) {
                return namesToDescriptions.values();
            }
        });
    }

    /**
     * Return a future which yields a collection of topic names.
     */
    public KafkaFuture<Collection<String>> names() {
        return future.thenApply(new KafkaFuture.Function<Map<String, TopicListItem>, Collection<String>>() {
            @Override
            public Collection<String> apply(Map<String, TopicListItem> namesToDescriptions) {
                return namesToDescriptions.keySet();
            }
        });
    }
}
