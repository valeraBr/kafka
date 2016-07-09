/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.state.HostInfo;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Listen for changes in the mapping from {@link HostInfo} -> {@link TopicPartition}s
 * This will be invoked when a rebalance occurs
 */
public interface PartitionsByHostStateChangeListener {
    /**
     * Invoked by {@link StreamThread#rebalanceListener} during
     * {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(Collection)}
     * and {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked(Collection)}
     * @param currentState  the current mapping of {@link HostInfo} -> {@link TopicPartition}s
     */
    void onChange(final Map<HostInfo, Set<TopicPartition>> currentState);
}
