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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An internal mutable cache of nodes, topics, and partitions in the Kafka cluster. This keeps an up-to-date Cluster
 * instance which is optimized for read access.
 */
class MetadataCache {
    private final String clusterId;
    private final List<Node> nodes;
    private final Set<String> unauthorizedTopics;
    private final Set<String> invalidTopics;
    private final Set<String> internalTopics;
    private final Node controller;
    private final Map<TopicPartition, PartitionInfoAndEpoch> partitionsByTopicPartition;

    private Cluster clusterInstance;

    MetadataCache(String clusterId,
                  List<Node> nodes,
                  Collection<PartitionInfoAndEpoch> partitions,
                  Set<String> unauthorizedTopics,
                  Set<String> invalidTopics,
                  Set<String> internalTopics,
                  Node controller) {
        this(clusterId, nodes, partitions, unauthorizedTopics, invalidTopics, internalTopics, controller, null);
    }

    MetadataCache(String clusterId,
                  List<Node> nodes,
                  Collection<PartitionInfoAndEpoch> partitions,
                  Set<String> unauthorizedTopics,
                  Set<String> invalidTopics,
                  Set<String> internalTopics,
                  Node controller,
                  Cluster clusterInstance) {
        this.clusterId = clusterId;
        this.nodes = nodes;
        this.unauthorizedTopics = unauthorizedTopics;
        this.invalidTopics = invalidTopics;
        this.internalTopics = internalTopics;
        this.controller = controller;

        this.partitionsByTopicPartition = new HashMap<>(partitions.size());
        for (PartitionInfoAndEpoch p : partitions) {
            this.partitionsByTopicPartition.put(new TopicPartition(p.partitionInfo().topic(), p.partitionInfo().partition()), p);
        }

        if (clusterInstance == null) {
            computeClusterView();
        } else {
            this.clusterInstance = clusterInstance;
        }
    }

    /**
     * Return the cached PartitionInfo iff it was for the given epoch
     * @param topicPartition
     * @param epoch
     * @return
     */
    Optional<PartitionInfo> getPartitionInfoHavingEpoch(TopicPartition topicPartition, int epoch) {
        PartitionInfoAndEpoch infoAndEpoch = partitionsByTopicPartition.get(topicPartition);
        if (infoAndEpoch == null) {
            return Optional.empty();
        } else {
            if (infoAndEpoch.epoch() == epoch) {
                return Optional.of(infoAndEpoch.partitionInfo());
            } else {
                return Optional.empty();
            }
        }
    }

    Cluster cluster() {
        if (clusterInstance == null) {
            throw new IllegalStateException("Cached Cluster instance should not be null, but was.");
        } else {
            return clusterInstance;
        }
    }

    /*synchronized boolean removePartition(TopicPartition topicPartition) {
        if (partitionsByTopicPartition.remove(topicPartition) != null) {
            //updateClusterView();
            return true;
        } else {
            return false;
        }
    }*/

    private void computeClusterView() {
        List<PartitionInfo> partitionInfos = partitionsByTopicPartition.values()
                .stream()
                .map(PartitionInfoAndEpoch::partitionInfo)
                .collect(Collectors.toList());
        this.clusterInstance = new Cluster(clusterId, nodes, partitionInfos, unauthorizedTopics, invalidTopics, internalTopics, controller);
    }

    static MetadataCache bootstrap(List<InetSocketAddress> addresses) {
        List<Node> nodes = new ArrayList<>();
        int nodeId = -1;
        for (InetSocketAddress address : addresses)
            nodes.add(new Node(nodeId--, address.getHostString(), address.getPort()));
        return new MetadataCache(null, nodes, Collections.emptyList(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Cluster.bootstrap(addresses));
    }

    static MetadataCache empty() {
        return new MetadataCache(null, Collections.emptyList(), Collections.emptyList(),
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Cluster.empty());
    }

    @Override
    public String toString() {
        return "MetadataCache{" +
                "cluster=" + cluster() +
                '}';
    }

    static class PartitionInfoAndEpoch {
        private final PartitionInfo partitionInfo;
        private final int epoch;

        PartitionInfoAndEpoch(PartitionInfo partitionInfo, int epoch) {
            this.partitionInfo = partitionInfo;
            this.epoch = epoch;
        }

        public PartitionInfo partitionInfo() {
            return partitionInfo;
        }

        public int epoch() {
            return epoch;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PartitionInfoAndEpoch that = (PartitionInfoAndEpoch) o;
            return epoch == that.epoch &&
                    Objects.equals(partitionInfo, that.partitionInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionInfo, epoch);
        }

        @Override
        public String toString() {
            return "PartitionInfoAndEpoch{" +
                    "partitionInfo=" + partitionInfo +
                    ", epoch=" + epoch +
                    '}';
        }
    }
}
