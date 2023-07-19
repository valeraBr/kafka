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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class AssignmentTopicMetadata implements AssignmentTopicDescriber {

    Map<Uuid, PartitionMetadata> TopicPartitionMetadata;

    public AssignmentTopicMetadata(Map<Uuid, PartitionMetadata> TopicPartitionMetadata) {
        this.TopicPartitionMetadata = TopicPartitionMetadata;
    }

    /**
     * Returns a set of subscribed topicIds.
     *
     * @return Set of topicIds corresponding to the subscribed topics.
     */
    @Override
    public Set<Uuid> subscribedTopicIds() {
        return TopicPartitionMetadata.keySet();
    }

    /**
     * Number of partitions for the given topicId.
     *
     * @param topicId   Uuid corresponding to the topic.
     * @return The number of partitions corresponding to the given topicId.
     *         If the topicId doesn't exist return 0;
     */
    @Override
    public int numPartitions(Uuid topicId) {
        PartitionMetadata partitionMetadata = TopicPartitionMetadata.get(topicId);
        if (partitionMetadata == null) {
            return 0;
        }

        return partitionMetadata.numPartitions();
    }

    /**
     * Returns all the racks associated with the replicas for the given partition.
     *
     * @param topicId   Uuid corresponding to the partition's topic.
     * @param partition Partition number within topic.
     * @return The set of racks corresponding to the replicas of the topics partition.
     *         If the topicId doesn't exist return an empty set;
     */
    @Override
    public Set<String> racksForPartition(Uuid topicId, int partition) {
        PartitionMetadata partitionMetadata = TopicPartitionMetadata.get(topicId);
        if (partitionMetadata == null) {
            return Collections.emptySet();
        }

        return partitionMetadata.racks(partition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AssignmentTopicMetadata)) return false;
        AssignmentTopicMetadata that = (AssignmentTopicMetadata) o;
        return TopicPartitionMetadata.equals(that.TopicPartitionMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(TopicPartitionMetadata);
    }

    @Override
    public String toString() {
        return "AssignmentTopicMetadata{" +
            "TopicPartitionMetadata=" + TopicPartitionMetadata +
            '}';
    }
}
