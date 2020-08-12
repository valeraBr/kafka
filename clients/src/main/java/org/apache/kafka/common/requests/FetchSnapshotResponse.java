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
package org.apache.kafka.common.requests;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

final public class FetchSnapshotResponse extends AbstractResponse {
    public final FetchSnapshotResponseData data;

    public FetchSnapshotResponse(FetchSnapshotResponseData data) {
        this.data = data;
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errors = new HashMap<>();

        Errors topLevelError = Errors.forCode(data.errorCode());
        if (topLevelError != Errors.NONE) {
            errors.put(topLevelError, 1);
        }

        for (FetchSnapshotResponseData.TopicSnapshot topicResponse : data.topics()) {
            for (FetchSnapshotResponseData.PartitionSnapshot partitionResponse : topicResponse.partitions()) {
                errors.compute(Errors.forCode(partitionResponse.errorCode()),
                    (error, count) -> count == null ? 1 : count + 1);
            }
        }

        return errors;
    }

    public static FetchSnapshotResponseData withTopError(Errors error) {
        return new FetchSnapshotResponseData().setErrorCode(error.code());
    }

    public static FetchSnapshotResponseData singletonWithError(TopicPartition topicPartition, Errors error) {
        return new FetchSnapshotResponseData()
            .setTopics(
                Collections.singletonList(
                    new FetchSnapshotResponseData.TopicSnapshot()
                        .setName(topicPartition.topic())
                        .setPartitions(
                            Collections.singletonList(
                                new FetchSnapshotResponseData.PartitionSnapshot()
                                    .setIndex(topicPartition.partition())
                                    .setErrorCode(error.code())
                            )
                        )
                )
            );
    }

    public static FetchSnapshotResponseData singletonWithData(
        TopicPartition topicPartition,
        UnaryOperator<FetchSnapshotResponseData.PartitionSnapshot> operator
    ) {
        FetchSnapshotResponseData.PartitionSnapshot partitionSnapshot = operator.apply(
            new FetchSnapshotResponseData.PartitionSnapshot().setIndex(topicPartition.partition())
        );

        return new FetchSnapshotResponseData()
            .setTopics(
                Collections.singletonList(
                    new FetchSnapshotResponseData.TopicSnapshot()
                        .setName(topicPartition.topic())
                        .setPartitions(Collections.singletonList(partitionSnapshot))
                )
            );
    }

    // TODO: write documentation. This function assumes that topic partitions are unique in `data`
    public static Optional<FetchSnapshotResponseData.PartitionSnapshot> forTopicPartition(
        FetchSnapshotResponseData data,
        TopicPartition topicPartition
    ) {
        return data
            .topics()
            .stream()
            .filter(topic -> topic.name().equals(topicPartition.topic()))
            .flatMap(topic -> topic.partitions().stream())
            .filter(parition -> parition.index() == topicPartition.partition())
            .findAny();
    }

}
