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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DescribeDirsResponse extends AbstractResponse {

    // request level key names
    private static final String LOG_DIRS_KEY_NAME = "log_dirs";

    // dir level key names
    private static final String ERROR_CODE_KEY_NAME = "error_code";
    private static final String PATH_KEY_NAME = "path";
    private static final String TOPICS_KEY_NAME = "topics";

    // topic level key names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITIONS_KEY_NAME = "partitions";

    // partition level key names
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String SIZE_KEY_NAME = "size";
    private static final String LOG_END_OFFSET_KEY_NAME = "log_end_offset";
    private static final String IS_TEMPORARY_KEY_NAME = "is_temporary";

    private final int throttleTimeMs;
    private final Map<String, LogDirInfo> logDirInfos;

    public DescribeDirsResponse(Struct struct) {
        throttleTimeMs = struct.getInt(THROTTLE_TIME_KEY_NAME);
        logDirInfos = new HashMap<>();

        for (Object logDirStructObj : struct.getArray(LOG_DIRS_KEY_NAME)) {
            Struct logDirStruct = (Struct) logDirStructObj;
            Errors error = Errors.forCode(logDirStruct.getShort(ERROR_CODE_KEY_NAME));
            String path = logDirStruct.getString(PATH_KEY_NAME);
            Map<TopicPartition, ReplicaInfo> replicaInfos = new HashMap<>();

            for (Object topicStructObj : logDirStruct.getArray(TOPICS_KEY_NAME)) {
                Struct topicStruct = (Struct) topicStructObj;
                String topic = topicStruct.getString(TOPIC_KEY_NAME);

                for (Object partitionStructObj : topicStruct.getArray(PARTITIONS_KEY_NAME)) {
                    Struct partitionStruct = (Struct) partitionStructObj;
                    int partition = partitionStruct.getInt(PARTITION_KEY_NAME);
                    long size = partitionStruct.getLong(SIZE_KEY_NAME);
                    long logEndOffset = partitionStruct.getLong(LOG_END_OFFSET_KEY_NAME);
                    boolean isTemporary = partitionStruct.getBoolean(IS_TEMPORARY_KEY_NAME);
                    ReplicaInfo replicaInfo = new ReplicaInfo(size, logEndOffset, isTemporary);
                    replicaInfos.put(new TopicPartition(topic, partition), replicaInfo);
                }
            }

            logDirInfos.put(path, new LogDirInfo(error, replicaInfos));
        }
    }

    /**
     * Constructor for version 0.
     */
    public DescribeDirsResponse(int throttleTimeMs, Map<String, LogDirInfo> logDirInfos) {
        this.throttleTimeMs = throttleTimeMs;
        this.logDirInfos = logDirInfos;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.DESCRIBE_DIRS.responseSchema(version));
        struct.set(THROTTLE_TIME_KEY_NAME, throttleTimeMs);
        List<Struct> logDirStructArray = new ArrayList<>();
        for (Map.Entry<String, LogDirInfo> logDirInfosEntry : logDirInfos.entrySet()) {
            LogDirInfo logDirInfo = logDirInfosEntry.getValue();
            Struct logDirStruct = struct.instance(LOG_DIRS_KEY_NAME);
            logDirStruct.set(ERROR_CODE_KEY_NAME, logDirInfo.error.code());
            logDirStruct.set(PATH_KEY_NAME, logDirInfosEntry.getKey());

            Map<String, Map<Integer, ReplicaInfo>> replicaInfosByTopic = CollectionUtils.groupDataByTopic(logDirInfo.replicaInfos);
            List<Struct> topicStructArray = new ArrayList<>();
            for (Map.Entry<String, Map<Integer, ReplicaInfo>> replicaInfosByTopicEntry : replicaInfosByTopic.entrySet()) {
                Struct topicStruct = logDirStruct.instance(TOPICS_KEY_NAME);
                topicStruct.set(TOPIC_KEY_NAME, replicaInfosByTopicEntry.getKey());
                List<Struct> partitionStructArray = new ArrayList<>();

                for (Map.Entry<Integer, ReplicaInfo> replicaInfosByPartitionEntry : replicaInfosByTopicEntry.getValue().entrySet()) {
                    Struct partitionStruct = topicStruct.instance(PARTITIONS_KEY_NAME);
                    ReplicaInfo replicaInfo = replicaInfosByPartitionEntry.getValue();
                    partitionStruct.set(PARTITION_KEY_NAME, replicaInfosByPartitionEntry.getKey());
                    partitionStruct.set(SIZE_KEY_NAME, replicaInfo.size);
                    partitionStruct.set(LOG_END_OFFSET_KEY_NAME, replicaInfo.logEndOffset);
                    partitionStruct.set(IS_TEMPORARY_KEY_NAME, replicaInfo.isTemporary);
                    partitionStructArray.add(partitionStruct);
                }
                topicStruct.set(PARTITIONS_KEY_NAME, partitionStructArray.toArray());
                topicStructArray.add(topicStruct);
            }
            logDirStruct.set(TOPICS_KEY_NAME, topicStructArray.toArray());
            logDirStructArray.add(logDirStruct);
        }
        struct.set(LOG_DIRS_KEY_NAME, logDirStructArray.toArray());
        return struct;
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Map<String, LogDirInfo> logDirInfos() {
        return logDirInfos;
    }

    public static DescribeDirsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeDirsResponse(ApiKeys.DESCRIBE_DIRS.responseSchema(version).read(buffer));
    }

    /**
     * Possible error code:
     *
     * DIR_NOT_AVAILABLE (57)
     * KAFKA_STORAGE_ERROR (56)
     * UNKNOWN (-1)
     */
    static public class LogDirInfo {
        public final Errors error;
        public final Map<TopicPartition, ReplicaInfo> replicaInfos;

        public LogDirInfo(Errors error, Map<TopicPartition, ReplicaInfo> replicaInfos) {
            this.error = error;
            this.replicaInfos = replicaInfos;
        }
    }

    static public class ReplicaInfo {

        public final long size;
        public final long logEndOffset;
        public final boolean isTemporary;

        public ReplicaInfo(long size, long logEndOffset, boolean isTemporary) {
            this.size = size;
            this.logEndOffset = logEndOffset;
            this.isTemporary = isTemporary;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("ReplicaInfo(size=")
                .append(size)
                .append(", logEndOffset=")
                .append(logEndOffset)
                .append(", isTemporary=")
                .append(isTemporary)
                .append(")");
            return builder.toString();
        }
    }
}