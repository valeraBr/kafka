/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.test;


import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MockConsumerInterceptor implements ClusterResourceListener, ConsumerInterceptor<String, String> {
    public static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
    public static final AtomicInteger CLOSE_COUNT = new AtomicInteger(0);
    public static final AtomicInteger ON_COMMIT_COUNT = new AtomicInteger(0);
    public static final AtomicBoolean IS_CLUSTER_ID_PRESENT_BEFORE_ON_CONSUME = new AtomicBoolean();
    public static final AtomicReference<ClusterResource> CLUSTER_META = new AtomicReference<>();


    public MockConsumerInterceptor() {
        INIT_COUNT.incrementAndGet();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // clientId must be in configs
        Object clientIdValue = configs.get(ConsumerConfig.CLIENT_ID_CONFIG);
        if (clientIdValue == null)
            throw new ConfigException("Mock consumer interceptor expects configuration " + ProducerConfig.CLIENT_ID_CONFIG);
    }

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        if (CLUSTER_META.get() != null
                && CLUSTER_META.get().getClusterId() != null
                && CLUSTER_META.get().getClusterId().length() == 48)
            IS_CLUSTER_ID_PRESENT_BEFORE_ON_CONSUME.set(true);

        Map<TopicPartition, List<ConsumerRecord<String, String>>> recordMap = new HashMap<>();
        for (TopicPartition tp : records.partitions()) {
            List<ConsumerRecord<String, String>> lst = new ArrayList<>();
            for (ConsumerRecord<String, String> record: records.records(tp)) {
                lst.add(new ConsumerRecord<>(record.topic(), record.partition(), record.offset(),
                                             record.timestamp(), record.timestampType(),
                                             record.checksum(), record.serializedKeySize(),
                                             record.serializedValueSize(),
                                             record.key(), record.value().toUpperCase(Locale.ROOT)));
            }
            recordMap.put(tp, lst);
        }
        return new ConsumerRecords<String, String>(recordMap);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        ON_COMMIT_COUNT.incrementAndGet();
    }

    @Override
    public void close() {
        CLOSE_COUNT.incrementAndGet();
    }

    public static void resetCounters() {
        INIT_COUNT.set(0);
        CLOSE_COUNT.set(0);
        ON_COMMIT_COUNT.set(0);
        CLUSTER_META.set(null);
        IS_CLUSTER_ID_PRESENT_BEFORE_ON_CONSUME.set(false);
    }

    @Override
    public void onUpdate(ClusterResource clusterResource) {
        CLUSTER_META.set(clusterResource);
    }
}
