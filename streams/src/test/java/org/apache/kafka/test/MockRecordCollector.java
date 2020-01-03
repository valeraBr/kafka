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
package org.apache.kafka.test;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.RecordCollector;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableList;

public class MockRecordCollector implements RecordCollector {

    // remember all records that are collected so far
    private final List<ProducerRecord<Object, Object>> collected = new LinkedList<>();

    // remember all commits that are submitted so far
    private final List<Map<TopicPartition, OffsetAndMetadata>> committed = new LinkedList<>();

    // remember if flushed is called
    private boolean flushed = false;

    @Override
    public void initialize() {}

    @Override
    public <K, V> void send(final String topic,
                            final K key,
                            final V value,
                            final Headers headers,
                            final Integer partition,
                            final Long timestamp,
                            final Serializer<K> keySerializer,
                            final Serializer<V> valueSerializer) {
        collected.add(new ProducerRecord<>(topic,
            partition,
            timestamp,
            key,
            value,
            headers));
    }

    @Override
    public <K, V> void send(final String topic,
                            final K key,
                            final V value,
                            final Headers headers,
                            final Long timestamp,
                            final Serializer<K> keySerializer,
                            final Serializer<V> valueSerializer,
                            final StreamPartitioner<? super K, ? super V> partitioner) {
        collected.add(new ProducerRecord<>(topic,
            0, // partition id
            timestamp,
            key,
            value,
            headers));
    }

    @Override
    public void commit(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        committed.add(offsets);
    }

    @Override
    public void flush() {
        flushed = true;
    }

    @Override
    public void close() {}

    @Override
    public Map<TopicPartition, Long> offsets() {
        return Collections.emptyMap();
    }

    public List<ProducerRecord<Object, Object>> collected() {
        return unmodifiableList(collected);
    }

    public List<Map<TopicPartition, OffsetAndMetadata>> committed() {
        return unmodifiableList(committed);
    }

    public boolean flushed() {
        return flushed;
    }

    public void clear() {
        this.flushed = false;
        this.collected.clear();
    }
}
