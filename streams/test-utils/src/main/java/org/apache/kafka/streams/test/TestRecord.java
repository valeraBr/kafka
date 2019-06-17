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
package org.apache.kafka.streams.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * TODO
 *
 * A key/value pair to be sent to Kafka. This consists of a topic name to which the record is being sent, an optional
 * partition number, and an optional key and value.
 * <p>
 * If a valid partition number is specified that partition will be used when sending the record. If no partition is
 * specified but a key is present a partition will be chosen using a hash of the key. If neither key nor partition is
 * present a partition will be assigned in a round-robin fashion.
 * <p>
 * The record also has an associated timestamp. If the user did not provide a timestamp, the producer will stamp the
 * record with its current time. The timestamp eventually used by Kafka depends on the timestamp type configured for
 * the topic.
 * <li>
 * If the topic is configured to use {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime},
 * the timestamp in the producer record will be used by the broker.
 * </li>
 * <li>
 * If the topic is configured to use {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime},
 * the timestamp in the producer record will be overwritten by the broker with the broker local time when it appends the
 * message to its log.
 * </li>
 * <p>
 * In either of the cases above, the timestamp that has actually been used will be returned to user in
 * {@link RecordMetadata}
 */
public class TestRecord<K, V> {

    private final Headers headers;
    private final K key;
    private final V value;
    private final Long timestamp;

    /**
     * Creates a record with a specified timestamp to be sent to a specified topic and partition
     * 
     * @param key The key that will be included in the record
     * @param value The record contents
     * @param headers the headers that will be included in the record
     * @param timestamp The timestamp of the record, in milliseconds since epoch. If null,
     *                  the timestamp is assigned using System.currentTimeMillis() or internally tracked time.
     */
    public TestRecord(final K key, V value, final Headers headers, final Long timestamp) {
        if (timestamp != null && timestamp < 0)
            throw new IllegalArgumentException(
                    String.format("Invalid timestamp: %d. Timestamp should always be non-negative or null.", timestamp));
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.headers = new RecordHeaders(headers);
    }

    /**
     * Creates a record with a specified timestamp to be sent to a specified topic and partition
     *
     * @param key The key that will be included in the record
     * @param value The record contents
     * @param timestamp The timestamp of the record, in milliseconds since epoch. If null,
     *                  the timestamp is assigned using System.currentTimeMillis() or internally tracked time.
     */
    public TestRecord(final K key, final V value, final Long timestamp) {
        this(key, value, null, timestamp);
    }

    /**
     * Creates a record to be sent to a specified topic and partition
     *
     * @param key The key that will be included in the record
     * @param value The record contents
     * @param headers The headers that will be included in the record
     */
    public TestRecord(final K key, final V value, final Headers headers) {
        this(key, value, headers, null);
    }
    
    /**
     * Creates a record to be sent to a specified topic and partition
     *
     * @param key The key that will be included in the record
     * @param value The record contents
     */
    public TestRecord(final K key, final V value) {
        this(key, value, null, null);
    }

    /**
     * Create a record with no key
     *
     * @param value The record contents
     */
    public TestRecord(final V value) {
        this(null, value, null, null);
    }

    /**
     * Create a TestRecord from ConsumerRecord
     *
     * @param record The record contents
     */
    public TestRecord(final ConsumerRecord<K, V> record) {
        this(record.key(), record.value(), record.headers(), record.timestamp());
    }

    /**
     * Create a TestRecord from ProducerRecord
     *
     * @param record The record contents
     */
    public TestRecord(final ProducerRecord<K, V> record) {
        this(record.key(), record.value(), record.headers(), record.timestamp());
    }

    /**
     * @return The headers
     */
    public Headers headers() {
        return headers;
    }

    /**
     * @return The key (or null if no key is specified)
     */
    public K key() {
        return key;
    }


    /**
     * @return The value
     */
    public V value() {
        return value;
    }

    /**
     * @return The timestamp, which is in milliseconds since epoch.
     */
    public Long timestamp() {
        return timestamp;
    }

    public Headers getHeaders() {
        return headers;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        String headers = this.headers == null ? "null" : this.headers.toString();
        String key = this.key == null ? "null" : this.key.toString();
        String value = this.value == null ? "null" : this.value.toString();
        String timestamp = this.timestamp == null ? "null" : this.timestamp.toString();
        return "TestRecord(headers=" + headers + ", key=" + key + ", value=" + value +
            ", timestamp=" + timestamp + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        else if (!(o instanceof TestRecord))
            return false;

        TestRecord<?, ?> that = (TestRecord<?, ?>) o;

        if (key != null ? !key.equals(that.key) : that.key != null) 
            return false;
        else if (headers != null ? !headers.equals(that.headers) : that.headers != null)
            return false;
        else if (value != null ? !value.equals(that.value) : that.value != null) 
            return false;
        else if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = headers != null ? headers.hashCode() : 0;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }
}
