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
package org.apache.kafka.common.errors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import java.nio.ByteBuffer;

public class ValueDeserializationException extends RecordDeserializationException {
    private final static long serialVersionUID = 1L;
    public ValueDeserializationException(TopicPartition partition,
                                         long offset,
                                         long timestamp,
                                         TimestampType timestampType,
                                         ByteBuffer keyBuffer,
                                         ByteBuffer valueBuffer,
                                         Headers headers,
                                         String message,
                                         Throwable cause) {
        super(partition, offset, timestamp, timestampType, keyBuffer, valueBuffer, headers, message, cause);
    }
}
