/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;


import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ProducerInterceptorsTest {
    private final TopicPartition tp = new TopicPartition("test", 0);
    private final ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>("test", 0, 1, "value");
    private int onAckCount = 0;
    private int onSendCount = 0;

    private class AppendProducerInterceptor implements ProducerInterceptor<Integer, String> {
        private String appendStr = "";
        private boolean throwExceptionOnSend = false;
        private boolean throwExceptionOnAck = false;

        public AppendProducerInterceptor(String appendStr) {
            this.appendStr = appendStr;
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public ProducerRecord<Integer, String> onSend(ProducerRecord<Integer, String> record) {
            onSendCount++;
            if (throwExceptionOnSend)
                throw new KafkaException("Injected exception in AppendProducerInterceptor.onSend");

            ProducerRecord<Integer, String> newRecord = new ProducerRecord<>(
                    record.topic(), record.partition(), record.key(), record.value().concat(appendStr));
            return newRecord;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            onAckCount++;
            if (throwExceptionOnAck)
                throw new KafkaException("Injected exception in AppendProducerInterceptor.onAcknowledgement");
        }

        @Override
        public void close() {
        }

        // if 'on' is true, onSend will always throw an exception
        public void injectOnSendError(boolean on) {
            throwExceptionOnSend = on;
        }

        // if 'on' is true, onAcknowledgement will always throw an exception
        public void injectOnAcknowledgementError(boolean on) {
            throwExceptionOnAck = on;
        }
    }

    @Test
    public void testOnSendChain() {
        List<ProducerInterceptor<Integer, String>> interceptorList = new ArrayList<>();
        // we are testing two different interceptors by configuring the same interceptor differently, which is not
        // how it would be done in KafkaProducer, but ok for testing interceptor callbacks
        AppendProducerInterceptor interceptor1 = new AppendProducerInterceptor("One");
        AppendProducerInterceptor interceptor2 = new AppendProducerInterceptor("Two");
        interceptorList.add(interceptor1);
        interceptorList.add(interceptor2);
        ProducerInterceptors<Integer, String> interceptors = new ProducerInterceptors<>(interceptorList);

        // verify that onSend() mutates the record as expected
        ProducerRecord<Integer, String> interceptedRecord = interceptors.onSend(producerRecord);
        assertEquals(2, onSendCount);
        assertEquals(producerRecord.topic(), interceptedRecord.topic());
        assertEquals(producerRecord.partition(), interceptedRecord.partition());
        assertEquals(producerRecord.key(), interceptedRecord.key());
        assertEquals(interceptedRecord.value(), producerRecord.value().concat("One").concat("Two"));

        // onSend() mutates the same record the same way
        ProducerRecord<Integer, String> anotherRecord = interceptors.onSend(producerRecord);
        assertEquals(4, onSendCount);
        assertEquals(interceptedRecord, anotherRecord);

        // verify that if one of the interceptors throws an exception, other interceptors' callbacks are still called
        interceptor1.injectOnSendError(true);
        ProducerRecord<Integer, String> partInterceptRecord = interceptors.onSend(producerRecord);
        assertEquals(6, onSendCount);
        assertEquals(partInterceptRecord.value(), producerRecord.value().concat("Two"));

        // verify the record remains valid if all onSend throws an exception
        interceptor2.injectOnSendError(true);
        ProducerRecord<Integer, String> noInterceptRecord = interceptors.onSend(producerRecord);
        assertEquals(producerRecord, noInterceptRecord);

        interceptors.close();
    }

    @Test
    public void testOnAcknowledgementChain() {
        List<ProducerInterceptor<Integer, String>> interceptorList = new ArrayList<>();
        // we are testing two different interceptors by configuring the same interceptor differently, which is not
        // how it would be done in KafkaProducer, but ok for testing interceptor callbacks
        AppendProducerInterceptor interceptor1 = new AppendProducerInterceptor("One");
        AppendProducerInterceptor interceptor2 = new AppendProducerInterceptor("Two");
        interceptorList.add(interceptor1);
        interceptorList.add(interceptor2);
        ProducerInterceptors<Integer, String> interceptors = new ProducerInterceptors<>(interceptorList);

        // verify onAck is called on all interceptors
        RecordMetadata meta = new RecordMetadata(tp, 0, 0);
        interceptors.onAcknowledgement(meta, null);
        assertEquals(2, onAckCount);

        // verify that onAcknowledgement exceptions do not propagate
        interceptor1.injectOnAcknowledgementError(true);
        interceptors.onAcknowledgement(meta, null);
        assertEquals(4, onAckCount);

        interceptor2.injectOnAcknowledgementError(true);
        interceptors.onAcknowledgement(meta, null);
        assertEquals(6, onAckCount);

        interceptors.close();
    }
}

