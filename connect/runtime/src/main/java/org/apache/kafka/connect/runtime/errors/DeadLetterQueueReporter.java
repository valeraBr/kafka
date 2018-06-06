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
package org.apache.kafka.connect.runtime.errors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.singleton;
import static org.apache.kafka.connect.header.ConnectHeaders.HEADER_PREFIX;

/**
 * Write the original consumed record into a dead letter queue. The dead letter queue is a Kafka topic located
 * on the same cluster used by the worker to maintain internal topics. Each connector is typically configured
 * with its own Kafka topic dead letter queue. By default, the topic name is not set, and if the
 * connector config doesn't specify one, this feature is disabled.
 */
public class DeadLetterQueueReporter implements ErrorReporter {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueReporter.class);

    private static final int DLQ_NUM_DESIRED_PARTITIONS = 1;

    public static final String ERROR_HEADER_PREFIX = HEADER_PREFIX + ".errors";
    public static final String ERROR_HEADER_ORIG_TOPIC = "topic";
    public static final String ERROR_HEADER_ORIG_PARTITION = "partition";
    public static final String ERROR_HEADER_ORIG_OFFSET = "offset";
    public static final String ERROR_HEADER_CONNECTOR_NAME = "connector.name";
    public static final String ERROR_HEADER_TASK_ID = "task.id";
    public static final String ERROR_HEADER_STAGE = "stage";
    public static final String ERROR_HEADER_EXECUTING_CLASS = "class.name";
    public static final String ERROR_HEADER_EXECPTION = "exception.class.name";
    public static final String ERROR_HEADER_EXECPTION_MESSAGE = "exception.message";
    public static final String ERROR_HEADER_EXECPTION_STACK_TRACE = "exception.stacktrace";

    private final SinkConnectorConfig connConfig;
    private final ConnectorTaskId connectorTaskId;

    private KafkaProducer<byte[], byte[]> kafkaProducer;
    private ErrorHandlingMetrics errorHandlingMetrics;

    public static DeadLetterQueueReporter createAndSetup(WorkerConfig workerConfig,
                                                         ConnectorTaskId id,
                                                         SinkConnectorConfig sinkConfig, Map<String, Object> producerProps) {
        String topic = sinkConfig.dlqTopicName();

        try (AdminClient admin = AdminClient.create(workerConfig.originals())) {
            if (!admin.listTopics().names().get().contains(topic)) {
                log.error("Topic {} doesn't exist. Will attempt to create topic.", topic);
                NewTopic schemaTopicRequest = new NewTopic(topic, DLQ_NUM_DESIRED_PARTITIONS, sinkConfig.dlqTopicReplicationFactor());
                admin.createTopics(singleton(schemaTopicRequest)).all().get();
            }
        } catch (InterruptedException e) {
            throw new ConnectException("Could not initialize dead letter queue with topic=" + topic, e);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new ConnectException("Could not initialize dead letter queue with topic=" + topic, e);
            }
        }

        KafkaProducer<byte[], byte[]> dlqProducer = new KafkaProducer<>(producerProps);
        return new DeadLetterQueueReporter(dlqProducer, sinkConfig, id);
    }

    /**
     * Initialize the dead letter queue reporter with a {@link KafkaProducer}.
     *
     * @param kafkaProducer a Kafka Producer to produce the original consumed records.
     */
    // Visible for testing
    DeadLetterQueueReporter(KafkaProducer<byte[], byte[]> kafkaProducer, SinkConnectorConfig connConfig, ConnectorTaskId id) {
        this.kafkaProducer = kafkaProducer;
        this.connConfig = connConfig;
        this.connectorTaskId = id;
    }

    @Override
    public void metrics(ErrorHandlingMetrics errorHandlingMetrics) {
        this.errorHandlingMetrics = errorHandlingMetrics;
    }

    /**
     * Write the raw records into a Kafka topic.
     *
     * @param context processing context containing the raw record at {@link ProcessingContext#consumerRecord()}.
     */
    public void report(ProcessingContext context) {
        final String dlqTopicName = connConfig.dlqTopicName();
        if (dlqTopicName.isEmpty()) {
            return;
        }

        errorHandlingMetrics.recordDeadLetterQueueProduceRequest();

        ConsumerRecord<byte[], byte[]> originalMessage = context.consumerRecord();
        if (originalMessage == null) {
            errorHandlingMetrics.recordDeadLetterQueueProduceFailed();
            return;
        }

        ProducerRecord<byte[], byte[]> producerRecord;
        if (originalMessage.timestamp() == RecordBatch.NO_TIMESTAMP) {
            producerRecord = new ProducerRecord<>(dlqTopicName, null,
                    originalMessage.key(), originalMessage.value(), originalMessage.headers());
        } else {
            producerRecord = new ProducerRecord<>(dlqTopicName, null, originalMessage.timestamp(),
                    originalMessage.key(), originalMessage.value(), originalMessage.headers());
        }

        if (connConfig.isDlqContextHeadersEnabled()) {
            populateContextHeaders(producerRecord, context);
        }

        this.kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("Could not produce message to dead letter queue. topic=" + dlqTopicName, exception);
                errorHandlingMetrics.recordDeadLetterQueueProduceFailed();
            }
        });
    }

    // Visible for testing
    void populateContextHeaders(ProducerRecord<byte[], byte[]> producerRecord, ProcessingContext context) {
        String topic = "";
        int partition = -1;
        long offset = -1;
        if (context.consumerRecord() != null) {
            topic = context.consumerRecord().topic();
            partition = context.consumerRecord().partition();
            offset = context.consumerRecord().offset();
        }

        add(producerRecord.headers(), prefix(ERROR_HEADER_ORIG_TOPIC), topic);
        add(producerRecord.headers(), prefix(ERROR_HEADER_ORIG_PARTITION), String.valueOf(partition));
        add(producerRecord.headers(), prefix(ERROR_HEADER_ORIG_OFFSET), String.valueOf(offset));
        add(producerRecord.headers(), prefix(ERROR_HEADER_CONNECTOR_NAME), connectorTaskId.connector());
        add(producerRecord.headers(), prefix(ERROR_HEADER_TASK_ID), String.valueOf(connectorTaskId.task()));
        add(producerRecord.headers(), prefix(ERROR_HEADER_STAGE), context.stage().name());
        add(producerRecord.headers(), prefix(ERROR_HEADER_EXECUTING_CLASS), context.executingClass().getName());
        if (context.error() != null) {
            add(producerRecord.headers(), prefix(ERROR_HEADER_EXECPTION), context.error().getClass().getName());
            add(producerRecord.headers(), prefix(ERROR_HEADER_EXECPTION_MESSAGE), context.error().getMessage());
            byte[] trace;
            if ((trace = stacktrace(context.error())) != null) {
                add(producerRecord.headers(), prefix(ERROR_HEADER_EXECPTION_STACK_TRACE), trace);
            }
        }
    }

    private byte[] stacktrace(Throwable error) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            PrintStream stream = new PrintStream(bos, true, "UTF-8");
            error.printStackTrace(stream);
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            log.error("Could not serialize stacktrace.", e);
        }
        return null;
    }

    private void add(Headers headers, String key, String value) {
        add(headers, key, value.getBytes(Charset.forName("UTF-8")));
    }

    private void add(Headers headers, String key, byte[] value) {
        if (headers.lastHeader(key) == null) {
            headers.add(key, value);
        } else {
            log.error("Header already contains the '" + key + "' key.");
        }
    }

    private String prefix(String errorHeaderName) {
        return ERROR_HEADER_PREFIX + "." + errorHeaderName;
    }

}
