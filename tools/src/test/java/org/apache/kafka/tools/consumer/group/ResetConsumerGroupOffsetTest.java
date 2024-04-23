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
package org.apache.kafka.tools.consumer.group;

import joptsimple.OptionException;
import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterConfigProperty;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases by:
 * - Non-existing consumer group
 * - One for each scenario, with scope=all-topics
 * - scope=one topic, scenario=to-earliest
 * - scope=one topic+partitions, scenario=to-earliest
 * - scope=topics, scenario=to-earliest
 * - scope=topics+partitions, scenario=to-earliest
 * - export/import
 */
@ExtendWith(value = ClusterTestExtensions.class)
@ClusterTestDefaults(clusterType = Type.ALL, serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")})
@Tag("integration")
public class ResetConsumerGroupOffsetTest {

    private static final String TOPIC = "foo";
    private static final String GROUP = "test.group";
    private final ClusterInstance cluster;

    public ResetConsumerGroupOffsetTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @AfterEach
    public void stopCluster() {
        cluster.stop();
    }


    private String[] basicArgs() {
        return new String[]{"--reset-offsets",
                "--bootstrap-server", cluster.bootstrapServers(),
                "--timeout", Long.toString(DEFAULT_MAX_WAIT_MS)};
    }

    private String[] buildArgsForGroups(List<String> groups, String... args) {
        List<String> res = new ArrayList<>(Arrays.asList(basicArgs()));
        for (String group : groups) {
            res.add("--group");
            res.add(group);
        }
        res.addAll(Arrays.asList(args));
        return res.toArray(new String[0]);
    }

    private String[] buildArgsForGroup(String group, String... args) {
        return buildArgsForGroups(singletonList(group), args);
    }

    private String[] buildArgsForAllGroups(String... args) {
        List<String> res = new ArrayList<>(Arrays.asList(basicArgs()));
        res.add("--all-groups");
        res.addAll(Arrays.asList(args));
        return res.toArray(new String[0]);
    }

    @ClusterTest
    public void testResetOffsetsNotExistingGroup() throws Exception {
        String group = "missing.group";
        String[] args = buildArgsForGroup(group, "--all-topics", "--to-current", "--execute");
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommand = getConsumerGroupService(args);
        // Make sure we got a coordinator
        TestUtils.waitForCondition(
                () -> "localhost".equals(consumerGroupCommand.collectGroupState(group).coordinator.host()),
                "Can't find a coordinator");
        Map<TopicPartition, OffsetAndMetadata> resetOffsets = consumerGroupCommand.resetOffsets().get(group);
        assertTrue(resetOffsets.isEmpty());
        assertTrue(committedOffsets(TOPIC, group).isEmpty());
    }

    @ClusterTest
    public void testResetOffsetsExistingTopic() {
        String group = "new.group";
        String[] args = buildArgsForGroup(group, "--topic", TOPIC, "--to-offset", "50");
        produceMessages(TOPIC, 100);
        resetAndAssertOffsets(args, 50, true, singletonList(TOPIC));
        resetAndAssertOffsets(addTo(args, "--dry-run"), 50, true, singletonList(TOPIC));
        resetAndAssertOffsets(addTo(args, "--execute"), 50, false, singletonList(TOPIC));
    }

    @ClusterTest
    public void testResetOffsetsExistingTopicSelectedGroups() throws Exception {
        produceMessages(TOPIC, 100);
        List<String> groups = IntStream.rangeClosed(1, 3).mapToObj(id -> GROUP + id).collect(Collectors.toList());
        for (String group : groups) {
            ConsumerGroupExecutor executor = addConsumerGroupExecutor(1, TOPIC, group, GroupProtocol.CLASSIC.name);
            awaitConsumerProgress(TOPIC, group, 100L);
            executor.shutdown();
        }
        String[] args = buildArgsForGroups(groups, "--topic", TOPIC, "--to-offset", "50");
        resetAndAssertOffsets(args, 50, true, singletonList(TOPIC));
        resetAndAssertOffsets(addTo(args, "--dry-run"), 50, true, singletonList(TOPIC));
        resetAndAssertOffsets(addTo(args, "--execute"), 50, false, singletonList(TOPIC));
    }

    @ClusterTest
    public void testResetOffsetsExistingTopicAllGroups() throws Exception {
        String[] args = buildArgsForAllGroups("--topic", TOPIC, "--to-offset", "50");
        produceMessages(TOPIC, 100);
        for (int i = 1; i <= 3; i++) {
            String group = GROUP + i;
            ConsumerGroupExecutor executor = addConsumerGroupExecutor(1, TOPIC, group, GroupProtocol.CLASSIC.name);
            awaitConsumerProgress(TOPIC, group, 100L);
            executor.shutdown();
        }
        resetAndAssertOffsets(args, 50, true, singletonList(TOPIC));
        resetAndAssertOffsets(addTo(args, "--dry-run"), 50, true, singletonList(TOPIC));
        resetAndAssertOffsets(addTo(args, "--execute"), 50, false, singletonList(TOPIC));
    }

    @ClusterTest
    public void testResetOffsetsAllTopicsAllGroups() throws Exception {
        String[] args = buildArgsForAllGroups("--all-topics", "--to-offset", "50");
        List<String> topics = IntStream.rangeClosed(1, 3).mapToObj(i -> TOPIC + i).collect(Collectors.toList());
        List<String> groups = IntStream.rangeClosed(1, 3).mapToObj(i -> GROUP + i).collect(Collectors.toList());
        topics.forEach(topic -> produceMessages(topic, 100));

        for (String topic : topics) {
            for (String group : groups) {
                ConsumerGroupExecutor executor = addConsumerGroupExecutor(3, topic, group, GroupProtocol.CLASSIC.name);
                awaitConsumerProgress(topic, group, 100);
                executor.shutdown();
            }
        }
        resetAndAssertOffsets(args, 50, true, topics);
        resetAndAssertOffsets(addTo(args, "--dry-run"), 50, true, topics);
        resetAndAssertOffsets(addTo(args, "--execute"), 50, false, topics);
    }

    @ClusterTest
    public void testResetOffsetsToLocalDateTime() throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);

        produceMessages(TOPIC, 100);

        ConsumerGroupExecutor executor = addConsumerGroupExecutor(1, TOPIC, GROUP, GroupProtocol.CLASSIC.name);
        awaitConsumerProgress(TOPIC, GROUP, 100L);
        executor.shutdown();

        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--to-datetime", format.format(calendar.getTime()), "--execute");
        resetAndAssertOffsets(args, 0);
    }

    @ClusterTest
    public void testResetOffsetsToZonedDateTime() throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

        produceMessages(TOPIC, 50);
        Date checkpoint = new Date();
        produceMessages(TOPIC, 50);

        ConsumerGroupExecutor executor = addConsumerGroupExecutor(1, TOPIC, GROUP, GroupProtocol.CLASSIC.name);
        awaitConsumerProgress(TOPIC, GROUP, 100L);
        executor.shutdown();

        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--to-datetime", format.format(checkpoint), "--execute");
        resetAndAssertOffsets(args, 50);
    }

    @ClusterTest
    public void testResetOffsetsByDuration() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--by-duration", "PT1M", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        resetAndAssertOffsets(args, 0);
    }

    @ClusterTest
    public void testResetOffsetsByDurationToEarliest() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--by-duration", "PT0.1S", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        resetAndAssertOffsets(args, 100);
    }

    @ClusterTest
    public void testResetOffsetsByDurationFallbackToLatestWhenNoRecords() throws ExecutionException, InterruptedException {
        String topic = "foo2";
        String[] args = buildArgsForGroup(GROUP, "--topic", topic, "--by-duration", "PT1M", "--execute");
        Admin admin = cluster.createAdminClient();
        admin.createTopics(Collections.singleton(new NewTopic(topic, 1, (short) 1))).all().get();
        resetAndAssertOffsets(args, 0, false, singletonList("foo2"));

        admin.deleteTopics(Collections.singleton(topic)).all().get();
    }

    @ClusterTest
    public void testResetOffsetsToEarliest() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--to-earliest", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        resetAndAssertOffsets(args, 0);
    }

    @ClusterTest
    public void testResetOffsetsToLatest() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--to-latest", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        produceMessages(TOPIC, 100);
        resetAndAssertOffsets(args, 200);
    }

    @ClusterTest
    public void testResetOffsetsToCurrentOffset() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--to-current", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        produceMessages(TOPIC, 100);
        resetAndAssertOffsets(args, 100);
    }

    @ClusterTest
    public void testResetOffsetsToSpecificOffset() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--to-offset", "1", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        resetAndAssertOffsets(args, 1);
    }

    @ClusterTest
    public void testResetOffsetsShiftPlus() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--shift-by", "50", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        produceMessages(TOPIC, 100);
        resetAndAssertOffsets(args, 150);
    }

    @ClusterTest
    public void testResetOffsetsShiftMinus() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--shift-by", "-50", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        produceMessages(TOPIC, 100);
        resetAndAssertOffsets(args, 50);
    }

    @ClusterTest
    public void testResetOffsetsShiftByLowerThanEarliest() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--shift-by", "-150", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        produceMessages(TOPIC, 100);
        resetAndAssertOffsets(args, 0);
    }

    @ClusterTest
    public void testResetOffsetsShiftByHigherThanLatest() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--all-topics", "--shift-by", "150", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        produceMessages(TOPIC, 100);
        resetAndAssertOffsets(args, 200);
    }

    @ClusterTest
    public void testResetOffsetsToEarliestOnOneTopic() throws Exception {
        String[] args = buildArgsForGroup(GROUP, "--topic", TOPIC, "--to-earliest", "--execute");
        produceConsumeAndShutdown(TOPIC, GROUP, 100, 1);
        resetAndAssertOffsets(args, 0);
    }

    @ClusterTest
    public void testResetOffsetsToEarliestOnOneTopicAndPartition() throws Exception {
        String topic = "bar";
        Admin admin = cluster.createAdminClient();
        admin.createTopics(Collections.singleton(new NewTopic(topic, 2, (short) 1))).all().get();

        String[] args = buildArgsForGroup(GROUP, "--topic", topic + ":1", "--to-earliest", "--execute");
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommand = getConsumerGroupService(args);

        produceConsumeAndShutdown(topic, GROUP, 100, 2);
        Map<TopicPartition, Long> priorCommittedOffsets = committedOffsets(topic, GROUP);

        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);
        Map<TopicPartition, Long> expectedOffsets = new HashMap<>();
        expectedOffsets.put(tp0, priorCommittedOffsets.get(tp0));
        expectedOffsets.put(tp1, 0L);
        resetAndAssertOffsetsCommitted(consumerGroupCommand, expectedOffsets, topic);

        admin.deleteTopics(Collections.singleton(topic)).all().get();
    }

    @ClusterTest
    public void testResetOffsetsToEarliestOnTopics() throws Exception {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Admin admin = cluster.createAdminClient();
        admin.createTopics(Arrays.asList(new NewTopic(topic1, 1, (short) 1),
                new NewTopic(topic2, 1, (short) 1))).all().get();

        String[] args = buildArgsForGroup(GROUP, "--topic", topic1, "--topic", topic2, "--to-earliest", "--execute");
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommand = getConsumerGroupService(args);

        produceConsumeAndShutdown(topic1, GROUP, 100, 1);
        produceConsumeAndShutdown(topic2, GROUP, 100, 1);

        TopicPartition tp1 = new TopicPartition(topic1, 0);
        TopicPartition tp2 = new TopicPartition(topic2, 0);

        Map<TopicPartition, Long> allResetOffsets = toOffsetMap(resetOffsets(consumerGroupCommand).get(GROUP));
        Map<TopicPartition, Long> expMap = new HashMap<>();
        expMap.put(tp1, 0L);
        expMap.put(tp2, 0L);
        assertEquals(expMap, allResetOffsets);
        assertEquals(singletonMap(tp1, 0L), committedOffsets(topic1, GROUP));
        assertEquals(singletonMap(tp2, 0L), committedOffsets(topic2, GROUP));

        admin.deleteTopics(Arrays.asList(topic1, topic2)).all().get();
    }

    @ClusterTest
    public void testResetOffsetsToEarliestOnTopicsAndPartitions() throws Exception {
        String topic1 = "topic1";
        String topic2 = "topic2";
        Admin admin = cluster.createAdminClient();
        admin.createTopics(Arrays.asList(new NewTopic(topic1, 2, (short) 1),
                new NewTopic(topic2, 2, (short) 1))).all().get();

        String[] args = buildArgsForGroup(GROUP, "--topic", topic1 + ":1", "--topic", topic2 + ":1", "--to-earliest", "--execute");
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommand = getConsumerGroupService(args);

        produceConsumeAndShutdown(topic1, GROUP, 100, 2);
        produceConsumeAndShutdown(topic2, GROUP, 100, 2);

        Map<TopicPartition, Long> priorCommittedOffsets1 = committedOffsets(topic1, GROUP);
        Map<TopicPartition, Long> priorCommittedOffsets2 = committedOffsets(topic2, GROUP);

        TopicPartition tp1 = new TopicPartition(topic1, 1);
        TopicPartition tp2 = new TopicPartition(topic2, 1);
        Map<TopicPartition, Long> allResetOffsets = toOffsetMap(resetOffsets(consumerGroupCommand).get(GROUP));
        Map<TopicPartition, Long> expMap = new HashMap<>();
        expMap.put(tp1, 0L);
        expMap.put(tp2, 0L);
        assertEquals(expMap, allResetOffsets);
        priorCommittedOffsets1.put(tp1, 0L);
        assertEquals(priorCommittedOffsets1, committedOffsets(topic1, GROUP));
        priorCommittedOffsets2.put(tp2, 0L);
        assertEquals(priorCommittedOffsets2, committedOffsets(topic2, GROUP));

        admin.deleteTopics(Arrays.asList(topic1, topic2)).all().get();
    }

    @ClusterTest
    // This one deals with old CSV export/import format for a single --group arg: "topic,partition,offset" to support old behavior
    public void testResetOffsetsExportImportPlanSingleGroupArg() throws Exception {
        String topic = "bar";
        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);
        Admin admin = cluster.createAdminClient();
        admin.createTopics(Collections.singleton(new NewTopic(topic, 2, (short) 1))).all().get();

        String[] cgcArgs = buildArgsForGroup(GROUP, "--all-topics", "--to-offset", "2", "--export");
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommand = getConsumerGroupService(cgcArgs);

        produceConsumeAndShutdown(topic, GROUP, 100, 2);

        File file = TestUtils.tempFile("reset", ".csv");

        Map<String, Map<TopicPartition, OffsetAndMetadata>> exportedOffsets = consumerGroupCommand.resetOffsets();
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        bw.write(consumerGroupCommand.exportOffsetsToCsv(exportedOffsets));
        bw.close();

        Map<TopicPartition, Long> exp1 = new HashMap<>();
        exp1.put(tp0, 2L);
        exp1.put(tp1, 2L);
        assertEquals(exp1, toOffsetMap(exportedOffsets.get(GROUP)));

        String[] cgcArgsExec = buildArgsForGroup(GROUP, "--all-topics", "--from-file", file.getCanonicalPath(), "--dry-run");
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommandExec = getConsumerGroupService(cgcArgsExec);
        Map<String, Map<TopicPartition, OffsetAndMetadata>> importedOffsets = consumerGroupCommandExec.resetOffsets();
        assertEquals(exp1, toOffsetMap(importedOffsets.get(GROUP)));

        admin.deleteTopics(Collections.singleton(topic));
    }

    @ClusterTest
    // This one deals with universal CSV export/import file format "group,topic,partition,offset",
    // supporting multiple --group args or --all-groups arg
    public void testResetOffsetsExportImportPlan() throws Exception {
        String group1 = GROUP + "1";
        String group2 = GROUP + "2";
        String topic1 = "bar1";
        String topic2 = "bar2";
        TopicPartition t1p0 = new TopicPartition(topic1, 0);
        TopicPartition t1p1 = new TopicPartition(topic1, 1);
        TopicPartition t2p0 = new TopicPartition(topic2, 0);
        TopicPartition t2p1 = new TopicPartition(topic2, 1);
        Admin admin = cluster.createAdminClient();
        admin.createTopics(Arrays.asList(new NewTopic(topic1, 2, (short) 1),
                new NewTopic(topic2, 2, (short) 1))).all().get();


        String[] cgcArgs = buildArgsForGroups(Arrays.asList(group1, group2), "--all-topics", "--to-offset", "2", "--export");
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommand = getConsumerGroupService(cgcArgs);

        produceConsumeAndShutdown(topic1, group1, 100, 1);
        produceConsumeAndShutdown(topic2, group2, 100, 1);

        awaitConsumerGroupInactive(consumerGroupCommand, group1);
        awaitConsumerGroupInactive(consumerGroupCommand, group2);

        File file = TestUtils.tempFile("reset", ".csv");

        Map<String, Map<TopicPartition, OffsetAndMetadata>> exportedOffsets = consumerGroupCommand.resetOffsets();
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));
        bw.write(consumerGroupCommand.exportOffsetsToCsv(exportedOffsets));
        bw.close();
        Map<TopicPartition, Long> exp1 = new HashMap<>();
        exp1.put(t1p0, 2L);
        exp1.put(t1p1, 2L);
        Map<TopicPartition, Long> exp2 = new HashMap<>();
        exp2.put(t2p0, 2L);
        exp2.put(t2p1, 2L);

        assertEquals(exp1, toOffsetMap(exportedOffsets.get(group1)));
        assertEquals(exp2, toOffsetMap(exportedOffsets.get(group2)));

        // Multiple --group's offset import
        String[] cgcArgsExec = buildArgsForGroups(Arrays.asList(group1, group2), "--all-topics", "--from-file", file.getCanonicalPath(), "--dry-run");
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommandExec = getConsumerGroupService(cgcArgsExec);
        Map<String, Map<TopicPartition, OffsetAndMetadata>> importedOffsets = consumerGroupCommandExec.resetOffsets();
        assertEquals(exp1, toOffsetMap(importedOffsets.get(group1)));
        assertEquals(exp2, toOffsetMap(importedOffsets.get(group2)));

        // Single --group offset import using "group,topic,partition,offset" csv format
        String[] cgcArgsExec2 = buildArgsForGroup(group1, "--all-topics", "--from-file", file.getCanonicalPath(), "--dry-run");
        ConsumerGroupCommand.ConsumerGroupService consumerGroupCommandExec2 = getConsumerGroupService(cgcArgsExec2);
        Map<String, Map<TopicPartition, OffsetAndMetadata>> importedOffsets2 = consumerGroupCommandExec2.resetOffsets();
        assertEquals(exp1, toOffsetMap(importedOffsets2.get(group1)));

        admin.deleteTopics(Arrays.asList(topic1, topic2));
    }

    @ClusterTest
    public void testResetWithUnrecognizedNewConsumerOption() {
        String[] cgcArgs = new String[]{"--new-consumer",
                "--bootstrap-server", cluster.bootstrapServers(),
                "--reset-offsets", "--group", GROUP,
                "--all-topics",
                "--to-offset", "2",
                "--export"};
        assertThrows(OptionException.class, () -> getConsumerGroupService(cgcArgs));
    }

    private Map<TopicPartition, Long> committedOffsets(String topic, String group) {
        try (Consumer<String, String> consumer = createNoAutoCommitConsumer(group)) {
            Set<TopicPartition> partitions = consumer.partitionsFor(topic).stream().map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition())).collect(Collectors.toSet());
            return consumer.committed(partitions).entrySet().stream().filter(e -> e.getValue() != null).collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
        }
    }

    private Consumer<String, String> createNoAutoCommitConsumer(String group) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(GROUP_ID_CONFIG, group);
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    }

    private ConsumerGroupCommand.ConsumerGroupService getConsumerGroupService(String[] args) {
        return new ConsumerGroupCommand.ConsumerGroupService(
                ConsumerGroupCommandOptions.fromArgs(args),
                singletonMap(AdminClientConfig.RETRIES_CONFIG,
                        Integer.toString(Integer.MAX_VALUE)));
    }

    private void produceMessages(String topic, int numMessages) {
        List<ProducerRecord<byte[], byte[]>> records = IntStream.range(0, numMessages)
                .mapToObj(i -> new ProducerRecord<byte[], byte[]>(topic, new byte[100 * 1000]))
                .collect(Collectors.toList());
        produceMessages(cluster, records, 1);
    }

    private void produceMessages(ClusterInstance cluster, List<ProducerRecord<byte[], byte[]>> records, int acks) {
        Properties props = new Properties();
        props.put("bootstrap.servers", cluster.bootstrapServers());
        props.put("acks", String.valueOf(acks));
        props.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            records.forEach(producer::send);
        }
    }

    private void produceConsumeAndShutdown(String topic, String group, int totalMessages, int numConsumers) throws Exception {
        produceMessages(topic, totalMessages);
        ConsumerGroupExecutor executor = addConsumerGroupExecutor(numConsumers, topic, group, GroupProtocol.CLASSIC.name);
        awaitConsumerProgress(topic, group, totalMessages);
        executor.shutdown();
    }

    ConsumerGroupExecutor addConsumerGroupExecutor(int numConsumers, String topic, String group, String groupProtocol) {
        return addConsumerGroupExecutor(numConsumers, topic, group, RangeAssignor.class.getName(), Optional.empty(), Optional.empty(), false, groupProtocol);
    }

    ConsumerGroupExecutor addConsumerGroupExecutor(int numConsumers, String topic, String group, String strategy, Optional<String> remoteAssignor, Optional<Properties> customPropsOpt, boolean syncCommit, String groupProtocol) {
        return new ConsumerGroupExecutor(cluster.bootstrapServers(), numConsumers, group, groupProtocol, topic, strategy, remoteAssignor, customPropsOpt, syncCommit);
    }

    private void awaitConsumerProgress(String topic, String group, long count) throws Exception {
        try (Consumer<String, String> consumer = createNoAutoCommitConsumer(group)) {
            Set<TopicPartition> partitions = consumer.partitionsFor(topic).stream().map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition())).collect(Collectors.toSet());

            TestUtils.waitForCondition(() -> {
                Collection<OffsetAndMetadata> committed = consumer.committed(partitions).values();
                long total = committed.stream().mapToLong(offsetAndMetadata -> Optional.ofNullable(offsetAndMetadata).map(OffsetAndMetadata::offset).orElse(0L)).sum();

                return total == count;
            }, "Expected that consumer group has consumed all messages from topic/partition. " + "Expected offset: " + count + ". Actual offset: " + committedOffsets(topic, group).values().stream().mapToLong(Long::longValue).sum());
        }
    }

    private void awaitConsumerGroupInactive(ConsumerGroupCommand.ConsumerGroupService consumerGroupService, String group) throws Exception {
        TestUtils.waitForCondition(() -> {
            String state = consumerGroupService.collectGroupState(group).state;
            return Objects.equals(state, "Empty") || Objects.equals(state, "Dead");
        }, "Expected that consumer group is inactive. Actual state: " + consumerGroupService.collectGroupState(group).state);
    }

    private void resetAndAssertOffsets(String[] args, long expectedOffset) {
        resetAndAssertOffsets(args, expectedOffset, false, singletonList(TOPIC));
    }

    private void resetAndAssertOffsets(String[] args, long expectedOffset, boolean dryRun, List<String> topics) {
        try (ConsumerGroupCommand.ConsumerGroupService consumerGroupCommand = getConsumerGroupService(args);) {
            Map<String, Map<TopicPartition, Long>> expectedOffsets = topics.stream().collect(Collectors.toMap(Function.identity(), topic -> singletonMap(new TopicPartition(topic, 0), expectedOffset)));
            Map<String, Map<TopicPartition, OffsetAndMetadata>> resetOffsetsResultByGroup = resetOffsets(consumerGroupCommand);
            for (final String topic : topics) {
                resetOffsetsResultByGroup.forEach((group, partitionInfo) -> {
                    Map<TopicPartition, Long> priorOffsets = committedOffsets(topic, group);
                    assertEquals(expectedOffsets.get(topic), partitionInfo.entrySet().stream().filter(entry -> Objects.equals(entry.getKey().topic(), topic)).collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset())));
                    assertEquals(dryRun ? priorOffsets : expectedOffsets.get(topic), committedOffsets(topic, group));
                });
            }
        }
    }

    private void resetAndAssertOffsetsCommitted(ConsumerGroupCommand.ConsumerGroupService consumerGroupService, Map<TopicPartition, Long> expectedOffsets, String topic) {
        Map<String, Map<TopicPartition, OffsetAndMetadata>> allResetOffsets = resetOffsets(consumerGroupService);

        allResetOffsets.forEach((group, offsetsInfo) -> {
            offsetsInfo.forEach((tp, offsetMetadata) -> {
                assertEquals(offsetMetadata.offset(), expectedOffsets.get(tp));
                assertEquals(expectedOffsets, committedOffsets(topic, group));
            });
        });
    }

    private Map<String, Map<TopicPartition, OffsetAndMetadata>> resetOffsets(ConsumerGroupCommand.ConsumerGroupService consumerGroupService) {
        return consumerGroupService.resetOffsets();
    }

    Map<TopicPartition, Long> toOffsetMap(Map<TopicPartition, OffsetAndMetadata> map) {
        return map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    private String[] addTo(String[] args, String... extra) {
        List<String> res = new ArrayList<>(Arrays.asList(args));
        res.addAll(Arrays.asList(extra));
        return res.toArray(new String[0]);
    }

    private ListenerName listenerName() {
        return ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT);
    }

    class AbstractConsumerGroupExecutor {
        final int numThreads;
        final ExecutorService executor;
        final List<AbstractConsumerRunnable> consumers = new ArrayList<>();

        public AbstractConsumerGroupExecutor(int numThreads) {
            this.numThreads = numThreads;
            this.executor = Executors.newFixedThreadPool(numThreads);
        }

        void submit(AbstractConsumerRunnable consumerThread) {
            consumers.add(consumerThread);
            executor.submit(consumerThread);
        }

        void shutdown() {
            consumers.forEach(AbstractConsumerRunnable::shutdown);
            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    class ConsumerGroupExecutor extends AbstractConsumerGroupExecutor {
        public ConsumerGroupExecutor(String broker, int numConsumers, String groupId, String groupProtocol, String topic, String strategy, Optional<String> remoteAssignor, Optional<Properties> customPropsOpt, boolean syncCommit) {
            super(numConsumers);
            IntStream.rangeClosed(1, numConsumers).forEach(i -> {
                ConsumerRunnable th = new ConsumerRunnable(broker, groupId, groupProtocol, topic, strategy, remoteAssignor, customPropsOpt, syncCommit);
                th.configure();
                submit(th);
            });
        }
    }

    abstract class AbstractConsumerRunnable implements Runnable {
        final String broker;
        final String groupId;
        final Optional<Properties> customPropsOpt;
        final boolean syncCommit;

        final Properties props = new Properties();
        KafkaConsumer<String, String> consumer;

        boolean configured = false;

        public AbstractConsumerRunnable(String broker, String groupId, Optional<Properties> customPropsOpt, boolean syncCommit) {
            this.broker = broker;
            this.groupId = groupId;
            this.customPropsOpt = customPropsOpt;
            this.syncCommit = syncCommit;
        }

        void configure() {
            configured = true;
            configure(props);
            customPropsOpt.ifPresent(props::putAll);
            consumer = new KafkaConsumer<>(props);
        }

        void configure(Properties props) {
            props.put("bootstrap.servers", broker);
            props.put("group.id", groupId);
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", StringDeserializer.class.getName());
        }

        abstract void subscribe();

        @Override
        public void run() {
            assert configured : "Must call configure before use";
            try {
                subscribe();
                while (true) {
                    consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                    if (syncCommit) consumer.commitSync();
                }
            } catch (WakeupException e) {
                // OK
            } finally {
                consumer.close();
            }
        }

        void shutdown() {
            consumer.wakeup();
        }
    }

    class ConsumerRunnable extends AbstractConsumerRunnable {
        final String topic;
        final String groupProtocol;
        final String strategy;
        final Optional<String> remoteAssignor;

        public ConsumerRunnable(String broker, String groupId, String groupProtocol, String topic, String strategy, Optional<String> remoteAssignor, Optional<Properties> customPropsOpt, boolean syncCommit) {
            super(broker, groupId, customPropsOpt, syncCommit);

            this.topic = topic;
            this.groupProtocol = groupProtocol;
            this.strategy = strategy;
            this.remoteAssignor = remoteAssignor;
        }

        @Override
        void configure(Properties props) {
            super.configure(props);
            props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol);
            if (groupProtocol.toUpperCase(Locale.ROOT).equals(GroupProtocol.CONSUMER.toString())) {
                remoteAssignor.ifPresent(assignor -> props.put(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG, assignor));
            } else {
                props.put("partition.assignment.strategy", strategy);
            }
        }

        @Override
        void subscribe() {
            consumer.subscribe(Collections.singleton(topic));
        }
    }

    class SimpleConsumerRunnable extends AbstractConsumerRunnable {
        final Collection<TopicPartition> partitions;

        public SimpleConsumerRunnable(String broker, String groupId, Collection<TopicPartition> partitions) {
            super(broker, groupId, Optional.empty(), false);

            this.partitions = partitions;
        }

        @Override
        void subscribe() {
            consumer.assign(partitions);
        }
    }
}
