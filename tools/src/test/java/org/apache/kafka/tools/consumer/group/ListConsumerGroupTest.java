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
import kafka.test.annotation.ClusterTests;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.apache.kafka.tools.ToolsTestUtils;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkSet;

@Tag("integration")
@ClusterTestDefaults(clusterType = Type.ALL, serverProperties = {
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
})
@ExtendWith(ClusterTestExtensions.class)
public class ListConsumerGroupTest {
    private final static String TOPIC = "foo";
    private final static String DEFAULT_GROUP = "test.default.group";
    private final static String PROTOCOL_GROUP = "test.protocol.group";
    private final ClusterInstance clusterInstance;

    ListConsumerGroupTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @ClusterTests({
            @ClusterTest(clusterType = Type.ZK, serverProperties = {
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
            }),
            @ClusterTest(clusterType = Type.KRAFT, serverProperties = {
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
            }),
            @ClusterTest(clusterType = Type.KRAFT, serverProperties = {
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG, value = "true"),
            })
    })
    public void testListConsumerGroupsWithoutFilters() throws Exception {
        createTopic(TOPIC);

        try (AutoCloseable defaultConsumerGroupExecutor = ConsumerGroupExecutor.buildConsumerGroup(
                clusterInstance.bootstrapServers(),
                1,
                DEFAULT_GROUP,
                TOPIC,
                GroupProtocol.CLASSIC.name(),
                Optional.empty(),
                Collections.emptyMap(),
                false);

             AutoCloseable protocolConsumerGroupExecutor = ConsumerGroupExecutor.buildConsumerGroup(
                     clusterInstance.bootstrapServers(),
                     1,
                     PROTOCOL_GROUP,
                     TOPIC,
                     clusterInstance.config().serverProperties().get("group.coordinator.new.enable") == "true" ? GroupProtocol.CONSUMER.name() : GroupProtocol.CLASSIC.name(),
                     Optional.empty(),
                     Collections.emptyMap(),
                     false);

             ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--list"});
        ) {
            Set<String> expectedGroups = set(Arrays.asList(DEFAULT_GROUP, PROTOCOL_GROUP));
            final AtomicReference<Set> foundGroups = new AtomicReference<>();

            TestUtils.waitForCondition(() -> {
                foundGroups.set(set(service.listConsumerGroups()));
                return Objects.equals(expectedGroups, foundGroups.get());
            }, "Expected --list to show groups " + expectedGroups + ", but found " + foundGroups.get() + ".");
        }
    }

    @ClusterTest
    public void testListWithUnrecognizedNewConsumerOption() {
        String[] cgcArgs = new String[]{"--new-consumer", "--bootstrap-server", clusterInstance.bootstrapServers(), "--list"};
        Assertions.assertThrows(OptionException.class, () -> getConsumerGroupService(cgcArgs));
    }

    @ClusterTests({
            @ClusterTest(clusterType = Type.ZK, serverProperties = {
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
            }),
            @ClusterTest(clusterType = Type.KRAFT, serverProperties = {
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
            }),
            @ClusterTest(clusterType = Type.KRAFT, serverProperties = {
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG, value = "true"),
            })
    })
    public void testListConsumerGroupsWithStates() throws Exception {
        createTopic(TOPIC);

        String groupProtocol = clusterInstance.config().serverProperties().get("group.coordinator.new.enable") == "true" ? GroupProtocol.CONSUMER.name() : GroupProtocol.CLASSIC.name();
        try (AutoCloseable protocolConsumerGroupExecutor = ConsumerGroupExecutor.buildConsumerGroup(
                clusterInstance.bootstrapServers(),
                1,
                PROTOCOL_GROUP,
                TOPIC,
                groupProtocol,
                Optional.empty(),
                Collections.emptyMap(),
                false);

             ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--state"});
        ) {
            Set<ConsumerGroupListing> expectedListing = mkSet(
                    new ConsumerGroupListing(
                            PROTOCOL_GROUP,
                            false,
                            Optional.of(ConsumerGroupState.STABLE),
                            Optional.of(GroupType.parse(groupProtocol))
                    )
            );

            assertGroupListing(
                    service,
                    Collections.emptySet(),
                    EnumSet.allOf(ConsumerGroupState.class),
                    expectedListing
            );

            expectedListing = mkSet(
                    new ConsumerGroupListing(
                            PROTOCOL_GROUP,
                            false,
                            Optional.of(ConsumerGroupState.STABLE),
                            Optional.of(GroupType.parse(groupProtocol))
                    )
            );

            assertGroupListing(
                    service,
                    Collections.emptySet(),
                    mkSet(ConsumerGroupState.STABLE),
                    expectedListing
            );

            assertGroupListing(
                    service,
                    Collections.emptySet(),
                    mkSet(ConsumerGroupState.PREPARING_REBALANCE),
                    Collections.emptySet()
            );
        }
    }

    @ClusterTests({
            @ClusterTest(clusterType = Type.ZK, serverProperties = {
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
            }),
            @ClusterTest(clusterType = Type.KRAFT, serverProperties = {
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
            }),
            @ClusterTest(clusterType = Type.KRAFT, serverProperties = {
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG, value = "true"),
            })
    })
    public void testListConsumerGroupsWithTypesClassicProtocol() throws Exception {
        createTopic(TOPIC);

        String groupProtocol = GroupProtocol.CLASSIC.name();
        try (AutoCloseable protocolConsumerGroupExecutor = ConsumerGroupExecutor.buildConsumerGroup(
                clusterInstance.bootstrapServers(),
                1,
                PROTOCOL_GROUP,
                TOPIC,
                groupProtocol,
                Optional.empty(),
                Collections.emptyMap(),
                false);

             ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--list"});
        ) {
            Set<ConsumerGroupListing> expectedListing = mkSet(
                    new ConsumerGroupListing(
                            PROTOCOL_GROUP,
                            false,
                            Optional.of(ConsumerGroupState.STABLE),
                            Optional.of(GroupType.CLASSIC)
                    )
            );

            // No filters explicitly mentioned. Expectation is that all groups are returned.
            assertGroupListing(
                    service,
                    Collections.emptySet(),
                    Collections.emptySet(),
                    expectedListing
            );

            // When group type is mentioned:
            // Old Group Coordinator returns empty listings if the type is not Classic.
            // New Group Coordinator returns groups according to the filter.
            assertGroupListing(
                    service,
                    mkSet(GroupType.CONSUMER),
                    Collections.emptySet(),
                    Collections.emptySet()
            );

            assertGroupListing(
                    service,
                    mkSet(GroupType.CLASSIC),
                    Collections.emptySet(),
                    expectedListing
            );
        }
    }

    @ClusterTests({
            @ClusterTest(clusterType = Type.KRAFT, serverProperties = {
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG, value = "true"),
            })
    })
    public void testListConsumerGroupsWithTypesConsumerProtocol() throws Exception {
        createTopic(TOPIC);

        String groupProtocol = GroupProtocol.CONSUMER.name();
        try (AutoCloseable defaultConsumerGroupExecutor = ConsumerGroupExecutor.buildConsumerGroup(
                clusterInstance.bootstrapServers(),
                1,
                DEFAULT_GROUP,
                TOPIC,
                GroupProtocol.CLASSIC.name(),
                Optional.empty(),
                Collections.emptyMap(),
                false);

             AutoCloseable protocolConsumerGroupExecutor = ConsumerGroupExecutor.buildConsumerGroup(
                     clusterInstance.bootstrapServers(),
                     1,
                     PROTOCOL_GROUP,
                     TOPIC,
                     groupProtocol,
                     Optional.empty(),
                     Collections.emptyMap(),
                     false);

             ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(new String[]{"--bootstrap-server", clusterInstance.bootstrapServers(), "--list"});
        ) {


            // No filters explicitly mentioned. Expectation is that all groups are returned.
            Set<ConsumerGroupListing> expectedListing = mkSet(
                    new ConsumerGroupListing(
                            DEFAULT_GROUP,
                            false,
                            Optional.of(ConsumerGroupState.STABLE),
                            Optional.of(GroupType.CLASSIC)
                    ),
                    new ConsumerGroupListing(
                            PROTOCOL_GROUP,
                            false,
                            Optional.of(ConsumerGroupState.STABLE),
                            Optional.of(GroupType.CONSUMER)
                    )
            );

            assertGroupListing(
                    service,
                    Collections.emptySet(),
                    Collections.emptySet(),
                    expectedListing
            );

            // When group type is mentioned:
            // New Group Coordinator returns groups according to the filter.
            expectedListing = mkSet(
                    new ConsumerGroupListing(
                            PROTOCOL_GROUP,
                            false,
                            Optional.of(ConsumerGroupState.STABLE),
                            Optional.of(GroupType.CONSUMER)
                    )
            );

            assertGroupListing(
                    service,
                    mkSet(GroupType.CONSUMER),
                    Collections.emptySet(),
                    expectedListing
            );

            expectedListing = mkSet(
                    new ConsumerGroupListing(
                            DEFAULT_GROUP,
                            false,
                            Optional.of(ConsumerGroupState.STABLE),
                            Optional.of(GroupType.CLASSIC)
                    )
            );

            assertGroupListing(
                    service,
                    mkSet(GroupType.CLASSIC),
                    Collections.emptySet(),
                    expectedListing
            );
        }
    }

    @ClusterTest
    public void testConsumerGroupStatesFromString() {
        Set<ConsumerGroupState> result = ConsumerGroupCommand.consumerGroupStatesFromString("Stable");
        Assertions.assertEquals(set(Collections.singleton(ConsumerGroupState.STABLE)), result);

        result = ConsumerGroupCommand.consumerGroupStatesFromString("Stable, PreparingRebalance");
        Assertions.assertEquals(set(Arrays.asList(ConsumerGroupState.STABLE, ConsumerGroupState.PREPARING_REBALANCE)), result);

        result = ConsumerGroupCommand.consumerGroupStatesFromString("Dead,CompletingRebalance,");
        Assertions.assertEquals(set(Arrays.asList(ConsumerGroupState.DEAD, ConsumerGroupState.COMPLETING_REBALANCE)), result);

        result = ConsumerGroupCommand.consumerGroupStatesFromString("stable");
        Assertions.assertEquals(set(Collections.singletonList(ConsumerGroupState.STABLE)), result);

        result = ConsumerGroupCommand.consumerGroupStatesFromString("stable, assigning");
        Assertions.assertEquals(set(Arrays.asList(ConsumerGroupState.STABLE, ConsumerGroupState.ASSIGNING)), result);

        result = ConsumerGroupCommand.consumerGroupStatesFromString("dead,reconciling,");
        Assertions.assertEquals(set(Arrays.asList(ConsumerGroupState.DEAD, ConsumerGroupState.RECONCILING)), result);

        Assertions.assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupStatesFromString("bad, wrong"));

        Assertions.assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupStatesFromString("  bad, Stable"));

        Assertions.assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupStatesFromString("   ,   ,"));
    }

    @ClusterTest
    public void testConsumerGroupTypesFromString() {
        Set<GroupType> result = ConsumerGroupCommand.consumerGroupTypesFromString("consumer");
        Assertions.assertEquals(set(Collections.singleton(GroupType.CONSUMER)), result);

        result = ConsumerGroupCommand.consumerGroupTypesFromString("consumer, classic");
        Assertions.assertEquals(set(Arrays.asList(GroupType.CONSUMER, GroupType.CLASSIC)), result);

        result = ConsumerGroupCommand.consumerGroupTypesFromString("Consumer, Classic");
        Assertions.assertEquals(set(Arrays.asList(GroupType.CONSUMER, GroupType.CLASSIC)), result);

        Assertions.assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupTypesFromString("bad, wrong"));

        Assertions.assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupTypesFromString("  bad, generic"));

        Assertions.assertThrows(IllegalArgumentException.class, () -> ConsumerGroupCommand.consumerGroupTypesFromString("   ,   ,"));
    }

    @ClusterTests({
            @ClusterTest(clusterType = Type.ZK, serverProperties = {
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
            }),
            @ClusterTest(clusterType = Type.KRAFT, serverProperties = {
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
            }),
            @ClusterTest(clusterType = Type.KRAFT, serverProperties = {
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG, value = "true"),
            })
    })
    public void testListGroupCommandClassicProtocol() throws Exception {
        createTopic(TOPIC);

        String groupProtocol = GroupProtocol.CLASSIC.name();
        try (AutoCloseable protocolConsumerGroupExecutor = ConsumerGroupExecutor.buildConsumerGroup(
                clusterInstance.bootstrapServers(),
                1,
                PROTOCOL_GROUP,
                TOPIC,
                groupProtocol,
                Optional.empty(),
                Collections.emptyMap(),
                false);
        ) {

            validateListOutput(
                    Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--list"),
                    Collections.emptyList(),
                    mkSet(
                            Collections.singletonList(PROTOCOL_GROUP)
                    )
            );

            validateListOutput(
                    Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--state"),
                    Arrays.asList("GROUP", "STATE"),
                    mkSet(
                            Arrays.asList(PROTOCOL_GROUP, "Stable")
                    )
            );

            validateListOutput(
                    Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--type"),
                    Arrays.asList("GROUP", "TYPE"),
                    mkSet(
                            Arrays.asList(PROTOCOL_GROUP, "Classic")
                    )
            );

            validateListOutput(
                    Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--type", "--state"),
                    Arrays.asList("GROUP", "TYPE", "STATE"),
                    mkSet(
                            Arrays.asList(PROTOCOL_GROUP, "Classic", "Stable")
                    )
            );

            validateListOutput(
                    Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--state", "Stable"),
                    Arrays.asList("GROUP", "STATE"),
                    mkSet(
                            Arrays.asList(PROTOCOL_GROUP, "Stable")
                    )
            );

            // Check case-insensitivity in state filter.
            validateListOutput(
                    Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--state", "stable"),
                    Arrays.asList("GROUP", "STATE"),
                    mkSet(
                            Arrays.asList(PROTOCOL_GROUP, "Stable")
                    )
            );

            validateListOutput(
                    Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--type", "Classic"),
                    Arrays.asList("GROUP", "TYPE"),
                    mkSet(
                            Arrays.asList(PROTOCOL_GROUP, "Classic")
                    )
            );

            // Check case-insensitivity in type filter.
            validateListOutput(
                    Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--type", "classic"),
                    Arrays.asList("GROUP", "TYPE"),
                    mkSet(
                            Arrays.asList(PROTOCOL_GROUP, "Classic")
                    )
            );
        }
    }

    @ClusterTests({
            @ClusterTest(clusterType = Type.KRAFT, serverProperties = {
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
                    @ClusterConfigProperty(key = GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG, value = "true"),
            })
    })
    public void testListGroupCommandConsumerProtocol() throws Exception {
        createTopic(TOPIC);

        String groupProtocol = GroupProtocol.CONSUMER.name();
        try (AutoCloseable protocolConsumerGroupExecutor = ConsumerGroupExecutor.buildConsumerGroup(
                clusterInstance.bootstrapServers(),
                1,
                PROTOCOL_GROUP,
                TOPIC,
                groupProtocol,
                Optional.empty(),
                Collections.emptyMap(),
                false);
        ) {
            validateListOutput(
                    Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--list"),
                    Collections.emptyList(),
                    mkSet(
                            Collections.singletonList(PROTOCOL_GROUP)
                    )
            );

            validateListOutput(
                    Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--state"),
                    Arrays.asList("GROUP", "STATE"),
                    mkSet(
                            Arrays.asList(PROTOCOL_GROUP, "Stable")
                    )
            );

            validateListOutput(
                    Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--type"),
                    Arrays.asList("GROUP", "TYPE"),
                    mkSet(
                            Arrays.asList(PROTOCOL_GROUP, "Consumer")
                    )
            );

            validateListOutput(
                    Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--type", "--state"),
                    Arrays.asList("GROUP", "TYPE", "STATE"),
                    mkSet(
                            Arrays.asList(PROTOCOL_GROUP, "Consumer", "Stable")
                    )
            );

            validateListOutput(
                    Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--type", "consumer"),
                    Arrays.asList("GROUP", "TYPE"),
                    mkSet(
                            Arrays.asList(PROTOCOL_GROUP, "Consumer")
                    )
            );

            validateListOutput(
                    Arrays.asList("--bootstrap-server", clusterInstance.bootstrapServers(), "--list", "--type", "consumer", "--state", "Stable"),
                    Arrays.asList("GROUP", "TYPE", "STATE"),
                    mkSet(
                            Arrays.asList(PROTOCOL_GROUP, "Consumer", "Stable")
                    )
            );
        }
    }

    private ConsumerGroupCommand.ConsumerGroupService getConsumerGroupService(String[] args) {
        ConsumerGroupCommandOptions opts = ConsumerGroupCommandOptions.fromArgs(args);
        ConsumerGroupCommand.ConsumerGroupService service = new ConsumerGroupCommand.ConsumerGroupService(
                opts,
                Collections.singletonMap(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );

        return service;
    }

    private void createTopic(String topic) {
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            Assertions.assertDoesNotThrow(() -> admin.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1))).topicId(topic).get());
        }
    }

    /**
     * Validates the consumer group listings returned against expected values using specified filters.
     *
     * @param service           The service to list consumer groups.
     * @param typeFilterSet     Filters for group types, empty for no filter.
     * @param stateFilterSet    Filters for group states, empty for no filter.
     * @param expectedListing   Expected consumer group listings.
     */
    private static void assertGroupListing(
        ConsumerGroupCommand.ConsumerGroupService service,
        Set<GroupType> typeFilterSet,
        Set<ConsumerGroupState> stateFilterSet,
        Set<ConsumerGroupListing> expectedListing
    ) throws Exception {
        final AtomicReference<Set<ConsumerGroupListing>> foundListing = new AtomicReference<>();
        TestUtils.waitForCondition(() -> {
            foundListing.set(set(service.listConsumerGroupsWithFilters(set(typeFilterSet), set(stateFilterSet))));
            return Objects.equals(set(expectedListing), foundListing.get());
        }, () -> "Expected to show groups " + expectedListing + ", but found " + foundListing.get() + ".");
    }

    /**
     * Validates that the output of the list command corresponds to the expected values.
     *
     * @param args              The arguments for the command line tool.
     * @param expectedHeader    The expected header as a list of strings; or an empty list
     *                          if a header is not expected.
     * @param expectedRows      The expected rows as a set of list of columns.
     * @throws InterruptedException
     */
    private static void validateListOutput(
        List<String> args,
        List<String> expectedHeader,
        Set<List<String>> expectedRows
    ) throws InterruptedException {
        final AtomicReference<String> out = new AtomicReference<>("");
        TestUtils.waitForCondition(() -> {
            String output = ToolsTestUtils.grabConsoleOutput(() -> ConsumerGroupCommand.main(args.toArray(new String[0])));
            out.set(output);

            int index = 0;
            String[] lines = output.split("\n");

            // Parse the header if one is expected.
            if (!expectedHeader.isEmpty()) {
                if (lines.length == 0) return false;
                List<String> header = Arrays.stream(lines[index++].split("\\s+")).collect(Collectors.toList());
                if (!expectedHeader.equals(header)) {
                    return false;
                }
            }

            // Parse the groups.
            Set<List<String>> groups = new HashSet<>();
            for (; index < lines.length; index++) {
                groups.add(Arrays.stream(lines[index].split("\\s+")).collect(Collectors.toList()));
            }
            return expectedRows.equals(groups);
        }, () -> String.format("Expected header=%s and groups=%s, but found:%n%s", expectedHeader, expectedRows, out.get()));
    }

    public static <T> Set<T> set(Collection<T> set) {
        return new HashSet<>(set);
    }
}
