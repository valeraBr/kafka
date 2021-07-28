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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigTransformer;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.NoneConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.PrincipalConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.distributed.ClusterConfigState;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConfigValueInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.runtime.AbstractHerder.keysWithVariableValues;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AbstractHerderTest {

    private static final String CONN1 = "sourceA";
    private static final ConnectorTaskId TASK0 = new ConnectorTaskId(CONN1, 0);
    private static final ConnectorTaskId TASK1 = new ConnectorTaskId(CONN1, 1);
    private static final ConnectorTaskId TASK2 = new ConnectorTaskId(CONN1, 2);
    private static final Integer MAX_TASKS = 3;
    private static final Map<String, String> CONN1_CONFIG = new HashMap<>();
    private static final String TEST_KEY = "testKey";
    private static final String TEST_KEY2 = "testKey2";
    private static final String TEST_KEY3 = "testKey3";
    private static final String TEST_VAL = "testVal";
    private static final String TEST_VAL2 = "testVal2";
    private static final String TEST_REF = "${file:/tmp/somefile.txt:somevar}";
    private static final String TEST_REF2 = "${file:/tmp/somefile2.txt:somevar2}";
    private static final String TEST_REF3 = "${file:/tmp/somefile3.txt:somevar3}";
    static {
        CONN1_CONFIG.put(ConnectorConfig.NAME_CONFIG, CONN1);
        CONN1_CONFIG.put(ConnectorConfig.TASKS_MAX_CONFIG, MAX_TASKS.toString());
        CONN1_CONFIG.put(SinkConnectorConfig.TOPICS_CONFIG, "foo,bar");
        CONN1_CONFIG.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, BogusSourceConnector.class.getName());
        CONN1_CONFIG.put(TEST_KEY, TEST_REF);
        CONN1_CONFIG.put(TEST_KEY2, TEST_REF2);
        CONN1_CONFIG.put(TEST_KEY3, TEST_REF3);
    }
    private static final Map<String, String> TASK_CONFIG = new HashMap<>();
    static {
        TASK_CONFIG.put(TaskConfig.TASK_CLASS_CONFIG, BogusSourceTask.class.getName());
        TASK_CONFIG.put(TEST_KEY, TEST_REF);
    }
    private static final List<Map<String, String>> TASK_CONFIGS = new ArrayList<>();
    static {
        TASK_CONFIGS.add(TASK_CONFIG);
        TASK_CONFIGS.add(TASK_CONFIG);
        TASK_CONFIGS.add(TASK_CONFIG);
    }
    private static final HashMap<ConnectorTaskId, Map<String, String>> TASK_CONFIGS_MAP = new HashMap<>();
    static {
        TASK_CONFIGS_MAP.put(TASK0, TASK_CONFIG);
        TASK_CONFIGS_MAP.put(TASK1, TASK_CONFIG);
        TASK_CONFIGS_MAP.put(TASK2, TASK_CONFIG);
    }
    private static final ClusterConfigState SNAPSHOT = new ClusterConfigState(1, null, Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG), Collections.singletonMap(CONN1, TargetState.STARTED),
            TASK_CONFIGS_MAP, Collections.emptySet());
    private static final ClusterConfigState SNAPSHOT_NO_TASKS = new ClusterConfigState(1, null, Collections.singletonMap(CONN1, 3),
            Collections.singletonMap(CONN1, CONN1_CONFIG), Collections.singletonMap(CONN1, TargetState.STARTED),
            Collections.emptyMap(), Collections.emptySet());

    private final String workerId = "workerId";
    private final String kafkaClusterId = "I4ZmrWqfT2e-upky_4fdPA";
    private final int generation = 5;
    private final String connector = "connector";
    private final ConnectorClientConfigOverridePolicy noneConnectorClientConfigOverridePolicy = new NoneConnectorClientConfigOverridePolicy();
    private Connector insConnector;

    final private Worker worker = mock(Worker.class);
    final private WorkerConfigTransformer transformer = mock(WorkerConfigTransformer.class);
    final private Plugins plugins = mock(Plugins.class);
    final private ClassLoader classLoader = mock(ClassLoader.class);
    final private ConfigBackingStore configStore = mock(ConfigBackingStore.class);
    final private StatusBackingStore statusStore = mock(StatusBackingStore.class);
    private ClassLoader loader;

    @Before
    public void before() {
        loader = Utils.getContextOrKafkaClassLoader();
    }

    @After
    public void tearDown() {
        if (loader != null) Plugins.compareAndSwapLoaders(loader);
    }

    @Test
    public void testConnectors() {
//        AbstractHerder herder = partialMockBuilder(AbstractHerder.class)
//            .withConstructor(
//                Worker.class,
//                String.class,
//                String.class,
//                StatusBackingStore.class,
//                ConfigBackingStore.class,
//                ConnectorClientConfigOverridePolicy.class
//            )
//            .withArgs(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
//            .addMockedMethod("generation")
//            .createMock();

        AbstractHerder herder = mock(AbstractHerder.class, withSettings()
                .useConstructor(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
                .defaultAnswer(CALLS_REAL_METHODS));

        when(herder.generation()).thenReturn(generation);
        when(herder.rawConfig(connector)).thenReturn(null);
        when(configStore.snapshot()).thenReturn(SNAPSHOT);
        assertEquals(Collections.singleton(CONN1), new HashSet<>(herder.connectors()));
    }

    @Test
    public void testConnectorStatus() {
        ConnectorTaskId taskId = new ConnectorTaskId(connector, 0);

        AbstractHerder herder = mock(AbstractHerder.class, withSettings()
                .useConstructor(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
                .defaultAnswer(CALLS_REAL_METHODS));

        when(herder.generation()).thenReturn(generation);
        when(herder.rawConfig(connector)).thenReturn(null);
        when(statusStore.get(connector))
            .thenReturn(new ConnectorStatus(connector, AbstractStatus.State.RUNNING, workerId, generation));
        when(statusStore.getAll(connector))
            .thenReturn(Collections.singletonList(
                new TaskStatus(taskId, AbstractStatus.State.UNASSIGNED, workerId, generation)));

        ConnectorStateInfo csi = herder.connectorStatus(connector);
    }

    @Test
    public void connectorStatus() {
        ConnectorTaskId taskId = new ConnectorTaskId(connector, 0);

        AbstractHerder herder = mock(AbstractHerder.class, withSettings()
                .useConstructor(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
                .defaultAnswer(CALLS_REAL_METHODS));

        when(herder.generation()).thenReturn(generation);
        when(herder.rawConfig(connector)).thenReturn(null);

        when(statusStore.get(connector))
                .thenReturn(new ConnectorStatus(connector, AbstractStatus.State.RUNNING, workerId, generation));

        when(statusStore.getAll(connector))
                .thenReturn(Collections.singletonList(
                        new TaskStatus(taskId, AbstractStatus.State.UNASSIGNED, workerId, generation)));
        when(worker.getPlugins()).thenReturn(plugins);

        ConnectorStateInfo state = herder.connectorStatus(connector);

        assertEquals(connector, state.name());
        assertEquals("RUNNING", state.connector().state());
        assertEquals(1, state.tasks().size());
        assertEquals(workerId, state.connector().workerId());

        ConnectorStateInfo.TaskState taskState = state.tasks().get(0);
        assertEquals(0, taskState.id());
        assertEquals("UNASSIGNED", taskState.state());
        assertEquals(workerId, taskState.workerId());
    }

    @Test
    public void taskStatus() {
        ConnectorTaskId taskId = new ConnectorTaskId("connector", 0);
        String workerId = "workerId";

        AbstractHerder herder = mock(AbstractHerder.class, withSettings()
                .useConstructor(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
                .defaultAnswer(CALLS_REAL_METHODS));

        when(herder.generation()).thenReturn(5);

        final ArgumentCaptor<TaskStatus> taskStatusArgumentCaptor = ArgumentCaptor.forClass(TaskStatus.class);
        doNothing().when(statusStore).putSafe(taskStatusArgumentCaptor.capture());

        when(statusStore.get(taskId)).thenAnswer(invocation -> taskStatusArgumentCaptor.getValue());

        herder.onFailure(taskId, new RuntimeException());

        ConnectorStateInfo.TaskState taskState = herder.taskStatus(taskId);
        assertEquals(workerId, taskState.workerId());
        assertEquals("FAILED", taskState.state());
        assertEquals(0, taskState.id());
        assertNotNull(taskState.trace());
    }

    @Test
    public void testBuildRestartPlanForUnknownConnector() {
        String connectorName = "UnknownConnector";
        RestartRequest restartRequest = new RestartRequest(connectorName, false, true);
        AbstractHerder herder = mock(AbstractHerder.class, withSettings()
                .useConstructor(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
                .defaultAnswer(CALLS_REAL_METHODS));

        when(herder.generation()).thenReturn(generation);

        when(statusStore.get(connectorName)).thenReturn(null);

        Optional<RestartPlan> mayBeRestartPlan = herder.buildRestartPlan(restartRequest);

        assertFalse(mayBeRestartPlan.isPresent());
    }

    @Test
    public void testBuildRestartPlanForConnectorAndTasks() {
        RestartRequest restartRequest = new RestartRequest(connector, false, true);

        ConnectorTaskId taskId1 = new ConnectorTaskId(connector, 1);
        ConnectorTaskId taskId2 = new ConnectorTaskId(connector, 2);
        List<TaskStatus> taskStatuses = new ArrayList<>();
        taskStatuses.add(new TaskStatus(taskId1, AbstractStatus.State.RUNNING, workerId, generation));
        taskStatuses.add(new TaskStatus(taskId2, AbstractStatus.State.FAILED, workerId, generation));

        AbstractHerder herder = mock(AbstractHerder.class, withSettings()
                .useConstructor(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
                .defaultAnswer(CALLS_REAL_METHODS));

        when(herder.generation()).thenReturn(generation);
        when(herder.rawConfig(connector)).thenReturn(null);

        when(statusStore.get(connector))
                .thenReturn(new ConnectorStatus(connector, AbstractStatus.State.RUNNING, workerId, generation));

        when(statusStore.getAll(connector))
                .thenReturn(taskStatuses);
        when(worker.getPlugins()).thenReturn(plugins);


        Optional<RestartPlan> mayBeRestartPlan = herder.buildRestartPlan(restartRequest);

        assertTrue(mayBeRestartPlan.isPresent());
        RestartPlan restartPlan = mayBeRestartPlan.get();
        assertTrue(restartPlan.shouldRestartConnector());
        assertTrue(restartPlan.shouldRestartTasks());
        assertEquals(2, restartPlan.taskIdsToRestart().size());
        assertTrue(restartPlan.taskIdsToRestart().contains(taskId1));
        assertTrue(restartPlan.taskIdsToRestart().contains(taskId2));
    }

    @Test
    public void testBuildRestartPlanForNoRestart() {
        RestartRequest restartRequest = new RestartRequest(connector, true, false);

        ConnectorTaskId taskId1 = new ConnectorTaskId(connector, 1);
        ConnectorTaskId taskId2 = new ConnectorTaskId(connector, 2);
        List<TaskStatus> taskStatuses = new ArrayList<>();
        taskStatuses.add(new TaskStatus(taskId1, AbstractStatus.State.RUNNING, workerId, generation));
        taskStatuses.add(new TaskStatus(taskId2, AbstractStatus.State.FAILED, workerId, generation));

        AbstractHerder herder = mock(AbstractHerder.class, withSettings()
                .useConstructor(worker, workerId, kafkaClusterId, statusStore, configStore, noneConnectorClientConfigOverridePolicy)
                .defaultAnswer(CALLS_REAL_METHODS));

        when(herder.generation()).thenReturn(generation);
        when(herder.rawConfig(connector)).thenReturn(null);

        when(statusStore.get(connector))
                .thenReturn(new ConnectorStatus(connector, AbstractStatus.State.RUNNING, workerId, generation));

        when(statusStore.getAll(connector))
                .thenReturn(taskStatuses);
        when(worker.getPlugins()).thenReturn(plugins);

        Optional<RestartPlan> mayBeRestartPlan = herder.buildRestartPlan(restartRequest);

        assertTrue(mayBeRestartPlan.isPresent());
        RestartPlan restartPlan = mayBeRestartPlan.get();
        assertFalse(restartPlan.shouldRestartConnector());
        assertFalse(restartPlan.shouldRestartTasks());
        assertTrue(restartPlan.taskIdsToRestart().isEmpty());
    }

    @Test
    public void testConfigValidationEmptyConfig() {
        AbstractHerder herder = createConfigValidationHerder(TestSourceConnector.class, noneConnectorClientConfigOverridePolicy, 0);

        assertThrows(BadRequestException.class, () -> herder.validateConnectorConfig(Collections.emptyMap(), false));
        verify(worker, times(2)).configTransformer();

    }

    @Test()
    public void testConfigValidationMissingName() {
        final Class<? extends Connector> connectorClass = TestSourceConnector.class;
        AbstractHerder herder = createConfigValidationHerder(connectorClass, noneConnectorClientConfigOverridePolicy);

        Map<String, String> config = Collections.singletonMap(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        ConfigInfos result = herder.validateConnectorConfig(config, false);

        // We expect there to be errors due to the missing name and .... Note that these assertions depend heavily on
        // the config fields for SourceConnectorConfig, but we expect these to change rarely.
        assertEquals(connectorClass.getName(), result.name());
        assertEquals(Arrays.asList(ConnectorConfig.COMMON_GROUP, ConnectorConfig.TRANSFORMS_GROUP,
                ConnectorConfig.PREDICATES_GROUP, ConnectorConfig.ERROR_GROUP, SourceConnectorConfig.TOPIC_CREATION_GROUP), result.groups());
        assertEquals(2, result.errorCount());
        Map<String, ConfigInfo> infos = result.values().stream()
                .collect(Collectors.toMap(info -> info.configKey().name(), Function.identity()));
        // Base connector config has 14 fields, connector's configs add 2
        assertEquals(17, infos.size());
        // Missing name should generate an error
        assertEquals(ConnectorConfig.NAME_CONFIG,
                infos.get(ConnectorConfig.NAME_CONFIG).configValue().name());
        assertEquals(1, infos.get(ConnectorConfig.NAME_CONFIG).configValue().errors().size());
        // "required" config from connector should generate an error
        assertEquals("required", infos.get("required").configValue().name());
        assertEquals(1, infos.get("required").configValue().errors().size());

        verify(plugins).newConnector(connectorClass.getName());
        verify(plugins).compareAndSwapLoaders(insConnector);
    }

    @Test
    public void testConfigValidationInvalidTopics() {
        final Class<? extends Connector> connectorClass = TestSinkConnector.class;
        AbstractHerder herder = createConfigValidationHerder(connectorClass, noneConnectorClientConfigOverridePolicy);

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        config.put(SinkConnectorConfig.TOPICS_CONFIG, "topic1,topic2");
        config.put(SinkConnectorConfig.TOPICS_REGEX_CONFIG, "topic.*");

        assertThrows(ConfigException.class, () -> herder.validateConnectorConfig(config, false));

        verify(plugins).newConnector(connectorClass.getName());
        verify(plugins).compareAndSwapLoaders(insConnector);
    }

    @Test
    public void testConfigValidationTopicsWithDlq() {
        final Class<? extends Connector> connectorClass = TestSinkConnector.class;
        AbstractHerder herder = createConfigValidationHerder(TestSinkConnector.class, noneConnectorClientConfigOverridePolicy);

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        config.put(SinkConnectorConfig.TOPICS_CONFIG, "topic1");
        config.put(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, "topic1");

        assertThrows(ConfigException.class, () -> herder.validateConnectorConfig(config, false));

        verify(plugins).newConnector(connectorClass.getName());
        verify(plugins).compareAndSwapLoaders(insConnector);
    }

    @Test
    public void testConfigValidationTopicsRegexWithDlq() {
        final Class<? extends Connector> connectorClass = TestSinkConnector.class;
        AbstractHerder herder = createConfigValidationHerder(connectorClass, noneConnectorClientConfigOverridePolicy);

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        config.put(SinkConnectorConfig.TOPICS_REGEX_CONFIG, "topic.*");
        config.put(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, "topic1");

        assertThrows(ConfigException.class, () -> herder.validateConnectorConfig(config, false));

        verify(plugins).newConnector(connectorClass.getName());
        verify(plugins).compareAndSwapLoaders(insConnector);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test()
    public void testConfigValidationTransformsExtendResults() {
        final Class<? extends Connector> connectorClass = TestSourceConnector.class;
        AbstractHerder herder = createConfigValidationHerder(connectorClass, noneConnectorClientConfigOverridePolicy);

        // 2 transform aliases defined -> 2 plugin lookups
        Set<PluginDesc<Transformation<?>>> transformations = new HashSet<>();
        transformations.add(transformationPluginDesc());
        when(plugins.transformations()).thenReturn(transformations);

        // Define 2 transformations. One has a class defined and so can get embedded configs, the other is missing
        // class info that should generate an error.
        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        config.put(ConnectorConfig.NAME_CONFIG, "connector-name");
        config.put(ConnectorConfig.TRANSFORMS_CONFIG, "xformA,xformB");
        config.put(ConnectorConfig.TRANSFORMS_CONFIG + ".xformA.type", SampleTransformation.class.getName());
        config.put("required", "value"); // connector required config
        ConfigInfos result = herder.validateConnectorConfig(config, false);
        assertEquals(herder.connectorTypeForClass(config.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG)), ConnectorType.SOURCE);

        // We expect there to be errors due to the missing name and .... Note that these assertions depend heavily on
        // the config fields for SourceConnectorConfig, but we expect these to change rarely.
        assertEquals(connectorClass.getName(), result.name());
        // Each transform also gets its own group
        List<String> expectedGroups = Arrays.asList(
                ConnectorConfig.COMMON_GROUP,
                ConnectorConfig.TRANSFORMS_GROUP,
                ConnectorConfig.PREDICATES_GROUP,
                ConnectorConfig.ERROR_GROUP,
                SourceConnectorConfig.TOPIC_CREATION_GROUP,
                "Transforms: xformA",
                "Transforms: xformB"
        );
        assertEquals(expectedGroups, result.groups());
        assertEquals(2, result.errorCount());
        Map<String, ConfigInfo> infos = result.values().stream()
                .collect(Collectors.toMap(info -> info.configKey().name(), Function.identity()));
        assertEquals(22, infos.size());
        // Should get 2 type fields from the transforms, first adds its own config since it has a valid class
        assertEquals("transforms.xformA.type",
                infos.get("transforms.xformA.type").configValue().name());
        assertTrue(infos.get("transforms.xformA.type").configValue().errors().isEmpty());
        assertEquals("transforms.xformA.subconfig",
                infos.get("transforms.xformA.subconfig").configValue().name());
        assertEquals("transforms.xformB.type", infos.get("transforms.xformB.type").configValue().name());
        assertFalse(infos.get("transforms.xformB.type").configValue().errors().isEmpty());

        verify(plugins, times(2)).transformations();
        verify(plugins).newConnector(connectorClass.getName());
        verify(plugins).compareAndSwapLoaders(insConnector);
    }

    @Test()
    public void testConfigValidationPredicatesExtendResults() {
        final Class<? extends Connector> connectorClass = TestSourceConnector.class;
        AbstractHerder herder = createConfigValidationHerder(connectorClass, noneConnectorClientConfigOverridePolicy);

        // 2 transform aliases defined -> 2 plugin lookups
        Set<PluginDesc<Transformation<?>>> transformations = new HashSet<>();
        transformations.add(transformationPluginDesc());
        when(plugins.transformations()).thenReturn(transformations);

        Set<PluginDesc<Predicate<?>>> predicates = new HashSet<>();
        predicates.add(predicatePluginDesc());
        when(plugins.predicates()).thenReturn(predicates);

        // Define 2 transformations. One has a class defined and so can get embedded configs, the other is missing
        // class info that should generate an error.
        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        config.put(ConnectorConfig.NAME_CONFIG, "connector-name");
        config.put(ConnectorConfig.TRANSFORMS_CONFIG, "xformA");
        config.put(ConnectorConfig.TRANSFORMS_CONFIG + ".xformA.type", SampleTransformation.class.getName());
        config.put(ConnectorConfig.TRANSFORMS_CONFIG + ".xformA.predicate", "predX");
        config.put(ConnectorConfig.PREDICATES_CONFIG, "predX,predY");
        config.put(ConnectorConfig.PREDICATES_CONFIG + ".predX.type", SamplePredicate.class.getName());
        config.put("required", "value"); // connector required config
        ConfigInfos result = herder.validateConnectorConfig(config, false);
        assertEquals(herder.connectorTypeForClass(config.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG)), ConnectorType.SOURCE);

        // We expect there to be errors due to the missing name and .... Note that these assertions depend heavily on
        // the config fields for SourceConnectorConfig, but we expect these to change rarely.
        assertEquals(connectorClass.getName(), result.name());
        // Each transform also gets its own group
        List<String> expectedGroups = Arrays.asList(
                ConnectorConfig.COMMON_GROUP,
                ConnectorConfig.TRANSFORMS_GROUP,
                ConnectorConfig.PREDICATES_GROUP,
                ConnectorConfig.ERROR_GROUP,
                SourceConnectorConfig.TOPIC_CREATION_GROUP,
                "Transforms: xformA",
                "Predicates: predX",
                "Predicates: predY"
        );
        assertEquals(expectedGroups, result.groups());
        assertEquals(2, result.errorCount());
        Map<String, ConfigInfo> infos = result.values().stream()
                .collect(Collectors.toMap(info -> info.configKey().name(), Function.identity()));
        assertEquals(24, infos.size());
        // Should get 2 type fields from the transforms, first adds its own config since it has a valid class
        assertEquals("transforms.xformA.type",
                infos.get("transforms.xformA.type").configValue().name());
        assertTrue(infos.get("transforms.xformA.type").configValue().errors().isEmpty());
        assertEquals("transforms.xformA.subconfig",
                infos.get("transforms.xformA.subconfig").configValue().name());
        assertEquals("transforms.xformA.predicate",
                infos.get("transforms.xformA.predicate").configValue().name());
        assertTrue(infos.get("transforms.xformA.predicate").configValue().errors().isEmpty());
        assertEquals("transforms.xformA.negate",
                infos.get("transforms.xformA.negate").configValue().name());
        assertTrue(infos.get("transforms.xformA.negate").configValue().errors().isEmpty());
        assertEquals("predicates.predX.type",
                infos.get("predicates.predX.type").configValue().name());
        assertEquals("predicates.predX.predconfig",
                infos.get("predicates.predX.predconfig").configValue().name());
        assertEquals("predicates.predY.type",
                infos.get("predicates.predY.type").configValue().name());
        assertFalse(
                infos.get("predicates.predY.type").configValue().errors().isEmpty());

        verify(plugins).transformations();
        verify(plugins, times(2)).predicates();
        verify(plugins).newConnector(connectorClass.getName());
        verify(plugins).compareAndSwapLoaders(insConnector);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private PluginDesc<Predicate<?>> predicatePluginDesc() {
        return new PluginDesc(SamplePredicate.class, "1.0", classLoader);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private PluginDesc<Transformation<?>> transformationPluginDesc() {
        return new PluginDesc(SampleTransformation.class, "1.0", classLoader);
    }

    @Test()
    public void testConfigValidationPrincipalOnlyOverride() throws Throwable {
        final Class<? extends Connector> connectorClass = TestSourceConnector.class;
        AbstractHerder herder = createConfigValidationHerder(connectorClass, new PrincipalConnectorClientConfigOverridePolicy());

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        config.put(ConnectorConfig.NAME_CONFIG, "connector-name");
        config.put("required", "value"); // connector required config
        String ackConfigKey = producerOverrideKey(ProducerConfig.ACKS_CONFIG);
        String saslConfigKey = producerOverrideKey(SaslConfigs.SASL_JAAS_CONFIG);
        config.put(ackConfigKey, "none");
        config.put(saslConfigKey, "jaas_config");

        ConfigInfos result = herder.validateConnectorConfig(config, false);
        assertEquals(herder.connectorTypeForClass(config.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG)), ConnectorType.SOURCE);

        // We expect there to be errors due to now allowed override policy for ACKS.... Note that these assertions depend heavily on
        // the config fields for SourceConnectorConfig, but we expect these to change rarely.
        assertEquals(TestSourceConnector.class.getName(), result.name());
        // Each transform also gets its own group
        List<String> expectedGroups = Arrays.asList(
            ConnectorConfig.COMMON_GROUP,
            ConnectorConfig.TRANSFORMS_GROUP,
            ConnectorConfig.PREDICATES_GROUP,
            ConnectorConfig.ERROR_GROUP,
            SourceConnectorConfig.TOPIC_CREATION_GROUP
        );
        assertEquals(expectedGroups, result.groups());
        assertEquals(1, result.errorCount());
        // Base connector config has 14 fields, connector's configs add 2, and 2 producer overrides
        assertEquals(19, result.values().size());
        assertTrue(result.values().stream().anyMatch(
            configInfo -> ackConfigKey.equals(configInfo.configValue().name()) && !configInfo.configValue().errors().isEmpty()));
        assertTrue(result.values().stream().anyMatch(
            configInfo -> saslConfigKey.equals(configInfo.configValue().name()) && configInfo.configValue().errors().isEmpty()));

        verify(plugins).newConnector(connectorClass.getName());
        verify(plugins).compareAndSwapLoaders(insConnector);
    }

    @Test
    public void testConfigValidationAllOverride() throws Throwable {
        final Class<? extends Connector> connectorClass = TestSourceConnector.class;
        AbstractHerder herder = createConfigValidationHerder(TestSourceConnector.class, new AllConnectorClientConfigOverridePolicy());

        Map<String, String> config = new HashMap<>();
        config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connectorClass.getName());
        config.put(ConnectorConfig.NAME_CONFIG, "connector-name");
        config.put("required", "value"); // connector required config
        // Try to test a variety of configuration types: string, int, long, boolean, list, class
        String protocolConfigKey = producerOverrideKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
        config.put(protocolConfigKey, "SASL_PLAINTEXT");
        String maxRequestSizeConfigKey = producerOverrideKey(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
        config.put(maxRequestSizeConfigKey, "420");
        String maxBlockConfigKey = producerOverrideKey(ProducerConfig.MAX_BLOCK_MS_CONFIG);
        config.put(maxBlockConfigKey, "28980");
        String idempotenceConfigKey = producerOverrideKey(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
        config.put(idempotenceConfigKey, "true");
        String bootstrapServersConfigKey = producerOverrideKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        config.put(bootstrapServersConfigKey, "SASL_PLAINTEXT://localhost:12345,SASL_PLAINTEXT://localhost:23456");
        String loginCallbackHandlerConfigKey = producerOverrideKey(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS);
        config.put(loginCallbackHandlerConfigKey, OAuthBearerUnsecuredLoginCallbackHandler.class.getName());

        final Set<String> overriddenClientConfigs = new HashSet<>();
        overriddenClientConfigs.add(protocolConfigKey);
        overriddenClientConfigs.add(maxRequestSizeConfigKey);
        overriddenClientConfigs.add(maxBlockConfigKey);
        overriddenClientConfigs.add(idempotenceConfigKey);
        overriddenClientConfigs.add(bootstrapServersConfigKey);
        overriddenClientConfigs.add(loginCallbackHandlerConfigKey);

        ConfigInfos result = herder.validateConnectorConfig(config, false);
        assertEquals(herder.connectorTypeForClass(config.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG)), ConnectorType.SOURCE);

        Map<String, String> validatedOverriddenClientConfigs = new HashMap<>();
        for (ConfigInfo configInfo : result.values()) {
            String configName = configInfo.configKey().name();
            if (overriddenClientConfigs.contains(configName)) {
                validatedOverriddenClientConfigs.put(configName, configInfo.configValue().value());
            }
        }
        Map<String, String> rawOverriddenClientConfigs = config.entrySet().stream()
            .filter(e -> overriddenClientConfigs.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        assertEquals(rawOverriddenClientConfigs, validatedOverriddenClientConfigs);

        verify(plugins).newConnector(connectorClass.getName());
        verify(plugins).compareAndSwapLoaders(insConnector);
    }

    @Test
    public void testReverseTransformConfigs() {
        // Construct a task config with constant values for TEST_KEY and TEST_KEY2
        Map<String, String> newTaskConfig = new HashMap<>();
        newTaskConfig.put(TaskConfig.TASK_CLASS_CONFIG, BogusSourceTask.class.getName());
        newTaskConfig.put(TEST_KEY, TEST_VAL);
        newTaskConfig.put(TEST_KEY2, TEST_VAL2);
        List<Map<String, String>> newTaskConfigs = new ArrayList<>();
        newTaskConfigs.add(newTaskConfig);

        // The SNAPSHOT has a task config with TEST_KEY and TEST_REF
        List<Map<String, String>> reverseTransformed = AbstractHerder.reverseTransform(CONN1, SNAPSHOT, newTaskConfigs);
        assertEquals(TEST_REF, reverseTransformed.get(0).get(TEST_KEY));

        // The SNAPSHOT has no task configs but does have a connector config with TEST_KEY2 and TEST_REF2
        reverseTransformed = AbstractHerder.reverseTransform(CONN1, SNAPSHOT_NO_TASKS, newTaskConfigs);
        assertEquals(TEST_REF2, reverseTransformed.get(0).get(TEST_KEY2));

        // The reverseTransformed result should not have TEST_KEY3 since newTaskConfigs does not have TEST_KEY3
        reverseTransformed = AbstractHerder.reverseTransform(CONN1, SNAPSHOT_NO_TASKS, newTaskConfigs);
        assertFalse(reverseTransformed.get(0).containsKey(TEST_KEY3));
    }

    @Test
    public void testConfigProviderRegex() {
        testConfigProviderRegex("\"${::}\"");
        testConfigProviderRegex("${::}");
        testConfigProviderRegex("\"${:/a:somevar}\"");
        testConfigProviderRegex("\"${file::somevar}\"");
        testConfigProviderRegex("${file:/a/b/c:}");
        testConfigProviderRegex("${file:/tmp/somefile.txt:somevar}");
        testConfigProviderRegex("\"${file:/tmp/somefile.txt:somevar}\"");
        testConfigProviderRegex("plain.PlainLoginModule required username=\"${file:/tmp/somefile.txt:somevar}\"");
        testConfigProviderRegex("plain.PlainLoginModule required username=${file:/tmp/somefile.txt:somevar}");
        testConfigProviderRegex("plain.PlainLoginModule required username=${file:/tmp/somefile.txt:somevar} not null");
        testConfigProviderRegex("plain.PlainLoginModule required username=${file:/tmp/somefile.txt:somevar} password=${file:/tmp/somefile.txt:othervar}");
        testConfigProviderRegex("plain.PlainLoginModule required username", false);
    }

    @Test
    public void testGenerateResultWithConfigValuesAllUsingConfigKeysAndWithNoErrors() {
        String name = "com.acme.connector.MyConnector";
        Map<String, ConfigDef.ConfigKey> keys = new HashMap<>();
        addConfigKey(keys, "config.a1", null);
        addConfigKey(keys, "config.b1", "group B");
        addConfigKey(keys, "config.b2", "group B");
        addConfigKey(keys, "config.c1", "group C");

        List<String> groups = Arrays.asList("groupB", "group C");
        List<ConfigValue> values = new ArrayList<>();
        addValue(values, "config.a1", "value.a1");
        addValue(values, "config.b1", "value.b1");
        addValue(values, "config.b2", "value.b2");
        addValue(values, "config.c1", "value.c1");

        ConfigInfos infos = AbstractHerder.generateResult(name, keys, values, groups);
        assertEquals(name, infos.name());
        assertEquals(groups, infos.groups());
        assertEquals(values.size(), infos.values().size());
        assertEquals(0, infos.errorCount());
        assertInfoKey(infos, "config.a1", null);
        assertInfoKey(infos, "config.b1", "group B");
        assertInfoKey(infos, "config.b2", "group B");
        assertInfoKey(infos, "config.c1", "group C");
        assertInfoValue(infos, "config.a1", "value.a1");
        assertInfoValue(infos, "config.b1", "value.b1");
        assertInfoValue(infos, "config.b2", "value.b2");
        assertInfoValue(infos, "config.c1", "value.c1");
    }

    @Test
    public void testGenerateResultWithConfigValuesAllUsingConfigKeysAndWithSomeErrors() {
        String name = "com.acme.connector.MyConnector";
        Map<String, ConfigDef.ConfigKey> keys = new HashMap<>();
        addConfigKey(keys, "config.a1", null);
        addConfigKey(keys, "config.b1", "group B");
        addConfigKey(keys, "config.b2", "group B");
        addConfigKey(keys, "config.c1", "group C");

        List<String> groups = Arrays.asList("groupB", "group C");
        List<ConfigValue> values = new ArrayList<>();
        addValue(values, "config.a1", "value.a1");
        addValue(values, "config.b1", "value.b1");
        addValue(values, "config.b2", "value.b2");
        addValue(values, "config.c1", "value.c1", "error c1");

        ConfigInfos infos = AbstractHerder.generateResult(name, keys, values, groups);
        assertEquals(name, infos.name());
        assertEquals(groups, infos.groups());
        assertEquals(values.size(), infos.values().size());
        assertEquals(1, infos.errorCount());
        assertInfoKey(infos, "config.a1", null);
        assertInfoKey(infos, "config.b1", "group B");
        assertInfoKey(infos, "config.b2", "group B");
        assertInfoKey(infos, "config.c1", "group C");
        assertInfoValue(infos, "config.a1", "value.a1");
        assertInfoValue(infos, "config.b1", "value.b1");
        assertInfoValue(infos, "config.b2", "value.b2");
        assertInfoValue(infos, "config.c1", "value.c1", "error c1");
    }

    @Test
    public void testGenerateResultWithConfigValuesMoreThanConfigKeysAndWithSomeErrors() {
        String name = "com.acme.connector.MyConnector";
        Map<String, ConfigDef.ConfigKey> keys = new HashMap<>();
        addConfigKey(keys, "config.a1", null);
        addConfigKey(keys, "config.b1", "group B");
        addConfigKey(keys, "config.b2", "group B");
        addConfigKey(keys, "config.c1", "group C");

        List<String> groups = Arrays.asList("groupB", "group C");
        List<ConfigValue> values = new ArrayList<>();
        addValue(values, "config.a1", "value.a1");
        addValue(values, "config.b1", "value.b1");
        addValue(values, "config.b2", "value.b2");
        addValue(values, "config.c1", "value.c1", "error c1");
        addValue(values, "config.extra1", "value.extra1");
        addValue(values, "config.extra2", "value.extra2", "error extra2");

        ConfigInfos infos = AbstractHerder.generateResult(name, keys, values, groups);
        assertEquals(name, infos.name());
        assertEquals(groups, infos.groups());
        assertEquals(values.size(), infos.values().size());
        assertEquals(2, infos.errorCount());
        assertInfoKey(infos, "config.a1", null);
        assertInfoKey(infos, "config.b1", "group B");
        assertInfoKey(infos, "config.b2", "group B");
        assertInfoKey(infos, "config.c1", "group C");
        assertNoInfoKey(infos, "config.extra1");
        assertNoInfoKey(infos, "config.extra2");
        assertInfoValue(infos, "config.a1", "value.a1");
        assertInfoValue(infos, "config.b1", "value.b1");
        assertInfoValue(infos, "config.b2", "value.b2");
        assertInfoValue(infos, "config.c1", "value.c1", "error c1");
        assertInfoValue(infos, "config.extra1", "value.extra1");
        assertInfoValue(infos, "config.extra2", "value.extra2", "error extra2");
    }

    @Test
    public void testGenerateResultWithConfigValuesWithNoConfigKeysAndWithSomeErrors() {
        String name = "com.acme.connector.MyConnector";
        Map<String, ConfigDef.ConfigKey> keys = new HashMap<>();

        List<String> groups = new ArrayList<>();
        List<ConfigValue> values = new ArrayList<>();
        addValue(values, "config.a1", "value.a1");
        addValue(values, "config.b1", "value.b1");
        addValue(values, "config.b2", "value.b2");
        addValue(values, "config.c1", "value.c1", "error c1");
        addValue(values, "config.extra1", "value.extra1");
        addValue(values, "config.extra2", "value.extra2", "error extra2");

        ConfigInfos infos = AbstractHerder.generateResult(name, keys, values, groups);
        assertEquals(name, infos.name());
        assertEquals(groups, infos.groups());
        assertEquals(values.size(), infos.values().size());
        assertEquals(2, infos.errorCount());
        assertNoInfoKey(infos, "config.a1");
        assertNoInfoKey(infos, "config.b1");
        assertNoInfoKey(infos, "config.b2");
        assertNoInfoKey(infos, "config.c1");
        assertNoInfoKey(infos, "config.extra1");
        assertNoInfoKey(infos, "config.extra2");
        assertInfoValue(infos, "config.a1", "value.a1");
        assertInfoValue(infos, "config.b1", "value.b1");
        assertInfoValue(infos, "config.b2", "value.b2");
        assertInfoValue(infos, "config.c1", "value.c1", "error c1");
        assertInfoValue(infos, "config.extra1", "value.extra1");
        assertInfoValue(infos, "config.extra2", "value.extra2", "error extra2");
    }

    protected void addConfigKey(Map<String, ConfigDef.ConfigKey> keys, String name, String group) {
        keys.put(name, new ConfigDef.ConfigKey(name, ConfigDef.Type.STRING, null, null,
                ConfigDef.Importance.HIGH, "doc", group, 10,
                ConfigDef.Width.MEDIUM, "display name", Collections.emptyList(), null, false));
    }

    protected void addValue(List<ConfigValue> values, String name, String value, String...errors) {
        values.add(new ConfigValue(name, value, new ArrayList<>(), Arrays.asList(errors)));
    }

    protected void assertInfoKey(ConfigInfos infos, String name, String group) {
        ConfigInfo info = findInfo(infos, name);
        assertEquals(name, info.configKey().name());
        assertEquals(group, info.configKey().group());
    }

    protected void assertNoInfoKey(ConfigInfos infos, String name) {
        ConfigInfo info = findInfo(infos, name);
        assertNull(info.configKey());
    }

    protected void assertInfoValue(ConfigInfos infos, String name, String value, String...errors) {
        ConfigValueInfo info = findInfo(infos, name).configValue();
        assertEquals(name, info.name());
        assertEquals(value, info.value());
        assertEquals(Arrays.asList(errors), info.errors());
    }

    protected ConfigInfo findInfo(ConfigInfos infos, String name) {
        return infos.values()
                    .stream()
                    .filter(i -> i.configValue().name().equals(name))
                    .findFirst()
                    .orElse(null);
    }

    private void testConfigProviderRegex(String rawConnConfig) {
        testConfigProviderRegex(rawConnConfig, true);
    }

    private void testConfigProviderRegex(String rawConnConfig, boolean expected) {
        Set<String> keys = keysWithVariableValues(Collections.singletonMap("key", rawConnConfig), ConfigTransformer.DEFAULT_PATTERN);
        boolean actual = keys != null && !keys.isEmpty() && keys.contains("key");
        assertEquals(String.format("%s should have matched regex", rawConnConfig), expected, actual);
    }

    private AbstractHerder createConfigValidationHerder(Class<? extends Connector> connectorClass,
                                                        ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy) {
        return createConfigValidationHerder(connectorClass, connectorClientConfigOverridePolicy, 1);
    }

    private AbstractHerder createConfigValidationHerder(Class<? extends Connector> connectorClass,
                                                        ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                                                        int countOfCallingNewConnector) {

        AbstractHerder herder = mock(AbstractHerder.class, withSettings()
                .useConstructor(worker, workerId, kafkaClusterId, statusStore, configStore, connectorClientConfigOverridePolicy)
                .defaultAnswer(CALLS_REAL_METHODS));

        when(herder.generation()).thenReturn(generation);

        // Call to validateConnectorConfig
        when(worker.configTransformer()).thenReturn(transformer);
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<Map<String, String>> mapArgumentCaptor = ArgumentCaptor.forClass(Map.class);
        when(transformer.transform(mapArgumentCaptor.capture())).thenAnswer(invocation -> mapArgumentCaptor.getValue());
        when(worker.getPlugins()).thenReturn(plugins);
        final Connector connector;
        try {
            connector = connectorClass.getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Couldn't create connector", e);
        }
        if (countOfCallingNewConnector > 0) {
            when(plugins.newConnector(connectorClass.getName())).thenReturn(connector);
            when(plugins.compareAndSwapLoaders(connector)).thenReturn(classLoader);
        }
        insConnector = connector;
        return herder;
    }

    public static class SampleTransformation<R extends ConnectRecord<R>> implements Transformation<R> {
        @Override
        public void configure(Map<String, ?> configs) {

        }

        @Override
        public R apply(R record) {
            return record;
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef()
                           .define("subconfig", ConfigDef.Type.STRING, "default", ConfigDef.Importance.LOW, "docs");
        }

        @Override
        public void close() {

        }
    }

    public static class SamplePredicate<R extends ConnectRecord<R>> implements Predicate<R> {

        @Override
        public ConfigDef config() {
            return new ConfigDef()
                    .define("predconfig", ConfigDef.Type.STRING, "default", ConfigDef.Importance.LOW, "docs");
        }

        @Override
        public boolean test(R record) {
            return false;
        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> configs) {

        }
    }

    // We need to use a real class here due to some issue with mocking java.lang.Class
    private abstract class BogusSourceConnector extends SourceConnector {
    }

    private abstract class BogusSourceTask extends SourceTask {
    }

    private static String producerOverrideKey(String config) {
        return ConnectorConfig.CONNECTOR_CLIENT_PRODUCER_OVERRIDES_PREFIX + config;
    }
}
