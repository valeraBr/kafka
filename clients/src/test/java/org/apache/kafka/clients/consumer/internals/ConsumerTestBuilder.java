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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchConfig;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.configuredIsolationLevel;
import static org.mockito.Mockito.spy;

public class ConsumerTestBuilder implements Closeable {

    static final long RETRY_BACKOFF_MS = 80;
    static final long RETRY_BACKOFF_MAX_MS = 1000;
    static final int REQUEST_TIMEOUT_MS = 500;

    final LogContext logContext = new LogContext();
    final Time time = new MockTime(0);
    final BlockingQueue<ApplicationEvent> applicationEventQueue;
    final LinkedBlockingQueue<BackgroundEvent> backgroundEventQueue;
    final ConsumerConfig config;
    final long retryBackoffMs;
    final SubscriptionState subscriptions;
    final ConsumerMetadata metadata;
    final FetchConfig<String, String> fetchConfig;
    final Metrics metrics;
    final FetchMetricsManager metricsManager;
    final NetworkClientDelegate networkClientDelegate;
    final OffsetsRequestManager offsetsRequestManager;
    final CoordinatorRequestManager coordinatorRequestManager;
    final CommitRequestManager commitRequestManager;
    final FetchRequestManager<String, String> fetchRequestManager;
    final RequestManagers<String, String> requestManagers;
    final ApplicationEventProcessor<String, String> applicationEventProcessor;
    final BackgroundEventProcessor backgroundEventProcessor;
    final MockClient client;

    public ConsumerTestBuilder() {
        this.applicationEventQueue = new LinkedBlockingQueue<>();
        this.backgroundEventQueue = new LinkedBlockingQueue<>();
        ErrorEventHandler errorEventHandler = new ErrorEventHandler(backgroundEventQueue);
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
                100,
                100,
                100,
                "group_id",
                Optional.empty(),
                RETRY_BACKOFF_MS,
                RETRY_BACKOFF_MAX_MS,
                true);
        GroupState groupState = new GroupState(groupRebalanceConfig);
        ApiVersions apiVersions = new ApiVersions();

        Properties properties = new Properties();
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, RETRY_BACKOFF_MS);
        properties.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);

        this.config = new ConsumerConfig(properties);
        IsolationLevel isolationLevel = configuredIsolationLevel(config);
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        final long requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.metrics = createMetrics(config, time);

        this.subscriptions = spy(createSubscriptionState(config, logContext));
        this.metadata = spy(new ConsumerMetadata(config, subscriptions, logContext, new ClusterResourceListeners()));
        this.fetchConfig = createFetchConfig(config, new Deserializers<>(config));
        this.metricsManager = createFetchMetricsManager(metrics);

        this.client = new MockClient(time, metadata);
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith(1, new HashMap<String, Integer>() {
            {
                String topic1 = "test1";
                put(topic1, 1);
                String topic2 = "test2";
                put(topic2, 1);
            }
        });
        this.client.updateMetadata(metadataResponse);

        this.networkClientDelegate = spy(new NetworkClientDelegate(time,
                config,
                logContext,
                client));
        this.offsetsRequestManager = spy(new OffsetsRequestManager(subscriptions,
                metadata,
                isolationLevel,
                time,
                retryBackoffMs,
                requestTimeoutMs,
                apiVersions,
                networkClientDelegate,
                logContext));
        this.coordinatorRequestManager = spy(new CoordinatorRequestManager(time,
                logContext,
                RETRY_BACKOFF_MS,
                RETRY_BACKOFF_MAX_MS,
                errorEventHandler,
                "group_id"));
        this.commitRequestManager = spy(new CommitRequestManager(time,
                logContext,
                subscriptions,
                config,
                coordinatorRequestManager,
                groupState));
        this.fetchRequestManager = spy(new FetchRequestManager<>(logContext,
                time,
                errorEventHandler,
                metadata,
                subscriptions,
                fetchConfig,
                metricsManager,
                networkClientDelegate));
        this.requestManagers = new RequestManagers<>(logContext,
                offsetsRequestManager,
                fetchRequestManager,
                Optional.of(coordinatorRequestManager),
                Optional.of(commitRequestManager));
        this.applicationEventProcessor = spy(new ApplicationEventProcessor<>(
                backgroundEventQueue,
                requestManagers,
                metadata,
                logContext));
        this.backgroundEventProcessor = spy(new BackgroundEventProcessor(logContext, backgroundEventQueue));
    }

    @Override
    public void close() {
        requestManagers.close();
    }

    public static class DefaultBackgroundThreadTestBuilder extends ConsumerTestBuilder {

        final DefaultBackgroundThread<String, String> backgroundThread;

        public DefaultBackgroundThreadTestBuilder() {
            this.backgroundThread = new DefaultBackgroundThread<>(
                    time,
                    logContext,
                    applicationEventQueue,
                    () -> applicationEventProcessor,
                    () -> networkClientDelegate,
                    () -> requestManagers);
        }

        @Override
        public void close() {
            backgroundThread.close();
        }
    }

    public static class DefaultEventHandlerTestBuilder extends ConsumerTestBuilder {

        final EventHandler eventHandler;

        public DefaultEventHandlerTestBuilder() {
            this.eventHandler = spy(new DefaultEventHandler<>(
                    time,
                    logContext,
                    applicationEventQueue,
                    backgroundEventQueue,
                    () -> applicationEventProcessor,
                    () -> networkClientDelegate,
                    () -> requestManagers));
        }

        @Override
        public void close() {
            eventHandler.close();
        }
    }

    public static class PrototypeAsyncConsumerTestBuilder extends DefaultEventHandlerTestBuilder {

        final PrototypeAsyncConsumer<String, String> consumer;

        public PrototypeAsyncConsumerTestBuilder(Optional<String> groupIdOpt) {
            String clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
            List<ConsumerPartitionAssignor> assignors = ConsumerPartitionAssignor.getAssignorInstances(
                    config.getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG),
                    config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId))
            );
            FetchCollector<String, String> fetchCollector = new FetchCollector<>(logContext,
                    metadata,
                    subscriptions,
                    fetchConfig,
                    metricsManager,
                    time);
            this.consumer = spy(new PrototypeAsyncConsumer<>(
                    logContext,
                    clientId,
                    new Deserializers<>(new StringDeserializer(), new StringDeserializer()),
                    new FetchBuffer(logContext),
                    fetchCollector,
                    new ConsumerInterceptors<>(Collections.emptyList()),
                    time,
                    eventHandler,
                    backgroundEventQueue,
                    metrics,
                    subscriptions,
                    metadata,
                    retryBackoffMs,
                    REQUEST_TIMEOUT_MS,
                    60000,
                    assignors,
                    groupIdOpt.orElse(null)));
        }

        @Override
        public void close() {
            consumer.close();
        }
    }
}
