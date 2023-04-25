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
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.consumer.internals.Utils.CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.consumer.internals.Utils.CONSUMER_METRIC_GROUP_PREFIX;

/**
 * An {@code EventHandler} that uses a single background thread to consume {@code ApplicationEvent} and produce
 * {@code BackgroundEvent} from the {@link DefaultBackgroundThread}.
 */
public class DefaultEventHandler implements EventHandler {

    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final DefaultBackgroundThread backgroundThread;

    public DefaultEventHandler(final ConsumerConfig config,
                               final GroupRebalanceConfig groupRebalanceConfig,
                               final LogContext logContext,
                               final SubscriptionState subscriptions,
                               final ApiVersions apiVersions,
                               final Metrics metrics,
                               final ClusterResourceListeners clusterResourceListeners,
                               final Sensor fetcherThrottleTimeSensor) {
        this(Time.SYSTEM,
                config,
                logContext,
                new LinkedBlockingQueue<>(),
                new LinkedBlockingQueue<>(),
                subscriptions,
                groupRebalanceConfig,
                apiVersions,
                metrics,
                clusterResourceListeners,
                fetcherThrottleTimeSensor);
    }

    public DefaultEventHandler(final Time time,
                               final ConsumerConfig config,
                               final LogContext logContext,
                               final BlockingQueue<ApplicationEvent> applicationEventQueue,
                               final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                               final SubscriptionState subscriptions,
                               final GroupRebalanceConfig groupRebalanceConfig,
                               final ApiVersions apiVersions,
                               final Metrics metrics,
                               final ClusterResourceListeners clusterResourceListeners,
                               final Sensor fetcherThrottleTimeSensor) {
        this.applicationEventQueue = applicationEventQueue;
        this.backgroundEventQueue = backgroundEventQueue;

        // Bootstrap a metadata object with the bootstrap server IP address, which will be used once for the
        // subsequent metadata refresh once the background thread has started up.
        final ConsumerMetadata metadata = new ConsumerMetadata(config,
                subscriptions,
                logContext,
                clusterResourceListeners);
        final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
        metadata.bootstrap(addresses);

        final NetworkClient networkClient = ClientUtils.createNetworkClient(config,
                metrics,
                CONSUMER_METRIC_GROUP_PREFIX,
                logContext,
                apiVersions,
                time,
                CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION,
                metadata,
                fetcherThrottleTimeSensor);
        this.backgroundThread = new DefaultBackgroundThread(time,
                config,
                logContext,
                this.applicationEventQueue,
                this.backgroundEventQueue,
                metadata,
                networkClient,
                subscriptions,
                groupRebalanceConfig);
        this.backgroundThread.start();
    }

    // VisibleForTesting
    DefaultEventHandler(final DefaultBackgroundThread backgroundThread,
                        final BlockingQueue<ApplicationEvent> applicationEventQueue,
                        final BlockingQueue<BackgroundEvent> backgroundEventQueue) {
        this.backgroundThread = backgroundThread;
        this.applicationEventQueue = applicationEventQueue;
        this.backgroundEventQueue = backgroundEventQueue;
        backgroundThread.start();
    }

    @Override
    public Optional<BackgroundEvent> poll() {
        return Optional.ofNullable(backgroundEventQueue.poll());
    }

    @Override
    public boolean isEmpty() {
        return backgroundEventQueue.isEmpty();
    }

    @Override
    public boolean add(final ApplicationEvent event) {
        backgroundThread.wakeup();
        return applicationEventQueue.add(event);
    }

    public void close() {
        try {
            backgroundThread.close();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
}
