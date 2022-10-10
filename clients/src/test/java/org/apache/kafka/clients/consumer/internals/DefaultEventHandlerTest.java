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

import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.clients.consumer.internals.events.NoopApplicationEvent;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.Closeable;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultEventHandlerTest {
    private final Properties properties = new Properties();

    @BeforeEach
    public void setup() {
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(RETRY_BACKOFF_MS_CONFIG, "100");
    }

    @Test
    @Timeout(1)
    public void testBasicPollAndAddWithNoopEvent() {
        Time time = new MockTime(1);
        LogContext logContext = new LogContext();
        SubscriptionState subscriptions = new SubscriptionState(new LogContext(), OffsetResetStrategy.NONE);
        ConsumerMetadata metadata = newConsumerMetadata(false, subscriptions);
        MockClient client = new MockClient(time, metadata);
        ConsumerNetworkClient consumerClient = new ConsumerNetworkClient(logContext, client, metadata, time,
                100, 1000, 100);
        BlockingQueue<ApplicationEvent> aq = new LinkedBlockingQueue<>();
        BlockingQueue<BackgroundEvent> bq = new LinkedBlockingQueue<>();
        DefaultEventHandler handler = new DefaultEventHandler(
                time,
                new ConsumerConfig(properties),
                logContext,
                aq,
                bq,
                subscriptions,
                metadata,
                consumerClient);
        assertTrue(client.active());
        assertTrue(handler.isEmpty());
        handler.add(new NoopApplicationEvent("testBasicPollAndAddWithNoopEvent"));
        while (handler.isEmpty()) {
            time.sleep(100);
        }
        assertTrue(handler.poll().get() instanceof NoopBackgroundEvent);
        assertFalse(client.hasInFlightRequests()); // noop does not send network request
        handler.close();
        assertFalse(client.active());
    }

    @Test
    @Timeout(1)
    public void testRunOnceBackgroundThread() {
        BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
        BlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();
        KafkaThread backgroundThread =
                new KafkaThread("", new RunOnceBackgroundThreadRunnable(applicationEventQueue, backgroundEventQueue), true);
        EventHandler eventHandler = new DefaultEventHandler(
                backgroundThread,
                applicationEventQueue,
                backgroundEventQueue);
        assertTrue(eventHandler.add(new NoopApplicationEvent("hello-world")));
        while (eventHandler.isEmpty()) { }
        Optional<BackgroundEvent> event = eventHandler.poll();
        assertTrue(event.isPresent());
        assertTrue(event.get() instanceof NoopBackgroundEvent);
        assertEquals(BackgroundEvent.EventType.NOOP, event.get().type);
        assertEquals("hello-world", ((NoopBackgroundEvent) event.get()).message);
    }

    private static ConsumerMetadata newConsumerMetadata(boolean includeInternalTopics, SubscriptionState subscriptions) {
        long refreshBackoffMs = 50;
        long expireMs = 50000;
        return new ConsumerMetadata(refreshBackoffMs, expireMs, includeInternalTopics, false,
                subscriptions, new LogContext(), new ClusterResourceListeners());
    }

    private class RunOnceBackgroundThreadRunnable implements Runnable, Closeable {
        private BlockingQueue<ApplicationEvent> applicationEventQueue;
        private BlockingQueue<BackgroundEvent> backgroundEventQueue;

        public RunOnceBackgroundThreadRunnable(BlockingQueue<ApplicationEvent> applicationEvents,
                                               BlockingQueue<BackgroundEvent> backgroundEventQueue) {
            this.applicationEventQueue = applicationEvents;
            this.backgroundEventQueue = backgroundEventQueue;
        }

        @Override
        public void run() {
            while (applicationEventQueue.isEmpty()) { }
            ApplicationEvent event = applicationEventQueue.poll();
            String message = ((NoopApplicationEvent) event).message;
            backgroundEventQueue.add(new NoopBackgroundEvent(message));
        }

        @Override
        public void close() {
        }
    }
}
