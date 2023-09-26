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

import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.ErrorBackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.EventProcessor;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ErrorEventHandlerTest {

    private ConsumerTestBuilder.DefaultEventHandlerTestBuilder testBuilder;
    private BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private ErrorEventHandler errorEventHandler;
    private BackgroundEventProcessor backgroundEventProcessor;

    @BeforeEach
    public void setup() {
        testBuilder = new ConsumerTestBuilder.DefaultEventHandlerTestBuilder();
        backgroundEventQueue = testBuilder.backgroundEventQueue;
        errorEventHandler = new ErrorEventHandler(backgroundEventQueue);
        backgroundEventProcessor = new BackgroundEventProcessor(testBuilder.logContext, backgroundEventQueue);
    }

    @AfterEach
    public void tearDown() {
        if (testBuilder != null)
            testBuilder.close();
    }

    @Test
    public void testNoEvents() {
        assertTrue(backgroundEventQueue.isEmpty());
        backgroundEventProcessor.process(ignored -> { });
        assertTrue(backgroundEventQueue.isEmpty());
    }

    @Test
    public void testSingleEvent() {
        BackgroundEvent event = new ErrorBackgroundEvent(new RuntimeException("A"));
        backgroundEventQueue.add(event);
        assertPeeked(event);
        backgroundEventProcessor.process(ignored -> { });
        assertTrue(backgroundEventQueue.isEmpty());
    }

    @Test
    public void testSingleErrorEvent() {
        KafkaException error = new KafkaException("error");
        BackgroundEvent event = new ErrorBackgroundEvent(error);
        errorEventHandler.handle(error);
        assertPeeked(event);
        assertProcessThrows(error);
    }

    @Test
    public void testMultipleEvents() {
        BackgroundEvent event1 = new ErrorBackgroundEvent(new RuntimeException("A"));
        backgroundEventQueue.add(event1);
        backgroundEventQueue.add(new ErrorBackgroundEvent(new RuntimeException("B")));
        backgroundEventQueue.add(new ErrorBackgroundEvent(new RuntimeException("C")));

        assertPeeked(event1);
        backgroundEventProcessor.process(ignored -> { });
        assertTrue(backgroundEventQueue.isEmpty());
    }

    @Test
    public void testMultipleErrorEvents() {
        Throwable error1 = new Throwable("error1");
        KafkaException error2 = new KafkaException("error2");
        KafkaException error3 = new KafkaException("error3");

        errorEventHandler.handle(error1);
        errorEventHandler.handle(error2);
        errorEventHandler.handle(error3);

        assertProcessThrows(new KafkaException(error1));
    }

    @Test
    public void testMixedEventsWithErrorEvents() {
        Throwable error1 = new Throwable("error1");
        KafkaException error2 = new KafkaException("error2");
        KafkaException error3 = new KafkaException("error3");

        RuntimeException errorToCheck = new RuntimeException("A");
        backgroundEventQueue.add(new ErrorBackgroundEvent(errorToCheck));
        errorEventHandler.handle(error1);
        backgroundEventQueue.add(new ErrorBackgroundEvent(new RuntimeException("B")));
        errorEventHandler.handle(error2);
        backgroundEventQueue.add(new ErrorBackgroundEvent(new RuntimeException("C")));
        errorEventHandler.handle(error3);
        backgroundEventQueue.add(new ErrorBackgroundEvent(new RuntimeException("D")));

        assertProcessThrows(new KafkaException(errorToCheck));
    }

    private void assertPeeked(BackgroundEvent event) {
        BackgroundEvent peekEvent = backgroundEventQueue.peek();
        assertNotNull(peekEvent);
        assertEquals(event, peekEvent);
    }

    private void assertProcessThrows(Throwable error) {
        assertFalse(backgroundEventQueue.isEmpty());

        try {
            TestProcessHandler processHandler = new TestProcessHandler();
            backgroundEventProcessor.process(processHandler);
            processHandler.maybeThrow();
            fail("Should have thrown error: " + error);
        } catch (Throwable t) {
            assertEquals(error.getClass(), t.getClass());
            assertEquals(error.getMessage(), t.getMessage());
        }

        assertTrue(backgroundEventQueue.isEmpty());
    }

    private static class TestProcessHandler implements EventProcessor.ProcessErrorHandler {

        private KafkaException first;

        @Override
        public void onProcessingError(KafkaException error) {
            if (first == null)
                first = error;
        }

        void maybeThrow() {
            if (first != null)
                throw first;
        }
    }
}
