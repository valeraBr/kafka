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
package org.apache.kafka.test;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.easymock.Capture;

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;

public class InternalProcessorContextMock {

    public static Builder builder() {
        return new Builder(processorContext());
    }

    public static class Builder {

        private InternalProcessorContext mock;
        private ProcessorContext processorContext;

        private String applicationId;
        private TaskId taskId;
        private Serde<?> keySerde;
        private Serde<?> valueSerde;
        private File stateDir;
        private StreamsMetricsImpl metrics;
        private final Map<String, StateStore> stateStoreMap;
        private final Map<String, StateRestoreCallback> stateRestoreCallbackMap;

        Builder(final ProcessorContext processorContext) {
            mock = mock(InternalProcessorContext.class);
            this.processorContext = processorContext;

            stateStoreMap = new HashMap<>();
            stateRestoreCallbackMap = new HashMap<>();

            applicationId = processorContext.applicationId();
            taskId = processorContext.taskId();
            keySerde = processorContext.keySerde();
            valueSerde = processorContext.valueSerde();
            stateDir = processorContext.stateDir();
            metrics = (StreamsMetricsImpl) processorContext.metrics();
        }

        public InternalProcessorContext build() {
            applicationId();
            taskId();
            keySerde();
            valueSerde();
            stateDir();
            metrics();
            register();
            getStateStore();
            schedule();

            replay(mock);
            return mock;
        }

        private void schedule() {
            final Capture<Duration> interval = Capture.newInstance();
            final Capture<PunctuationType> type = Capture.newInstance();
            final Capture<Punctuator> punctuator = Capture.newInstance();
            expect(mock.schedule(capture(interval), capture(type), capture(punctuator)))
                .andAnswer(() -> processorContext.schedule(interval.getValue(), type.getValue(), punctuator.getValue()))
                .anyTimes();
        }

        private void getStateStore() {
            final Capture<String> stateStoreNameCapture = Capture.newInstance();
            expect(mock.getStateStore(capture(stateStoreNameCapture)))
                    .andAnswer(() -> stateStoreMap.get(stateStoreNameCapture.getValue())).anyTimes();
        }

        private void register() {
            final Capture<StateStore> storeCapture = Capture.newInstance();
            final Capture<StateRestoreCallback> restoreCallbackCapture = Capture.newInstance();

            mock.register(capture(storeCapture), capture(restoreCallbackCapture));

            expectLastCall()
                    .andAnswer(() -> {
                        stateStoreMap.put(storeCapture.getValue().name(), storeCapture.getValue());
                        stateRestoreCallbackMap.put(storeCapture.getValue().name(), restoreCallbackCapture.getValue());
                        return null;
                    }).anyTimes();
        }

        private void metrics() {
            expect(mock.metrics()).andReturn(metrics).anyTimes();
        }

        private void stateDir() {
            expect(mock.stateDir()).andReturn(stateDir).anyTimes();
        }

        private void valueSerde() {
            expect((Serde) mock.valueSerde()).andReturn(valueSerde).anyTimes();
        }

        private void keySerde() {
            expect((Serde) mock.keySerde()).andReturn(keySerde).anyTimes();
        }

        private void taskId() {
            expect(mock.taskId()).andReturn(taskId).anyTimes();
        }

        private void applicationId() {
            expect(mock.applicationId()).andReturn(applicationId).anyTimes();
        }
    }

    private static ProcessorContext processorContext() {
        return new MockProcessorContext();
    }
}
