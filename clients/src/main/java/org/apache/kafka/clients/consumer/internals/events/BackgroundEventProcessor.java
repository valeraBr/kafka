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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.DefaultBackgroundThread;
import org.apache.kafka.common.utils.LogContext;

import java.util.concurrent.BlockingQueue;

/**
 * An {@link EventProcessor} that is created and executes in the application thread for the purpose of processing
 * {@link BackgroundEvent background events} generated by  {@link DefaultBackgroundThread the background thread}.
 * Those events are generally of two types:
 *
 * <ul>
 *     <li>Errors that occur in the background thread that need to be propagated to the application thread</li>
 *     <li>{@link ConsumerRebalanceListener} callbacks that are to be executed on the application thread</li>
 * </ul>
 */
public class BackgroundEventProcessor extends EventProcessor<BackgroundEvent> {

    public BackgroundEventProcessor(final LogContext logContext,
                                    final BlockingQueue<BackgroundEvent> backgroundEventQueue) {
        super(logContext, backgroundEventQueue);
    }

    @Override
    protected Class<BackgroundEvent> getEventClass() {
        return BackgroundEvent.class;
    }

    @Override
    public void process(final BackgroundEvent event) {
        if (event.type() == BackgroundEvent.Type.ERROR)
            process((ErrorBackgroundEvent) event);
        else
            throw new IllegalArgumentException("Background event type " + event.type() + " was not expected");
    }

    private void process(final ErrorBackgroundEvent event) {
        throw event.error();
    }
}
