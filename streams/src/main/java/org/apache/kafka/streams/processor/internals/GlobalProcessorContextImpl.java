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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.time.Duration;
import java.util.List;

public class GlobalProcessorContextImpl extends AbstractProcessorContext {

    private final static To SEND_TO_ALL = To.all();
    private final ToInternal toInternal = new ToInternal();

    public GlobalProcessorContextImpl(final StreamsConfig config,
                                      final StateManager stateMgr,
                                      final StreamsMetricsImpl metrics,
                                      final ThreadCache cache) {
        super(new TaskId(-1, -1), config, metrics, stateMgr, cache);
    }

    @Override
    public StateStore getStateStore(final String name) {
        return stateManager.getGlobalStore(name);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(final K key, final V value) {
        forward(key, value, SEND_TO_ALL);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(final K key, final V value, final To to) {
        toInternal.update(to);
        if (toInternal.hasTimestamp()) {
            recordContext.setTimestamp(toInternal.timestamp());
        }
        final ProcessorNode previousNode = currentNode();
        try {
            final List<ProcessorNode<K, V>> children = (List<ProcessorNode<K, V>>) currentNode().children();
            final String sendTo = toInternal.child();
            if (sendTo != null) {
                final ProcessorNode child = currentNode().getChild(sendTo);
                if (child == null) {
                    throw new StreamsException("Unknown downstream node: " + sendTo + " either does not exist or is not" +
                        " connected to this processor.");
                }
                forward(child, key, value);
            } else {
                if (children.size() == 1) {
                    final ProcessorNode child = children.get(0);
                    forward(child, key, value);
                } else {
                    for (final ProcessorNode child : children) {
                        forward(child, key, value);
                    }
                }
            }
        } finally {
            setCurrentNode(previousNode);
        }
    }

    @SuppressWarnings("unchecked")
    private <K, V> void forward(final ProcessorNode child,
                                final K key,
                                final V value) {
        setCurrentNode(child);
        child.process(key, value);
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @SuppressWarnings("deprecation")
    @Override
    public <K, V> void forward(final K key, final V value, final int childIndex) {
        throw new UnsupportedOperationException("this should not happen: forward() not supported in global processor context.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @SuppressWarnings("deprecation")
    @Override
    public <K, V> void forward(final K key, final V value, final String childName) {
        throw new UnsupportedOperationException("this should not happen: forward() not supported in global processor context.");
    }

    @Override
    public void commit() {
        //no-op
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    @Deprecated
    public Cancellable schedule(final long interval, final PunctuationType type, final Punctuator callback) {
        throw new UnsupportedOperationException("this should not happen: schedule() not supported in global processor context.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public Cancellable schedule(final Duration interval, final PunctuationType type, final Punctuator callback) {
        throw new UnsupportedOperationException("this should not happen: schedule() not supported in global processor context.");
    }

    /**
     * @throws UnsupportedOperationException on every invocation
     */
    @Override
    public long streamTime() {
        throw new UnsupportedOperationException("Stream-time is not defined for global tasks.");
    }
}