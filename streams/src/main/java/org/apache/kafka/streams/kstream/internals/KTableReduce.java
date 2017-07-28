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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.ReducerWithKey;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class KTableReduce<K, V> implements KTableProcessorSupplier<K, V, V> {

    private final String storeName;
    private final ReducerWithKey<K, V> addReducerWithKey;
    private final ReducerWithKey<K, V> removeReducerWithKey;

    private boolean sendOldValues = false;

    public KTableReduce(String storeName, ReducerWithKey<K, V> addReducerWithKey, ReducerWithKey<K, V> removeReducerWithKey) {
        this.storeName = storeName;
        this.addReducerWithKey = addReducerWithKey;
        this.removeReducerWithKey = removeReducerWithKey;
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }

    @Override
    public Processor<K, Change<V>> get() {
        return new KTableReduceProcessor();
    }

    private class KTableReduceProcessor extends AbstractProcessor<K, Change<V>> {

        private KeyValueStore<K, V> store;
        private TupleForwarder<K, V> tupleForwarder;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            store = (KeyValueStore<K, V>) context.getStateStore(storeName);
            tupleForwarder = new TupleForwarder<K, V>(store, context, new ForwardingCacheFlushListener<K, V>(context, sendOldValues), sendOldValues);
        }

        /**
         * @throws StreamsException if key is null
         */
        @Override
        public void process(K key, Change<V> value) {
            // the keys should never be null
            if (key == null)
                throw new StreamsException("Record key for KTable reduce operator with state " + storeName + " should not be null.");

            V oldAgg = store.get(key);
            V newAgg = oldAgg;

            // first try to add the new new value
            if (value.newValue != null) {
                if (newAgg == null) {
                    newAgg = value.newValue;
                } else {
                    newAgg = addReducerWithKey.apply(key, newAgg, value.newValue);
                }
            }

            // then try to remove the old value
            if (value.oldValue != null) {
                newAgg = removeReducerWithKey.apply(key, newAgg, value.oldValue);
            }

            // update the store with the new value
            store.put(key, newAgg);
            tupleForwarder.maybeForward(key, newAgg, oldAgg);
        }
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {

        return new KTableValueGetterSupplier<K, V>() {

            public KTableValueGetter<K, V> get() {
                return new KTableAggregateValueGetter();
            }

            @Override
            public String[] storeNames() {
                return new String[]{storeName};
            }
        };
    }

    private class KTableAggregateValueGetter implements KTableValueGetter<K, V> {

        private KeyValueStore<K, V> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            store = (KeyValueStore<K, V>) context.getStateStore(storeName);
        }

        @Override
        public V get(K key) {
            return store.get(key);
        }

    }
}