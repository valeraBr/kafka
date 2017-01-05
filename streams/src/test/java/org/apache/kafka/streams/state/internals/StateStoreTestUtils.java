/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;

import java.util.Collections;

@SuppressWarnings("unchecked")
public class StateStoreTestUtils {

    public static <K, V> KeyValueStore<K, V> newKeyValueStore(String name, Class<K> keyType, Class<V> valueType) {
        final InMemoryKeyValueStoreSupplier<K, V> supplier = new InMemoryKeyValueStoreSupplier<>(name,
                                                                                                 null,
                                                                                                 null,
                                                                                                 new MockTime(),
                                                                                                 false,
                                                                                                 Collections.<String, Object>emptyMap());

        final StateStore stateStore = supplier.get();
        stateStore.init(new MockProcessorContext(StateSerdes.withBuiltinTypes(name, keyType, valueType),
                new NoOpRecordCollector()), stateStore);
        return (KeyValueStore<K, V>) stateStore;

    }

    static class NoOpReadOnlyStore<K, V>
            implements ReadOnlyKeyValueStore<K, V>, StateStore {

        @Override
        public V get(final K key) {
            return null;
        }

        @Override
        public KeyValueIterator<K, V> range(final K from, final K to) {
            return null;
        }

        @Override
        public KeyValueIterator<K, V> all() {
            return null;
        }

        @Override
        public long approximateNumEntries() {
            return 0L;
        }

        @Override
        public String name() {
            return "";
        }

        @Override
        public void init(final ProcessorContext context, final StateStore root) {

        }

        @Override
        public void flush() {

        }

        @Override
        public void close() {

        }

        @Override
        public boolean persistent() {
            return false;
        }

        @Override
        public boolean isOpen() {
            return false;
        }

    }
}
