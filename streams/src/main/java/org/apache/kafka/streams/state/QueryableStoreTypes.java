/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.kafka.streams.state;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyStore;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyWindowStore;

public class QueryableStoreTypes {

    public static <K, V> QueryableStoreType<ReadOnlyKeyValueStore<K, V>> keyValueStore() {
        return new KeyValueStoreType<>();
    }

    public static <K, V> QueryableStoreType<ReadOnlyWindowStore<K, V>> windowStore() {
        return new WindowStoreType<>();
    }

    public static abstract class QueryableStoreTypeMatcher<T> implements QueryableStoreType<T> {

        private final Class matchTo;

        public QueryableStoreTypeMatcher(Class matchTo) {
            this.matchTo = matchTo;
        }
        @SuppressWarnings("unchecked")
        @Override
        public boolean accepts(final StateStore stateStore) {
            return matchTo.isAssignableFrom(stateStore.getClass());
        }
    }

    private static class KeyValueStoreType<K, V> extends
                                                 QueryableStoreTypeMatcher<ReadOnlyKeyValueStore<K, V>> {
        KeyValueStoreType() {
            super(ReadOnlyKeyValueStore.class);
        }

        @Override
        public ReadOnlyKeyValueStore<K, V> create(
            final UnderlyingStoreProvider<ReadOnlyKeyValueStore<K, V>> storeProvider,
            final String storeName) {
            return new CompositeReadOnlyStore<>(storeProvider, storeName);
        }

    }

    private static class WindowStoreType<K, V> extends QueryableStoreTypeMatcher<ReadOnlyWindowStore<K,
            V>> {
        WindowStoreType() {
            super(ReadOnlyWindowStore.class);
        }

        @Override
        public ReadOnlyWindowStore<K, V> create(
            final UnderlyingStoreProvider<ReadOnlyWindowStore<K, V>> storeProvider,
            final String storeName) {
            return new CompositeReadOnlyWindowStore<>(storeProvider, storeName);
        }

    }
}
