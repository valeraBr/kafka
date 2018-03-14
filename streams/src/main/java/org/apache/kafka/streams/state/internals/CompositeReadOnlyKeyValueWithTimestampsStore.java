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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueWithTimestampStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.List;
import java.util.Objects;

/**
 * A wrapper over the underlying {@link ReadOnlyKeyValueStore}s found in a {@link
 * org.apache.kafka.streams.processor.internals.ProcessorTopology}
 *
 * @param <K> key type
 * @param <V> value type
 */
public class CompositeReadOnlyKeyValueWithTimestampsStore<K, V> implements ReadOnlyKeyValueWithTimestampStore<K, V> {

    private final StateStoreProvider storeProvider;
    private final QueryableStoreType<ReadOnlyKeyValueWithTimestampStore<K, V>> storeType;
    private final String storeName;

    public CompositeReadOnlyKeyValueWithTimestampsStore(final StateStoreProvider storeProvider,
                                                        final QueryableStoreType<ReadOnlyKeyValueWithTimestampStore<K, V>> storeType,
                                                        final String storeName) {
        this.storeProvider = storeProvider;
        this.storeType = storeType;
        this.storeName = storeName;
    }


    @Override
    public ValueAndTimestamp<V> get(final K key) {
        Objects.requireNonNull(key);
        final List<ReadOnlyKeyValueWithTimestampStore<K, V>> stores = storeProvider.stores(storeName, storeType);
        for (final ReadOnlyKeyValueWithTimestampStore<K, V> store : stores) {
            try {
                final ValueAndTimestamp<V> result = store.get(key);
                if (result != null) {
                    return result;
                }
            } catch (final InvalidStateStoreException e) {
                throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
            }

        }
        return null;
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> range(final K from,
                                                           final K to) {
        Objects.requireNonNull(from);
        Objects.requireNonNull(to);
        final NextIteratorFunction<K, ValueAndTimestamp<V>, ReadOnlyKeyValueWithTimestampStore<K, V>> nextIteratorFunction = store -> {
            try {
                return store.range(from, to);
            } catch (final InvalidStateStoreException e) {
                throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
            }
        };
        final List<ReadOnlyKeyValueWithTimestampStore<K, V>> stores = storeProvider.stores(storeName, storeType);
        return new DelegatingPeekingKeyValueIterator<>(storeName, new CompositeKeyValueIterator<>(stores.iterator(), nextIteratorFunction));
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> all() {
        final NextIteratorFunction<K, ValueAndTimestamp<V>, ReadOnlyKeyValueWithTimestampStore<K, V>> nextIteratorFunction = store -> {
            try {
                return store.all();
            } catch (final InvalidStateStoreException e) {
                throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
            }
        };
        final List<ReadOnlyKeyValueWithTimestampStore<K, V>> stores = storeProvider.stores(storeName, storeType);
        return new DelegatingPeekingKeyValueIterator<>(storeName, new CompositeKeyValueIterator<>(stores.iterator(), nextIteratorFunction));
    }

    @Override
    public long approximateNumEntries() {
        final List<ReadOnlyKeyValueWithTimestampStore<K, V>> stores = storeProvider.stores(storeName, storeType);
        long total = 0;
        for (final ReadOnlyKeyValueWithTimestampStore<K, V> store : stores) {
            total += store.approximateNumEntries();
            if (total < 0) {
                return Long.MAX_VALUE;
            }
        }
        return total;
    }


}

