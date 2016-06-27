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
package org.apache.kafka.streams;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.ReadOnlyStoresProvider;
import org.apache.kafka.streams.state.UnderlyingStoreProvider;

import java.util.ArrayList;
import java.util.List;

/**
 * A wrapper over all of the {@link ReadOnlyStoresProvider}s in a Topology
 */
class QueryableStoreProvider {

    private final List<ReadOnlyStoresProvider> storeProviders;

    QueryableStoreProvider(final List<ReadOnlyStoresProvider> storeProviders) {
        this.storeProviders = new ArrayList<>(storeProviders);
    }

    /**
     * Get a composite object wrapping the instances of the {@link StateStore} with the provided
     * storeName and {@link QueryableStoreType}
     * @param storeName             name of the store
     * @param queryableStoreType    accept stores passing {@link QueryableStoreType#accepts(StateStore)}
     * @param <T>                   The expected type of the returned store
     * @return  A composite object that wraps the store instances.
     */
    public <T> T getStore(final String storeName, final QueryableStoreType<T> queryableStoreType) {
        final List<T> allStores = new ArrayList<>();
        for (ReadOnlyStoresProvider storeProvider : storeProviders) {
            allStores.addAll(storeProvider.getStores(storeName, queryableStoreType));
        }
        if (allStores.isEmpty()) {
            return null;
        }
        return queryableStoreType.create(new UnderlyingStoreProvider<>(storeProviders,
                                                                       queryableStoreType), storeName);
    }
}
