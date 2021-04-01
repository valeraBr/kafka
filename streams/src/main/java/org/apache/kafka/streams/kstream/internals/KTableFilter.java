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

import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import static org.apache.kafka.streams.state.ValueAndTimestamp.getValueOrNull;

class KTableFilter<K, V> implements KTableChangeProcessorSupplier<K, V, V, K, V> {
    private final KTableImpl<K, ?, V> parent;
    private final Predicate<? super K, ? super V> predicate;
    private final boolean filterNot;
    private final String queryableName;
    private boolean sendOldValues;

    KTableFilter(final KTableImpl<K, ?, V> parent,
                 final Predicate<? super K, ? super V> predicate,
                 final boolean filterNot,
                 final String queryableName) {
        this.parent = parent;
        this.predicate = predicate;
        this.filterNot = filterNot;
        this.queryableName = queryableName;
        // If upstream is already materialized, enable sending old values to avoid sending unnecessary tombstones:
        this.sendOldValues = parent.enableSendingOldValues(false);
    }

    @Override
    public Processor<K, Change<V>, K, Change<V>> get() {
        return new KTableFilterProcessor();
    }

    @Override
    public boolean enableSendingOldValues(final boolean forceMaterialization) {
        if (queryableName != null) {
            sendOldValues = true;
            return true;
        }

        if (parent.enableSendingOldValues(forceMaterialization)) {
            sendOldValues = true;
        }
        return sendOldValues;
    }

    private V computeValue(final K key, final V value) {
        V newValue = null;

        if (value != null && (filterNot ^ predicate.test(key, value))) {
            newValue = value;
        }

        return newValue;
    }

    private ValueAndTimestamp<V> computeValue(final K key, final ValueAndTimestamp<V> valueAndTimestamp) {
        ValueAndTimestamp<V> newValueAndTimestamp = null;

        if (valueAndTimestamp != null) {
            final V value = valueAndTimestamp.value();
            if (filterNot ^ predicate.test(key, value)) {
                newValueAndTimestamp = valueAndTimestamp;
            }
        }

        return newValueAndTimestamp;
    }


    private class KTableFilterProcessor extends ContextualProcessor<K, Change<V>, K, Change<V>> {
        private TimestampedKeyValueStore<K, V> store;
        private TupleChangeForwarder<K, V> tupleForwarder;

        @Override
        public void init(final ProcessorContext<K, Change<V>> context) {
            super.init(context);
            if (queryableName != null) {
                store = context.getStateStore(queryableName);
                tupleForwarder = new TupleChangeForwarder<>(
                    store,
                    context,
                    new TupleChangeCacheFlushListener<>(context),
                    sendOldValues);
            }
        }

        @Override
        public void process(final Record<K, Change<V>> record) {
            final V newValue = computeValue(record.key(), record.value().newValue);
            final V oldValue = computeOldValue(record.key(), record.value());

            if (sendOldValues && oldValue == null && newValue == null) {
                return; // unnecessary to forward here.
            }

            if (queryableName != null) {
                store.put(record.key(), ValueAndTimestamp.make(newValue, record.timestamp()));
                tupleForwarder.maybeForward(record, newValue, oldValue);
            } else {
                context().forward(record.withValue(new Change<>(newValue, oldValue)));
            }
        }

        private V computeOldValue(final K key, final Change<V> change) {
            if (!sendOldValues) {
                return null;
            }

            return queryableName != null
                ? getValueOrNull(store.get(key))
                : computeValue(key, change.oldValue);
        }
    }

    @Override
    public KTableValueAndTimestampGetterSupplier<K, V> view() {
        // if the KTable is materialized, use the materialized store to return getter value;
        // otherwise rely on the parent getter and apply filter on-the-fly
        if (queryableName != null) {
            return new KTableMaterializedValueAndTimestampGetterSupplier<>(queryableName);
        } else {
            return new KTableValueAndTimestampGetterSupplier<K, V>() {
                final KTableValueAndTimestampGetterSupplier<K, V> parentValueGetterSupplier =
                    parent.valueAndTimestampGetterSupplier();

                public KTableValueAndTimestampGetter<K, V> get() {
                    return new KTableFilterValueGetter(parentValueGetterSupplier.get());
                }

                @Override
                public String[] storeNames() {
                    return parentValueGetterSupplier.storeNames();
                }
            };
        }
    }


    private class KTableFilterValueGetter implements KTableValueAndTimestampGetter<K, V> {
        private final KTableValueAndTimestampGetter<K, V> parentGetter;

        KTableFilterValueGetter(final KTableValueAndTimestampGetter<K, V> parentGetter) {
            this.parentGetter = parentGetter;
        }

        @Override
        public <KParent, VParent> void init(ProcessorContext<KParent, VParent> context) {
            parentGetter.init(context);
        }

        @Override
        public ValueAndTimestamp<V> get(final K key) {
            return computeValue(key, parentGetter.get(key));
        }

        @Override
        public void close() {
            parentGetter.close();
        }
    }

}
