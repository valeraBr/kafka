/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.streams.kstream.AggregatorSupplier;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.ValueMapper;

public class SumSupplier<K, V> implements AggregatorSupplier<K, V, Long> {

    private final ValueMapper<V, Long> valueMapper;

    public SumSupplier(ValueMapper<V, Long> valueMapper) {
        this.valueMapper = valueMapper;
    }

    private class LongSum implements Aggregator<K, V, Long> {

        @Override
        public Long initialValue(){
            return 0L;
        }

        @Override
        public Long add(K aggKey, V value, Long aggregate) {
            return aggregate + valueMapper.apply(value);
        }

        @Override
        public Long remove(K aggKey, V value, Long aggregate) {
            return aggregate - valueMapper.apply(value);
        }

        @Override
        public Long merge(Long aggr1, Long aggr2) {
            return aggr1 + aggr2;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Aggregator<K, V, Long> get() {
        return new LongSum();
    }
}