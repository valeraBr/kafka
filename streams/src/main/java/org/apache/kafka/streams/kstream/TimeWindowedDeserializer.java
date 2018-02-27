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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 *  The inner deserializer class can be specified by setting the property key.deserializer.inner.class,
 *  value.deserializer.inner.class or deserializer.inner.class,
 *  if the no-arg constructor is called and hence it is not passed during initialization.
 *  Note that the first two take precedence over the last.
 */
public class TimeWindowedDeserializer<T> implements Deserializer<Windowed<T>> {

    private final Long windowSize;
    
    private Deserializer<T> inner;
    
    // Default constructor needed by Kafka
    public TimeWindowedDeserializer() {
        this(null, Long.MAX_VALUE);
    }

    // TODO: fix this part as last bits of KAFKA-4468
    public TimeWindowedDeserializer(final Deserializer<T> inner) {
        this(inner, Long.MAX_VALUE);
    }

    public TimeWindowedDeserializer(final Deserializer<T> inner, final long windowSize) {
        this.inner = inner;
        this.windowSize = windowSize;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (inner == null) {
            String propertyName = isKey ? StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS : StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS;
            String value = (String) configs.get(propertyName);
            try {
                inner = Serde.class.cast(Utils.newInstance(value, Serde.class)).deserializer();
                inner.configure(configs, isKey);
            } catch (ClassNotFoundException e) {
                throw new ConfigException(propertyName, value, "Serde class " + value + " could not be found.");
            }
        }
    }

    @Override
    public Windowed<T> deserialize(String topic, byte[] data) {
        return WindowedSerdes.TimeWindowedSerde.from(data, windowSize, inner, topic);
    }

    @Override
    public void close() {
        inner.close();
    }
    
    // Only for testing
    public Deserializer<T> innerDeserializer() {
        return inner;
    }
    
    public Long getWindowSize() {
        return this.windowSize;
    }
}
