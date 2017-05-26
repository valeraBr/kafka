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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.test.MockProcessorContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.Options;

public class RocksDBStoreTest {
    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();

    private RocksDBStore<String, String> subject;

    @Before
    public void setUp() throws Exception {
        subject = new RocksDBStore<>("test", Serdes.String(), Serdes.String());
    }

    @After
    public void tearDown() throws Exception {
        subject.close();
    }

    @Test
    public void testClass() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, MockRocksDbConfigSetter.class);
        subject.openDB(new ConfigurableProcessorContext(tempDir.newFolder(), Serdes.String(), Serdes.String(),
                null, null, configs));

        assertTrue(MockRocksDbConfigSetter.called);
    }

    @Test
    public void testString() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, MockRocksDbConfigSetter.class.getName());
        subject.openDB(new ConfigurableProcessorContext(tempDir.newFolder(), Serdes.String(), Serdes.String(),
                null, null, configs));

        assertTrue(MockRocksDbConfigSetter.called);
    }

    public static class MockRocksDbConfigSetter implements RocksDBConfigSetter {
        static boolean called;

        public MockRocksDbConfigSetter() {
            called = false;
        }

        @Override
        public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
            called = true;
        }
    }


    private static class ConfigurableProcessorContext extends MockProcessorContext {
        Map<String, Object> configs;

        ConfigurableProcessorContext(File stateDir, Serde<?> keySerde, Serde<?> valSerde, RecordCollector collector,
                                     ThreadCache cache, Map<String, Object> configs) {
            super(stateDir, keySerde, valSerde, collector, cache);
            this.configs = configs;
        }

        @Override
        public Map<String, Object> appConfigs() {
            return configs;
        }
    }
}
