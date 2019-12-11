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

import static java.time.Duration.ofMillis;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TimeWindowedCogroupedKStream;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

public class TimeWindowedCogroupedKStreamImplTest {

    private final MockProcessorSupplier<Windowed<String>, String> processorSupplier = new MockProcessorSupplier<>();
    private static final String TOPIC = "topic";
    private static final String TOPIC2 = "topic2";
    private static final String OUTPUT = "output";
    private final StreamsBuilder builder = new StreamsBuilder();
    private KGroupedStream<String, String> groupedStream;

    private KGroupedStream<String, String> groupedStream2;
    private CogroupedKStream<String, String> cogroupedStream;
    private TimeWindowedCogroupedKStream<String, String> windowedCogroupedStream;

    private final Properties props = StreamsTestUtils
        .getStreamsConfig(Serdes.String(), Serdes.String());

    @Before
    public void setup() {
        final KStream<String, String> stream = builder.stream(TOPIC, Consumed
            .with(Serdes.String(), Serdes.String()));
        final KStream<String, String> stream2 = builder.stream(TOPIC2, Consumed
            .with(Serdes.String(), Serdes.String()));

        groupedStream = stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        groupedStream2 = stream2.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        cogroupedStream = groupedStream.cogroup(MockAggregator.TOSTRING_ADDER)
                .cogroup(groupedStream2, MockAggregator.TOSTRING_REMOVER);
        windowedCogroupedStream = cogroupedStream.windowedBy(TimeWindows.of(ofMillis(500L)));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerOnAggregate() {
        windowedCogroupedStream.aggregate(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullMaterializedOnTwoOptionAggregate() {
        windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT, (Materialized<String, String, WindowStore<Bytes, byte[]>>) null);
    }
    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullNamedTwoOptionOnAggregate() {
        windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT, (Named) null);
    }
    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerTwoOptionNamedOnAggregate() {
        windowedCogroupedStream.aggregate(null, Named.as("test"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerTwoOptionMaterializedOnAggregate() {
        windowedCogroupedStream.aggregate(null, Materialized.as("test"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullInitializerThreeOptionOnAggregate() {
        windowedCogroupedStream.aggregate(null, Named.as("test"), Materialized.as("test"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullMaterializedOnAggregate() {
        windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT, Named.as("Test"), null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotHaveNullNamedOnAggregate() {
        windowedCogroupedStream.aggregate(MockInitializer.STRING_INIT, null, Materialized.as("test"));
    }

    @Test
    public void timeWindowAggregateTest() {
        final KTable<Windowed<String>, String> customers = groupedStream.cogroup(MockAggregator.TOSTRING_ADDER)
                .windowedBy(TimeWindows.of(ofMillis(500L))).aggregate(
                        MockInitializer.STRING_INIT, Materialized.with(Serdes.String(), Serdes.String()));
        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(
                    TOPIC, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<Windowed<String>, String> testOutputTopic = driver.createOutputTopic(
                    OUTPUT, new TimeWindowedDeserializer<>(new StringDeserializer()), new StringDeserializer());
            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k2", "A", 0);
            testInputTopic.pipeInput("k2", "A", 1);
            testInputTopic.pipeInput("k1", "A", 2);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A+A", 1);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A+A", 2);
        }

    }

    @Test
    public void timeWindowAggregateTest2StreamsTest() {

        final KTable<Windowed<String>, String> customers = windowedCogroupedStream.aggregate(
                MockInitializer.STRING_INIT, Materialized.with(Serdes.String(), Serdes.String()));
        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(
                    TOPIC, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<Windowed<String>, String> testOutputTopic = driver.createOutputTopic(
                    OUTPUT, new TimeWindowedDeserializer<>(new StringDeserializer()), new StringDeserializer());

            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k2", "A", 0);
            testInputTopic.pipeInput("k2", "A", 1);
            testInputTopic.pipeInput("k1", "A", 2);
            testInputTopic.pipeInput("k1", "B", 3);
            testInputTopic.pipeInput("k2", "B", 3);
            testInputTopic.pipeInput("k2", "B", 4);
            testInputTopic.pipeInput("k1", "B", 4);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A+A", 1);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A+A", 2);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A+A+B", 3);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A+A+B", 3);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A+A+B+B", 4);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A+A+B+B", 4);
        }

    }

    @Test
    public void timeWindowMixAggregatorsTest() {

        final KTable<Windowed<String>, String> customers = windowedCogroupedStream.aggregate(
                MockInitializer.STRING_INIT, Materialized.with(Serdes.String(), Serdes.String()));
        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(
                    TOPIC, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic2 = driver.createInputTopic(
                    TOPIC2, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<Windowed<String>, String> testOutputTopic = driver.createOutputTopic(
                    OUTPUT, new TimeWindowedDeserializer<>(new StringDeserializer()), new StringDeserializer());

            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k2", "A", 0);
            testInputTopic.pipeInput("k2", "A", 1);
            testInputTopic.pipeInput("k1", "A", 2);
            testInputTopic2.pipeInput("k1", "B", 3);
            testInputTopic2.pipeInput("k2", "B", 3);
            testInputTopic2.pipeInput("k2", "B", 4);
            testInputTopic2.pipeInput("k1", "B", 4);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A+A", 1);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A+A", 2);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A+A-B", 3);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A+A-B", 3);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A+A-B-B", 4);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A+A-B-B", 4);
        }

    }

    @Test
    public void timeWindowAggregateManyWindowsTest() {

        final KTable<Windowed<String>, String> customers = groupedStream.cogroup(MockAggregator.TOSTRING_ADDER)
                .windowedBy(TimeWindows.of(ofMillis(500L))).aggregate(
                        MockInitializer.STRING_INIT, Materialized.with(Serdes.String(), Serdes.String()));
        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(
                    TOPIC, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<Windowed<String>, String> testOutputTopic = driver.createOutputTopic(
                    OUTPUT, new TimeWindowedDeserializer<>(new StringDeserializer()), new StringDeserializer());

            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k2", "A", 0);
            testInputTopic.pipeInput("k2", "A", 501L);
            testInputTopic.pipeInput("k1", "A", 501L);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A", 501);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A", 501);
        }
    }


    @Test
    public void timeWindowMixAggregatorsManyWindowsTest() {

        final KTable<Windowed<String>, String> customers = windowedCogroupedStream.aggregate(
                MockInitializer.STRING_INIT, Materialized.with(Serdes.String(), Serdes.String()));
        customers.toStream().to(OUTPUT);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            final TestInputTopic<String, String> testInputTopic = driver.createInputTopic(
                    TOPIC, new StringSerializer(), new StringSerializer());
            final TestInputTopic<String, String> testInputTopic2 = driver.createInputTopic(
                    TOPIC2, new StringSerializer(), new StringSerializer());
            final TestOutputTopic<Windowed<String>, String> testOutputTopic = driver.createOutputTopic(
                    OUTPUT, new TimeWindowedDeserializer<>(new StringDeserializer()), new StringDeserializer());

            testInputTopic.pipeInput("k1", "A", 0);
            testInputTopic.pipeInput("k2", "A", 0);
            testInputTopic.pipeInput("k2", "A", 1);
            testInputTopic.pipeInput("k1", "A", 2);
            testInputTopic2.pipeInput("k1", "B", 3);
            testInputTopic2.pipeInput("k2", "B", 3);
            testInputTopic2.pipeInput("k2", "B", 501);
            testInputTopic2.pipeInput("k1", "B", 501);

            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A", 0);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A+A", 1);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A+A", 2);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0+A+A-B", 3);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0+A+A-B", 3);
            assertOutputKeyValueTimestamp(testOutputTopic, "k2", "0-B", 501);
            assertOutputKeyValueTimestamp(testOutputTopic, "k1", "0-B", 501);
        }
    }

    private void assertOutputKeyValueTimestamp(final TestOutputTopic<Windowed<String>, String> outputTopic,
                                               final String expectedKey,
                                               final String expectedValue,
                                               final long expectedTimestamp) {
        final TestRecord<Windowed<String>, String> realRecord = outputTopic.readRecord();
        final TestRecord<String, String> nonWindowedRecord = new TestRecord<>(
                realRecord.getKey().key(), realRecord.getValue(), null, realRecord.timestamp());
        final TestRecord<String, String> testRecord = new TestRecord<>(expectedKey, expectedValue, null, expectedTimestamp);
        assertThat(nonWindowedRecord, equalTo(testRecord));
    }
}
