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
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KCogroupedStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindowedKCogroupedStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class KCogroupedWindowedStreamImplTest {

    private final Consumed<String, String> stringConsumed = Consumed
        .with(Serdes.String(), Serdes.String());
    private final MockProcessorSupplier<Windowed<String>, String> processorSupplier = new MockProcessorSupplier<>();
    private static final String TOPIC = "topic";
    private static final String TOPIC2 = "topic2";
    private final StreamsBuilder builder = new StreamsBuilder();
    private KGroupedStream<String, String> groupedStream;

    private KGroupedStream<String, String> groupedStream2;
    private KCogroupedStream<String, String, String> cogroupedStream;
    private TimeWindowedKCogroupedStream<String, String> windowedCogroupedStream;

    private final ConsumerRecordFactory<String, String> recordFactory =
        new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
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
        cogroupedStream = groupedStream.cogroup(MockAggregator.TOSTRING_ADDER).cogroup(groupedStream2, MockAggregator.TOSTRING_ADDER);
        windowedCogroupedStream = cogroupedStream.windowedBy(TimeWindows.of(ofMillis(500L)));
    }

    @Test
    public void timeWindowTest() {
        assertNotNull(windowedCogroupedStream);
    }

    @Test
    public void timeWindowAggregateTest() {

         final KTable customers = groupedStream.cogroup(MockAggregator.TOSTRING_ADDER).windowedBy(TimeWindows.of(ofMillis(500L))).aggregate(MockInitializer.STRING_INIT);
        customers.toStream().process(processorSupplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 0));
            driver.pipeInput(recordFactory.create(TOPIC, "11", "A", 0));
            driver.pipeInput(recordFactory.create(TOPIC, "11", "A", 1));
            driver.pipeInput(recordFactory.create(TOPIC, "1", "A", 2));
        }
        assertThat(
            processorSupplier.theCapturedProcessor().lastValueAndTimestampPerKey
                .get(new Windowed<>("1", new TimeWindow(0, 500))),
            equalTo(ValueAndTimestamp.make("0+A+A", 2)));
        assertThat(
            processorSupplier.theCapturedProcessor().lastValueAndTimestampPerKey
                .get(new Windowed<>("11", new TimeWindow(0, 500))),
            equalTo(ValueAndTimestamp.make("0+A+A", 1)));
    }




}
