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
package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.FullChangeSerde;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.test.MockInternalProcessorContext;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.time.Duration;
import java.util.Collection;

import static java.time.Duration.ZERO;
import static java.time.Duration.ofMillis;
import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxBytes;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxRecords;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;
import static org.apache.kafka.streams.kstream.WindowedSerdes.sessionWindowedSerdeFrom;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

@SuppressWarnings("PointlessArithmeticExpression")
public class KTableSuppressProcessorTest {
    private static final long ARBITRARY_LONG = 5L;

    private static final Change<Long> ARBITRARY_CHANGE = new Change<>(7L, 14L);

    @Test
    public void zeroTimeLimitShouldImmediatelyEmit() {
        final KTableSuppressProcessor<String, Long> processor =
            new KTableSuppressProcessor<>(getImpl(untilTimeLimit(ZERO, unbounded())), String(), new FullChangeSerde<>(Long()));

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = ARBITRARY_LONG;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        context.setStreamTime(timestamp);
        final String key = "hey";
        final Change<Long> value = ARBITRARY_CHANGE;
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void windowedZeroTimeLimitShouldImmediatelyEmit() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor =
            new KTableSuppressProcessor<>(
                getImpl(untilTimeLimit(ZERO, unbounded())),
                timeWindowedSerdeFrom(String.class, 100L),
                new FullChangeSerde<>(Long())
            );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = ARBITRARY_LONG;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        context.setStreamTime(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0L, 100L));
        final Change<Long> value = ARBITRARY_CHANGE;
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void intermediateSuppressionShouldBufferAndEmitLater() {
        final KTableSuppressProcessor<String, Long> processor =
            new KTableSuppressProcessor<>(
                getImpl(untilTimeLimit(ofMillis(1), unbounded())),
                String(),
                new FullChangeSerde<>(Long())
            );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 0L;
        context.setRecordMetadata("topic", 0, 0, null, timestamp);
        context.setStreamTime(timestamp);
        final String key = "hey";
        final Change<Long> value = new Change<>(null, 1L);
        processor.process(key, value);
        assertThat(context.forwarded(), hasSize(0));

        context.setRecordMetadata("topic", 0, 1, null, 1L);
        context.setStreamTime(1L);
        processor.process("tick", new Change<>(null, null));

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void finalResultsSuppressionShouldBufferAndEmitAtGraceExpiration() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor = new KTableSuppressProcessor<>(
            finalResults(ofMillis(1L)),
            timeWindowedSerdeFrom(String.class, 1L),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 100L;
        context.setRecordMetadata("topic", 0, 0, null, timestamp);
        context.setStreamTime(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(timestamp - 1L, timestamp));
        final Change<Long> value = ARBITRARY_CHANGE;
        processor.process(key, value);
        assertThat(context.forwarded(), hasSize(0));

        context.setRecordMetadata("topic", 0, 1, null, timestamp + 1L);
        context.setStreamTime(timestamp + 1L);
        processor.process(new Windowed<>("dummyKey", new TimeWindow(timestamp, timestamp + 1L)), ARBITRARY_CHANGE);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    /**
     * Testing a special case of final results: that even with a grace period of 0,
     * it will still buffer events and emit only after the end of the window.
     * As opposed to emitting immediately the way regular suppresion would with a time limit of 0.
     */
    @Test
    public void finalResultsWithZeroGraceShouldStillBufferUntilTheWindowEnd() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor = new KTableSuppressProcessor<>(
            finalResults(ofMillis(0)),
            timeWindowedSerdeFrom(String.class, 100L),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        // note the record is in the past, but the window end is in the future, so we still have to buffer,
        // even though the grace period is 0.
        final long timestamp = 5L;
        final long streamTime = 99L;
        final long windowEnd = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        context.setStreamTime(streamTime);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0, windowEnd));
        final Change<Long> value = ARBITRARY_CHANGE;
        processor.process(key, value);
        assertThat(context.forwarded(), hasSize(0));

        context.setRecordMetadata("", 0, 1L, null, windowEnd);
        context.setStreamTime(windowEnd);
        processor.process(new Windowed<>("dummyKey", new TimeWindow(windowEnd, windowEnd + 100L)), ARBITRARY_CHANGE);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void finalResultsWithZeroGraceAtWindowEndShouldImmediatelyEmit() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor = new KTableSuppressProcessor<>(
            finalResults(ofMillis(0)),
            timeWindowedSerdeFrom(String.class, 100L),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        context.setStreamTime(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0, 100L));
        final Change<Long> value = ARBITRARY_CHANGE;
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void finalResultsShouldSuppressTombstonesForTimeWindows() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor = new KTableSuppressProcessor<>(
            finalResults(ofMillis(0)),
            timeWindowedSerdeFrom(String.class, 100L),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        context.setStreamTime(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0, 100L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(0));
    }

    @Test
    public void finalResultsShouldSuppressTombstonesForSessionWindows() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor = new KTableSuppressProcessor<>(
            finalResults(ofMillis(0)),
            sessionWindowedSerdeFrom(String.class),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        context.setStreamTime(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new SessionWindow(0L, 0L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(0));
    }

    @Test
    public void suppressShouldNotSuppressTombstonesForTimeWindows() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor = new KTableSuppressProcessor<>(
            getImpl(untilTimeLimit(ofMillis(0), maxRecords(0))),
            timeWindowedSerdeFrom(String.class, 100L),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        context.setStreamTime(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new TimeWindow(0L, 100L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void suppressShouldNotSuppressTombstonesForSessionWindows() {
        final KTableSuppressProcessor<Windowed<String>, Long> processor = new KTableSuppressProcessor<>(
            getImpl(untilTimeLimit(ofMillis(0), maxRecords(0))),
            sessionWindowedSerdeFrom(String.class),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        context.setStreamTime(timestamp);
        final Windowed<String> key = new Windowed<>("hey", new SessionWindow(0L, 0L));
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void suppressShouldNotSuppressTombstonesForKTable() {
        final KTableSuppressProcessor<String, Long> processor = new KTableSuppressProcessor<>(
            getImpl(untilTimeLimit(ofMillis(0), maxRecords(0))),
            Serdes.String(),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 100L;
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        context.setStreamTime(timestamp);
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void suppressShouldEmitWhenOverRecordCapacity() {
        final KTableSuppressProcessor<String, Long> processor = new KTableSuppressProcessor<>(
            getImpl(untilTimeLimit(Duration.ofDays(100), maxRecords(1))),
            Serdes.String(),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 100L;
        context.setStreamTime(timestamp);
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        context.setRecordMetadata("", 0, 1L, null, timestamp + 1);
        processor.process("dummyKey", value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void suppressShouldEmitWhenOverByteCapacity() {
        final KTableSuppressProcessor<String, Long> processor = new KTableSuppressProcessor<>(
            getImpl(untilTimeLimit(Duration.ofDays(100), maxBytes(60L))),
            Serdes.String(),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 100L;
        context.setStreamTime(timestamp);
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        context.setRecordMetadata("", 0, 1L, null, timestamp + 1);
        processor.process("dummyKey", value);

        assertThat(context.forwarded(), hasSize(1));
        final MockProcessorContext.CapturedForward capturedForward = context.forwarded().get(0);
        assertThat(capturedForward.keyValue(), is(new KeyValue<>(key, value)));
        assertThat(capturedForward.timestamp(), is(timestamp));
    }

    @Test
    public void suppressShouldShutDownWhenOverRecordCapacity() {
        final KTableSuppressProcessor<String, Long> processor = new KTableSuppressProcessor<>(
            getImpl(untilTimeLimit(Duration.ofDays(100), maxRecords(1).shutDownWhenFull())),
            Serdes.String(),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 100L;
        context.setStreamTime(timestamp);
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        context.setCurrentNode(new ProcessorNode("testNode"));
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        context.setRecordMetadata("", 0, 1L, null, timestamp);
        try {
            processor.process("dummyKey", value);
            fail("expected an exception");
        } catch (final StreamsException e) {
            assertThat(e.getMessage(), containsString("buffer exceeded its max capacity"));
        }
    }

    @Test
    public void suppressShouldShutDownWhenOverByteCapacity() {
        final KTableSuppressProcessor<String, Long> processor = new KTableSuppressProcessor<>(
            getImpl(untilTimeLimit(Duration.ofDays(100), maxBytes(60L).shutDownWhenFull())),
            Serdes.String(),
            new FullChangeSerde<>(Long())
        );

        final MockInternalProcessorContext context = new MockInternalProcessorContext();
        processor.init(context);

        final long timestamp = 100L;
        context.setStreamTime(timestamp);
        context.setRecordMetadata("", 0, 0L, null, timestamp);
        context.setCurrentNode(new ProcessorNode("testNode"));
        final String key = "hey";
        final Change<Long> value = new Change<>(null, ARBITRARY_LONG);
        processor.process(key, value);

        context.setRecordMetadata("", 0, 1L, null, timestamp);
        try {
            processor.process("dummyKey", value);
            fail("expected an exception");
        } catch (final StreamsException e) {
            assertThat(e.getMessage(), containsString("buffer exceeded its max capacity"));
        }
    }

    @SuppressWarnings("unchecked")
    private <K extends Windowed> SuppressedInternal<K> finalResults(final Duration grace) {
        return ((FinalResultsSuppressionBuilder) untilWindowCloses(unbounded())).buildFinalResultsSuppression(grace);
    }

    private static <E> Matcher<Collection<E>> hasSize(final int i) {
        return new BaseMatcher<Collection<E>>() {
            @Override
            public void describeTo(final Description description) {
                description.appendText("a collection of size " + i);
            }

            @SuppressWarnings("unchecked")
            @Override
            public boolean matches(final Object item) {
                if (item == null) {
                    return false;
                } else {
                    return ((Collection<E>) item).size() == i;
                }
            }

        };
    }

    private static <K> SuppressedInternal<K> getImpl(final Suppressed<K> suppressed) {
        return (SuppressedInternal<K>) suppressed;
    }

    private <K> Serde<Windowed<K>> timeWindowedSerdeFrom(final Class<K> rawType, final long windowSize) {
        final Serde<K> kSerde = Serdes.serdeFrom(rawType);
        return new Serdes.WrapperSerde<>(
            new TimeWindowedSerializer<>(kSerde.serializer()),
            new TimeWindowedDeserializer<>(kSerde.deserializer(), windowSize)
        );
    }
}