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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBuffer.Eviction;
import org.apache.kafka.test.MockInternalProcessorContext;
import org.apache.kafka.test.MockInternalProcessorContext.MockRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TimeOrderedKeyValueBufferTest<B extends TimeOrderedKeyValueBuffer<String, String>> {
    private static final RecordHeaders V_1_CHANGELOG_HEADERS =
        new RecordHeaders(new Header[] {new RecordHeader("v", new byte[] {(byte) 1})});

    private static final String APP_ID = "test-app";
    private final Function<String, B> bufferSupplier;
    private final String testName;

    // As we add more buffer implementations/configurations, we can add them here
    @Parameterized.Parameters(name = "{index}: test={0}")
    public static Collection<Object[]> parameters() {
        return singletonList(
            new Object[] {
                "in-memory buffer",
                (Function<String, InMemoryTimeOrderedKeyValueBuffer<String, String>>) name ->
                    new InMemoryTimeOrderedKeyValueBuffer
                        .Builder<>(name, Serdes.String(), Serdes.String())
                        .build()
            }
        );
    }

    public TimeOrderedKeyValueBufferTest(final String testName, final Function<String, B> bufferSupplier) {
        this.testName = testName + "_" + new Random().nextInt(Integer.MAX_VALUE);
        this.bufferSupplier = bufferSupplier;
    }

    private static MockInternalProcessorContext makeContext() {
        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");

        final TaskId taskId = new TaskId(0, 0);

        final MockInternalProcessorContext context = new MockInternalProcessorContext(properties, taskId, TestUtils.tempDirectory());
        context.setRecordCollector(new MockRecordCollector());

        return context;
    }


    private static void cleanup(final MockInternalProcessorContext context, final TimeOrderedKeyValueBuffer<String, String> buffer) {
        try {
            buffer.close();
            Utils.delete(context.stateDir());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void shouldInit() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        cleanup(context, buffer);
    }

    @Test
    public void shouldAcceptData() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "2p93nf");
        cleanup(context, buffer);
    }

    @Test
    public void shouldRejectNullValues() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        try {
            buffer.put(0, "asdf",
                       null,
                       getContext(0)
            );
            fail("expected an exception");
        } catch (final NullPointerException expected) {
            // expected
        }
        cleanup(context, buffer);
    }

    @Test
    public void shouldRemoveData() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "qwer");
        assertThat(buffer.numRecords(), is(1));
        buffer.evictWhile(() -> true, kv -> { });
        assertThat(buffer.numRecords(), is(0));
        cleanup(context, buffer);
    }

    @Test
    public void shouldRespectEvictionPredicate() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "eyt");
        putRecord(buffer, context, 1L, 0L, "zxcv", "rtg");
        assertThat(buffer.numRecords(), is(2));
        final List<Eviction<String, String>> evicted = new LinkedList<>();
        buffer.evictWhile(() -> buffer.numRecords() > 1, evicted::add);
        assertThat(buffer.numRecords(), is(1));
        assertThat(evicted, is(singletonList(new Eviction<>("asdf", "eyt", getContext(0L)))));
        cleanup(context, buffer);
    }

    @Test
    public void shouldTrackCount() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "oin");
        assertThat(buffer.numRecords(), is(1));
        putRecord(buffer, context, 1L, 0L, "asdf", "wekjn");
        assertThat(buffer.numRecords(), is(1));
        putRecord(buffer, context, 0L, 0L, "zxcv", "24inf");
        assertThat(buffer.numRecords(), is(2));
        cleanup(context, buffer);
    }

    @Test
    public void shouldTrackSize() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "23roni");
        assertThat(buffer.bufferSize(), is(43L));
        putRecord(buffer, context, 1L, 0L, "asdf", "3l");
        assertThat(buffer.bufferSize(), is(39L));
        putRecord(buffer, context, 0L, 0L, "zxcv", "qfowin");
        assertThat(buffer.bufferSize(), is(82L));
        cleanup(context, buffer);
    }

    @Test
    public void shouldTrackMinTimestamp() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 1L, 0L, "asdf", "2093j");
        assertThat(buffer.minTimestamp(), is(1L));
        putRecord(buffer, context, 0L, 0L, "zxcv", "3gon4i");
        assertThat(buffer.minTimestamp(), is(0L));
        cleanup(context, buffer);
    }

    @Test
    public void shouldEvictOldestAndUpdateSizeAndCountAndMinTimestamp() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);

        putRecord(buffer, context, 1L, 0L, "zxcv", "o23i4");
        assertThat(buffer.numRecords(), is(1));
        assertThat(buffer.bufferSize(), is(42L));
        assertThat(buffer.minTimestamp(), is(1L));

        putRecord(buffer, context, 0L, 0L, "asdf", "3ng");
        assertThat(buffer.numRecords(), is(2));
        assertThat(buffer.bufferSize(), is(82L));
        assertThat(buffer.minTimestamp(), is(0L));

        final AtomicInteger callbackCount = new AtomicInteger(0);
        buffer.evictWhile(() -> true, kv -> {
            switch (callbackCount.incrementAndGet()) {
                case 1: {
                    assertThat(kv.key(), is("asdf"));
                    assertThat(buffer.numRecords(), is(2));
                    assertThat(buffer.bufferSize(), is(82L));
                    assertThat(buffer.minTimestamp(), is(0L));
                    break;
                }
                case 2: {
                    assertThat(kv.key(), is("zxcv"));
                    assertThat(buffer.numRecords(), is(1));
                    assertThat(buffer.bufferSize(), is(42L));
                    assertThat(buffer.minTimestamp(), is(1L));
                    break;
                }
                default: {
                    fail("too many invocations");
                    break;
                }
            }
        });
        assertThat(callbackCount.get(), is(2));
        assertThat(buffer.numRecords(), is(0));
        assertThat(buffer.bufferSize(), is(0L));
        assertThat(buffer.minTimestamp(), is(Long.MAX_VALUE));
        cleanup(context, buffer);
    }

    @Test
    public void shouldFlush() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        final String value1 = "2093j";
        final String value2 = "3gon4i";
        putRecord(buffer, context, 2L, 0L, "asdf", value1);
        putRecord(buffer, context, 1L, 1L, "zxcv", value2);
        putRecord(buffer, context, 0L, 2L, "deleteme", "deadbeef");

        // replace "deleteme" with a tombstone
        buffer.evictWhile(() -> buffer.minTimestamp() < 1, kv -> { });

        // flush everything to the changelog
        buffer.flush();

        // the buffer should serialize the buffer time and the value as byte[],
        // which we can't compare for equality using ProducerRecord.
        // As a workaround, I'm deserializing them and shoving them in a KeyValue, just for ease of testing.

        final List<ProducerRecord<String, KeyValue<Long, String>>> collected =
            ((MockRecordCollector) context.recordCollector())
                .collected()
                .stream()
                .map(pr -> {
                    final KeyValue<Long, String> niceValue;
                    if (pr.value() == null) {
                        niceValue = null;
                    } else {
                        final byte[] timestampAndValue = pr.value();
                        final ByteBuffer wrap = ByteBuffer.wrap(timestampAndValue);
                        final long timestamp = wrap.getLong();
                        final byte[] value = new byte[pr.value().length - Long.BYTES];
                        wrap.get(value);
                        niceValue = new KeyValue<>(timestamp, new String(value, UTF_8));
                    }

                    return new ProducerRecord<>(pr.topic(),
                                                pr.partition(),
                                                pr.timestamp(),
                                                new String(pr.key(), UTF_8),
                                                niceValue,
                                                pr.headers());
                })
                .collect(Collectors.toList());

        assertThat(collected, is(asList(
            new ProducerRecord<>(APP_ID + "-" + testName + "-changelog",
                                 0,   // Producer will assign
                                 null,
                                 "deleteme",
                                 null,
                                 null
            ),
            new ProducerRecord<>(APP_ID + "-" + testName + "-changelog",
                                 0,
                                 1L,
                                 "zxcv",
                                 new KeyValue<>(1L, value2),
                                 null
            ),
            new ProducerRecord<>(APP_ID + "-" + testName + "-changelog",
                                 0,
                                 0L,
                                 "asdf",
                                 new KeyValue<>(2L, value1),
                                 null
            )
        )));

        cleanup(context, buffer);
    }

    @Test
    public void shouldRestoreOldFormat() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);

        final RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", null));

        stateRestoreCallback.restoreBatch(asList(
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 0,
                                 0,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 ByteBuffer.allocate(Long.BYTES + 6).putLong(0L).put("doomed".getBytes(UTF_8)).array()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 1,
                                 1,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 -1,
                                 "asdf".getBytes(UTF_8),
                                 ByteBuffer.allocate(Long.BYTES + 4).putLong(2L).put("qwer".getBytes(UTF_8)).array()),
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 2,
                                 2,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 ByteBuffer.allocate(Long.BYTES + 5).putLong(1L).put("3o4im".getBytes(UTF_8)).array())
        ));

        assertThat(buffer.numRecords(), is(3));
        assertThat(buffer.minTimestamp(), is(0L));
        assertThat(buffer.bufferSize(), is(160L));

        stateRestoreCallback.restoreBatch(singletonList(
            new ConsumerRecord<>("changelog-topic",
                                 0,
                                 3,
                                 3,
                                 TimestampType.CREATE_TIME,
                                 -1,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 null)
        ));

        assertThat(buffer.numRecords(), is(2));
        assertThat(buffer.minTimestamp(), is(1L));
        assertThat(buffer.bufferSize(), is(103L));

        // flush the buffer into a list in buffer order so we can make assertions about the contents.

        final List<Eviction<String, String>> evicted = new LinkedList<>();
        buffer.evictWhile(() -> true, evicted::add);

        // Several things to note:
        // * The buffered records are ordered according to their buffer time (serialized in the value of the changelog)
        // * The record timestamps are properly restored, and not conflated with the record's buffer time.
        // * The keys and values are properly restored
        // * The record topic is set to the changelog topic. This was an oversight in the original implementation,
        //   which is fixed in changelog format v1. But upgraded applications still need to be able to handle the
        //   original format.

        assertThat(evicted, is(asList(
            new Eviction<>(
                "zxcv",
                "3o4im",
                new ProcessorRecordContext(2L, 2, 0, "changelog-topic", null)),
            new Eviction<>(
                "asdf",
                "qwer",
                new ProcessorRecordContext(1L, 1, 0, "changelog-topic", null))
        )));

        cleanup(context, buffer);
    }

    @Test
    public void shouldIgnoreHeadersOnRestore() {
        final TimeOrderedKeyValueBuffer<String, String> buffer = bufferSupplier.apply(testName);
        final MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);

        final RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", null));

        final RecordHeaders unknownFlagHeaders = new RecordHeaders(new Header[] {new RecordHeader("v", new byte[] {(byte) -1})});

        final byte[] todeleteValue = getRecord("doomed", 0).serialize();
        try {
            stateRestoreCallback.restoreBatch(singletonList(
                new ConsumerRecord<>("changelog-topic",
                                     0,
                                     0,
                                     999,
                                     TimestampType.CREATE_TIME,
                                     -1L,
                                     -1,
                                     -1,
                                     "todelete".getBytes(UTF_8),
                                     ByteBuffer.allocate(Long.BYTES + todeleteValue.length).putLong(0L).put(todeleteValue).array(),
                                     unknownFlagHeaders)
            ));

            final List<Eviction<String, String>> evicted = new LinkedList<>();
            buffer.evictWhile(() -> buffer.numRecords() > 0, evicted::add);
            assertThat(evicted.size(), is(1));
            final Eviction<String, String> eviction = evicted.get(0);
            assertThat(eviction.recordContext().headers(), nullValue());
        } finally {
            cleanup(context, buffer);
        }
    }

    private static void putRecord(final TimeOrderedKeyValueBuffer<String, String> buffer,
                                  final MockInternalProcessorContext context,
                                  final long streamTime,
                                  final long recordTimestamp,
                                  final String key,
                                  final String value) {
        final ProcessorRecordContext recordContext = getContext(recordTimestamp);
        context.setRecordContext(recordContext);
        buffer.put(streamTime, key, value, recordContext);
    }

    private static ContextualRecord getRecord(final String value, final long timestamp) {
        return new ContextualRecord(
            value.getBytes(UTF_8),
            getContext(timestamp)
        );
    }

    private static ProcessorRecordContext getContext(final long recordTimestamp) {
        return new ProcessorRecordContext(recordTimestamp, 0, 0, "topic", null);
    }
}
