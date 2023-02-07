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
package org.apache.kafka.tools;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.tools.ConsoleProducer.Header;
import org.apache.kafka.tools.ConsoleProducer.LineMessageReader;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LineMessageReaderTest {
    private static Properties defaultTestProps() {
        Properties props = new Properties();
        props.put("topic", "topic");
        props.put("parse.key", "true");
        props.put("parse.headers", "true");
        return props;
    }

    @Test
    public void testLineReader() {
        String input = "key0\tvalue0\nkey1\tvalue1";

        Properties props = defaultTestProps();
        props.put("parse.headers", "false");

        runTest(props, input, record("key0", "value0"), record("key1", "value1"));
    }

    @Test
    public void testLineReaderHeader() {
        String input = "headerKey0:headerValue0,headerKey1:headerValue1\tkey0\tvalue0\n";
        ProducerRecord<String, String> expected = record(
                "key0",
                "value0",
                asList(new Header("headerKey0", "headerValue0".getBytes(UTF_8)),
                        new Header("headerKey1", "headerValue1".getBytes(UTF_8))));

        runTest(defaultTestProps(), input, expected);
    }

    @Test
    public void testMinimalValidInputWithHeaderKeyAndValue() {
        runTest(defaultTestProps(), ":\t\t",
                record("", "", singletonList(new Header("", "".getBytes(UTF_8)))));
    }

    @Test
    public void testKeyMissingValue() {
        Properties props = defaultTestProps();
        props.put("parse.headers", "false");
        runTest(props, "key\t", record("key", ""));
    }

    @Test
    public void testDemarcationsLongerThanOne() {
        Properties props = defaultTestProps();
        props.put("key.separator", "\t\t");
        props.put("headers.delimiter", "\t\t");
        props.put("headers.separator", "---");
        props.put("headers.key.separator", "::::");

        runTest(
                props,
                "headerKey0.0::::headerValue0.0---headerKey1.0::::\t\tkey\t\tvalue",
                record("key",
                        "value",
                        asList(new Header("headerKey0.0", "headerValue0.0".getBytes(UTF_8)),
                                new Header("headerKey1.0", "".getBytes(UTF_8)))));
    }

    @Test
    public void testLineReaderHeaderNoKey() {
        String input = "headerKey:headerValue\tvalue\n";

        Properties props = defaultTestProps();
        props.put("parse.key", "false");

        runTest(props, input, record(null, "value",
                singletonList(new Header("headerKey", "headerValue".getBytes(UTF_8)))));
    }

    @Test
    public void testLineReaderOnlyValue() {
        Properties props = defaultTestProps();
        props.put("parse.key", "false");
        props.put("parse.headers", "false");

        runTest(props, "value\n", record(null, "value"));
    }

    @Test
    public void testParseHeaderEnabledWithCustomDelimiterAndVaryingNumberOfKeyValueHeaderPairs() {
        Properties props = defaultTestProps();
        props.put("key.separator", "#");
        props.put("headers.delimiter", "!");
        props.put("headers.separator", "&");
        props.put("headers.key.separator", ":");

        String input =
                "headerKey0.0:headerValue0.0&headerKey0.1:headerValue0.1!key0#value0\n" +
                        "headerKey1.0:headerValue1.0!key1#value1";

        ProducerRecord<String, String> record0 = record(
                "key0",
                "value0",
                asList(new Header("headerKey0.0", "headerValue0.0".getBytes(UTF_8)),
                        new Header("headerKey0.1", "headerValue0.1".getBytes(UTF_8))));

        ProducerRecord<String, String> record1 = record(
                "key1",
                "value1",
                asList(new Header("headerKey1.0", "headerValue1.0".getBytes(UTF_8))));

        runTest(props, input, record0, record1);
    }

    @Test
    public void testMissingKeySeparator() {
        MessageReader lineReader = new LineMessageReader();
        String input =
                "headerKey0.0:headerValue0.0,headerKey0.1:headerValue0.1\tkey0\tvalue0\n" +
                        "headerKey1.0:headerValue1.0\tkey1[MISSING-DELIMITER]value1";

        lineReader.init(new ByteArrayInputStream(input.getBytes(UTF_8)), defaultTestProps());
        lineReader.readMessage();

        KafkaException expectedException = assertThrows(KafkaException.class, lineReader::readMessage);

        assertEquals(
                "No key separator found on line number 2: 'headerKey1.0:headerValue1.0\tkey1[MISSING-DELIMITER]value1'",
                expectedException.getMessage()
        );
    }

    @Test
    public void testMissingHeaderKeySeparator() {
        MessageReader lineReader = new LineMessageReader();
        String input = "key[MISSING-DELIMITER]val\tkey0\tvalue0\n";
        lineReader.init(new ByteArrayInputStream(input.getBytes(UTF_8)), defaultTestProps());

        KafkaException expectedException = assertThrows(KafkaException.class, lineReader::readMessage);

        assertEquals(
                "No header key separator found in pair 'key[MISSING-DELIMITER]val' on line number 1",
                expectedException.getMessage()
        );
    }

    @Test
    public void testHeaderDemarcationCollision() {
        Properties props = defaultTestProps();
        props.put("headers.delimiter", "\t");
        props.put("headers.separator", "\t");
        props.put("headers.key.separator", "\t");

        assertThrowsOnInvalidPatternConfig(props, "headers.delimiter and headers.separator may not be equal");

        props.put("headers.separator", ",");
        assertThrowsOnInvalidPatternConfig(props, "headers.delimiter and headers.key.separator may not be equal");

        props.put("headers.key.separator", ",");
        assertThrowsOnInvalidPatternConfig(props, "headers.separator and headers.key.separator may not be equal");
    }

    private static void assertThrowsOnInvalidPatternConfig(Properties props, String expectedMessage) {
        KafkaException exception = assertThrows(KafkaException.class, () -> new LineMessageReader().init(null, props));
        assertEquals(expectedMessage, exception.getMessage());
    }

    @Test
    public void testIgnoreErrorInInput() {
        String input =
                "headerKey0.0:headerValue0.0\tkey0\tvalue0\n" +
                        "headerKey1.0:headerValue1.0,headerKey1.1:headerValue1.1[MISSING-HEADER-DELIMITER]key1\tvalue1\n" +
                        "headerKey2.0:headerValue2.0\tkey2[MISSING-KEY-DELIMITER]value2\n" +
                        "headerKey3.0:headerValue3.0[MISSING-HEADER-DELIMITER]key3[MISSING-KEY-DELIMITER]value3\n";

        Properties props = defaultTestProps();
        props.put("ignore.error", "true");

        ProducerRecord<String, String> validRecord = record(
                "key0",
                "value0",
                asList(new Header("headerKey0.0", "headerValue0.0".getBytes(UTF_8))));

        ProducerRecord<String, String> missingHeaderDelimiter = record(
                null,
                "value1",
                asList(new Header("headerKey1.0", "headerValue1.0".getBytes(UTF_8)),
                        new Header("headerKey1.1", "headerValue1.1[MISSING-HEADER-DELIMITER]key1".getBytes(UTF_8))));

        ProducerRecord<String, String> missingKeyDelimiter = record(
                null,
                "key2[MISSING-KEY-DELIMITER]value2",
                asList(new Header("headerKey2.0", "headerValue2.0".getBytes(UTF_8))));

        ProducerRecord<String, String> missingKeyHeaderDelimiter = record(
                null,
                "headerKey3.0:headerValue3.0[MISSING-HEADER-DELIMITER]key3[MISSING-KEY-DELIMITER]value3",
                emptyList());

        runTest(props, input, validRecord, missingHeaderDelimiter, missingKeyDelimiter, missingKeyHeaderDelimiter);
    }

    @Test
    public void testMalformedHeaderIgnoreError() {
        String input = "key-val\tkey0\tvalue0\n";

        Properties props = defaultTestProps();
        props.put("ignore.error", "true");

        ProducerRecord<String, String> expected = record(
                "key0",
                "value0",
                singletonList(new Header("key-val", null)));

        runTest(props, input, expected);
    }

    @Test
    public void testNullMarker() {
        String input =
                "key\t\n" +
                        "key\t<NULL>\n" +
                        "key\t<NULL>value\n" +
                        "<NULL>\tvalue\n" +
                        "<NULL>\t<NULL>";

        Properties props = defaultTestProps();
        props.put("null.marker", "<NULL>");
        props.put("parse.headers", "false");
        runTest(props, input,
                record("key", ""),
                record("key", null),
                record("key", "<NULL>value"),
                record(null, "value"),
                record(null, null));

        // If the null marker is not set
        props.remove("null.marker");
        runTest(props, input,
                record("key", ""),
                record("key", "<NULL>"),
                record("key", "<NULL>value"),
                record("<NULL>", "value"),
                record("<NULL>", "<NULL>"));
    }

    @Test
    public void testNullMarkerWithHeaders() {
        String input =
                "h0:v0,h1:v1\t<NULL>\tvalue\n" +
                        "<NULL>\tkey\t<NULL>\n" +
                        "h0:,h1:v1\t<NULL>\t<NULL>\n" +
                        "h0:<NULL>,h1:v1\tkey\t<NULL>\n" +
                        "h0:<NULL>,h1:<NULL>value\tkey\t<NULL>\n";
        Header header = new Header("h1", "v1".getBytes(UTF_8));

        Properties props = defaultTestProps();
        props.put("null.marker", "<NULL>");
        runTest(props, input,
                record(null, "value", asList(new Header("h0", "v0".getBytes(UTF_8)), header)),
                record("key", null),
                record(null, null, asList(new Header("h0", "".getBytes(UTF_8)), header)),
                record("key", null, asList(new Header("h0", null), header)),
                record("key", null, asList(new Header("h0", null), new Header("h1", "<NULL>value".getBytes(UTF_8)))));

        // If the null marker is not set
        MessageReader lineReader = new LineMessageReader();
        props.remove("null.marker");
        lineReader.init(new ByteArrayInputStream(input.getBytes(UTF_8)), props);
        assertRecordEquals(record(
                "<NULL>",
                "value",
                asList(new Header("h0", "v0".getBytes(UTF_8)), header)),
                lineReader.readMessage());
        // line 2 is not valid anymore
        KafkaException expectedException = assertThrows(KafkaException.class, lineReader::readMessage);
        assertEquals(
                "No header key separator found in pair '<NULL>' on line number 2",
                expectedException.getMessage()
        );
        assertRecordEquals(record("<NULL>", "<NULL>", asList(new Header("h0", "".getBytes(UTF_8)), header)), lineReader.readMessage());
        assertRecordEquals(record("key", "<NULL>", asList(new Header("h0", "<NULL>".getBytes(UTF_8)), header)), lineReader.readMessage());
        assertRecordEquals(record("key", "<NULL>", asList(new Header("h0", "<NULL>".getBytes(UTF_8)), new Header("h1", "<NULL>value".getBytes(UTF_8)))), lineReader.readMessage());
    }

    @Test
    public void testNullMarkerHeaderKeyThrows() {
        String input = "<NULL>:v0,h1:v1\tkey\tvalue\n";

        Properties props = defaultTestProps();
        props.put("null.marker", "<NULL>");
        MessageReader lineReader = new LineMessageReader();
        lineReader.init(new ByteArrayInputStream(input.getBytes(UTF_8)), props);
        KafkaException expectedException = assertThrows(KafkaException.class, lineReader::readMessage);
        assertEquals(
                "Header keys should not be equal to the null marker '<NULL>' as they can't be null",
                expectedException.getMessage()
        );

        // If the null marker is not set
        props.remove("null.marker");
        runTest(props, input, record(
                "key",
                "value",
                asList(new Header("<NULL>", "v0".getBytes(UTF_8)), new Header("h1", "v1".getBytes(UTF_8)))));
    }

    @Test
    public void testInvalidNullMarker() {
        Properties props = defaultTestProps();
        props.put("headers.delimiter", "-");
        props.put("headers.separator", ":");
        props.put("headers.key.separator", "/");

        props.put("null.marker", "-");
        assertThrowsOnInvalidPatternConfig(props, "null.marker and headers.delimiter may not be equal");

        props.put("null.marker", ":");
        assertThrowsOnInvalidPatternConfig(props, "null.marker and headers.separator may not be equal");

        props.put("null.marker", "/");
        assertThrowsOnInvalidPatternConfig(props, "null.marker and headers.key.separator may not be equal");
    }

    @SafeVarargs
    private static void runTest(Properties props, String input, ProducerRecord<String, String>... expectedRecords) {
        MessageReader lineReader = new LineMessageReader();
        lineReader.init(new ByteArrayInputStream(input.getBytes(UTF_8)), props);

        for (ProducerRecord<String, String> expectedRecord: expectedRecords) {
            assertRecordEquals(expectedRecord, lineReader.readMessage());
        }
    }

    //  The equality method of ProducerRecord compares memory references for the header iterator, this is why this custom equality check is used.
    private static <K, V> void assertRecordEquals(ProducerRecord<K, V> expected, ProducerRecord<byte[], byte[]> actual) {
        assertEquals(expected.key(), actual.key() == null ? null : new String(actual.key()));
        assertEquals(expected.value(), actual.value() == null ? null : new String(actual.value()));
        assertArrayEquals(expected.headers().toArray(), actual.headers().toArray());
    }

    private static <K, V> ProducerRecord<K, V> record(K key, V value, List<Header> headers) {
        ProducerRecord<K, V> record = new ProducerRecord<>("topic", key, value);
        headers.forEach(h -> record.headers().add(h.key(), h.value()));
        return record;
    }

    private static <K, V> ProducerRecord<K, V> record(K key, V value) {
        return new ProducerRecord<>("topic", key, value);
    }
}
