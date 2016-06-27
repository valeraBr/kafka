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
 **/

package org.apache.kafka.connect.data;

import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DateTest {
    private static final GregorianCalendar EPOCH;
    private static final GregorianCalendar EPOCH_PLUS_TEN_THOUSAND_DAYS;
    private static final GregorianCalendar EPOCH_PLUS_TIME_COMPONENT;
    static {
        EPOCH = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        EPOCH.setTimeZone(TimeZone.getTimeZone("UTC"));

        EPOCH_PLUS_TIME_COMPONENT = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 1);
        EPOCH_PLUS_TIME_COMPONENT.setTimeZone(TimeZone.getTimeZone("UTC"));

        EPOCH_PLUS_TEN_THOUSAND_DAYS = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        EPOCH_PLUS_TEN_THOUSAND_DAYS.setTimeZone(TimeZone.getTimeZone("UTC"));
        EPOCH_PLUS_TEN_THOUSAND_DAYS.add(Calendar.DATE, 10000);
    }

    @Test
    public void testBuilder() {
        Schema plain = Date.SCHEMA;
        assertEquals(Date.LOGICAL_NAME, plain.name());
        assertEquals(1, (Object) plain.version());
    }

    @Test
    public void testFromLogical() {
        assertEquals((Integer) 0, Date.fromLogical(Date.SCHEMA, EPOCH.getTime()));
        assertEquals((Integer) 10000, Date.fromLogical(Date.SCHEMA, EPOCH_PLUS_TEN_THOUSAND_DAYS.getTime()));
    }

    @Test(expected = DataException.class)
    public void testFromLogicalInvalidSchema() {
        Date.fromLogical(Date.builder().name("invalid").build(), EPOCH.getTime());
    }

    @Test(expected = DataException.class)
    public void testFromLogicalInvalidHasTimeComponents() {
        Date.fromLogical(Date.SCHEMA, EPOCH_PLUS_TIME_COMPONENT.getTime());
    }

    @Test
    public void testToLogical() {
        assertEquals(EPOCH.getTime(), Date.toLogical(Date.SCHEMA, 0));
        assertEquals(EPOCH_PLUS_TEN_THOUSAND_DAYS.getTime(), Date.toLogical(Date.SCHEMA, 10000));
    }

    @Test(expected = DataException.class)
    public void testToLogicalInvalidSchema() {
        Date.toLogical(Date.builder().name("invalid").build(), 0);
    }

    @Test
    public void testToLogicalNullValue() {
        Schema nullableSchema = Date.builder().optional().build();
        java.util.Date actual = Date.toLogical(nullableSchema, null);
        assertNull("actual should be null.", actual);
    }

    @Test(expected = DataException.class)
    public void testToLogicalNullValueNonOptionalSchema() {
        Schema nullableSchema = Date.builder().build();
        java.util.Date actual = Date.toLogical(nullableSchema, null);
        assertNull("actual should be null.", actual);
    }

    @Test
    public void testFromLogicalNullValue() {
        Schema nullableSchema = Date.builder().optional().build();
        Integer actual = Date.fromLogical(nullableSchema, null);
        assertNull("actual should be null.", actual);
    }

    @Test(expected = DataException.class)
    public void testFromLogicalNullValueNonOptionalSchema() {
        Schema nullableSchema = Date.builder().build();
        Integer actual = Date.fromLogical(nullableSchema, null);
        assertNull("actual should be null.", actual);
    }
}
