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

package org.apache.kafka.common.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

public class CircularIteratorTest {

    @Test(expected = NullPointerException.class)
    public void testNullCollection() {
        new CircularIterator<>(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyCollection() {
        new CircularIterator<>(Collections.emptyList());
    }

    @Test()
    public void testCycleCollection() {
        final CircularIterator<String> it = new CircularIterator<>(Arrays.asList("A", "B", "C"));

        assertTrue(it.hasNext());
        assertEquals("A", it.next());
        assertTrue(it.hasNext());
        assertEquals("B", it.next());
        assertTrue(it.hasNext());
        assertEquals("C", it.next());
        assertTrue(it.hasNext());
        assertEquals("A", it.next());
        assertTrue(it.hasNext());
    }

    @Test()
    public void testPeekCollection() {
        final CircularIterator<String> it = new CircularIterator<>(Arrays.asList("A", "B", "C"));

        assertTrue(it.hasNext());
        assertEquals("A", it.peek());
        assertTrue(it.hasNext());
        assertEquals("A", it.peek());
        assertTrue(it.hasNext());
        assertEquals("A", it.next());

        assertEquals("B", it.next());
        assertTrue(it.hasNext());
        assertEquals("C", it.next());
        assertTrue(it.hasNext());
        assertEquals("A", it.next());
        assertTrue(it.hasNext());
    }
}
