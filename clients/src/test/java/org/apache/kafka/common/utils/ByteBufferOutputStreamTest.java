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

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ByteBufferOutputStreamTest {

    @Test
    public void testExpandBufferOnPositionIncrease() throws Exception {
        testExpandBufferOnPositionIncrease(ByteBuffer.allocate(16));
    }

    @Test
    public void testExpandBufferOnPositionIncreaseDirectBuffer() throws Exception {
        testExpandBufferOnPositionIncrease(ByteBuffer.allocateDirect(16));
    }

    private void testExpandBufferOnPositionIncrease(ByteBuffer initialBuffer) throws Exception {
        ByteBufferOutputStream output = new ByteBufferOutputStream(initialBuffer);
        output.write("hello".getBytes());
        output.position(32);
        assertEquals(32, output.position());
        assertEquals(0, initialBuffer.position());

        ByteBuffer buffer = output.buffer();
        assertEquals(32, buffer.limit());
        buffer.position(0);
        buffer.limit(5);
        byte[] bytes = new byte[5];
        buffer.get(bytes);
        assertArrayEquals("hello".getBytes(), bytes);
    }

    @Test
    public void testExpandBufferOnWrite() throws Exception {
        testExpandBufferOnWrite(ByteBuffer.allocate(16));
    }

    @Test
    public void testExpandBufferOnWriteDirectBuffer() throws Exception {
        testExpandBufferOnWrite(ByteBuffer.allocateDirect(16));
    }

    private void testExpandBufferOnWrite(ByteBuffer initialBuffer) throws Exception {
        ByteBufferOutputStream output = new ByteBufferOutputStream(initialBuffer);
        output.write("hello".getBytes());
        output.write(new byte[27]);
        assertEquals(32, output.position());
        assertEquals(0, initialBuffer.position());

        ByteBuffer buffer = output.buffer();
        assertEquals(32, buffer.limit());
        buffer.position(0);
        buffer.limit(5);
        byte[] bytes = new byte[5];
        buffer.get(bytes);
        assertArrayEquals("hello".getBytes(), bytes);
    }

    @Test
    public void testWriteByteBuffer() {
        testWriteByteBuffer(ByteBuffer.allocate(16));
    }

    @Test
    public void testWriteDirectByteBuffer() {
        testWriteByteBuffer(ByteBuffer.allocateDirect(16));
    }

    private void testWriteByteBuffer(ByteBuffer input) {
        long value = 234239230L;
        input.putLong(value);
        input.flip();

        ByteBufferOutputStream output = new ByteBufferOutputStream(ByteBuffer.allocate(32));
        output.write(input);
        assertEquals(8, output.position());
        assertEquals(value, output.buffer().getLong(0));
    }

}
