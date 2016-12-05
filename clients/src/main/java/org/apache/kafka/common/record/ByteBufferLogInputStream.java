/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.common.record;

import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;

/**
 * A byte buffer backed log input stream. This class avoids the need to copy records by returning
 * slices from the underlying byte buffer.
 */
class ByteBufferLogInputStream implements LogInputStream<ByteBufferLogInputStream.ByteBufferLogEntry> {
    private final ByteBuffer buffer;
    private final int maxMessageSize;

    ByteBufferLogInputStream(ByteBuffer buffer, int maxMessageSize) {
        this.buffer = buffer;
        this.maxMessageSize = maxMessageSize;
    }

    public ByteBufferLogEntry nextEntry() throws IOException {
        int remaining = buffer.remaining();
        if (Records.LOG_OVERHEAD > remaining)
            return null;

        int size = buffer.getInt(buffer.position() + Records.SIZE_OFFSET);
        if (size < Record.RECORD_OVERHEAD_V0)
            throw new CorruptRecordException(String.format("Record size is less than the minimum record overhead (%d)", Record.RECORD_OVERHEAD_V0));
        if (size > maxMessageSize)
            throw new CorruptRecordException(String.format("Record size exceeds the largest allowable message size (%d).", maxMessageSize));

        if (size + Records.LOG_OVERHEAD > remaining)
            return null;

        ByteBufferLogEntry slice = new ByteBufferLogEntry(buffer);
        buffer.position(buffer.position() + slice.sizeInBytes());
        return slice;
    }

    public static class ByteBufferLogEntry extends LogEntry {
        private final ByteBuffer buffer;
        private final Record record;

        public ByteBufferLogEntry(ByteBuffer buffer) {
            ByteBuffer entryBuffer = buffer.duplicate();
            int size = entryBuffer.getInt(entryBuffer.position() + Records.SIZE_OFFSET);
            entryBuffer.limit(entryBuffer.position() + LOG_OVERHEAD + size);
            this.buffer = entryBuffer.slice();

            ByteBuffer recordBuffer = this.buffer.duplicate();
            recordBuffer.position(LOG_OVERHEAD);
            this.record = new Record(recordBuffer.slice());
        }

        @Override
        public long offset() {
            return buffer.getLong(0);
        }

        @Override
        public Record record() {
            return record;
        }

        public void setOffset(long offset) {
            buffer.putLong(Records.OFFSET_OFFSET, offset);
        }

        public void setCreateTime(long timestamp) {
            if (record.magic() == Record.MAGIC_VALUE_V0)
                throw new IllegalArgumentException("Cannot set timestamp for a record with magic = 0");

            long currentTimestamp = record.timestamp();
            // We don't need to recompute crc if the timestamp is not updated.
            if (record.timestampType() == TimestampType.CREATE_TIME && currentTimestamp == timestamp)
                return;

            byte attributes = record.attributes();
            buffer.put(LOG_OVERHEAD + Record.ATTRIBUTES_OFFSET, TimestampType.CREATE_TIME.updateAttributes(attributes));
            buffer.putLong(LOG_OVERHEAD + Record.TIMESTAMP_OFFSET, timestamp);
            long crc = record.computeChecksum();
            Utils.writeUnsignedInt(buffer, LOG_OVERHEAD + Record.CRC_OFFSET, crc);
        }

        public void setLogAppendTime(long timestamp) {
            if (record.magic() == Record.MAGIC_VALUE_V0)
                throw new IllegalArgumentException("Cannot set timestamp for a record with magic = 0");

            byte attributes = record.attributes();
            buffer.put(LOG_OVERHEAD + Record.ATTRIBUTES_OFFSET, TimestampType.LOG_APPEND_TIME.updateAttributes(attributes));
            buffer.putLong(LOG_OVERHEAD + Record.TIMESTAMP_OFFSET, timestamp);
            long crc = record.computeChecksum();
            Utils.writeUnsignedInt(buffer, LOG_OVERHEAD + Record.CRC_OFFSET, crc);
        }

        public ByteBuffer buffer() {
            return buffer;
        }
    }

}
