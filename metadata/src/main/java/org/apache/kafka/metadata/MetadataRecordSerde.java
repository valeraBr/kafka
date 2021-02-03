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
package org.apache.kafka.metadata;

import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.raft.RecordSerde;

public class MetadataRecordSerde implements RecordSerde<ApiMessageAndVersion> {

    @Override
    public int recordSize(ApiMessageAndVersion data, ObjectSerializationCache serializationCache) {
        int size = ByteUtils.sizeOfUnsignedVarint(data.message().apiKey());
        size += ByteUtils.sizeOfUnsignedVarint(data.version());
        size += data.message().size(serializationCache, data.version());
        return size;
    }

    @Override
    public void write(ApiMessageAndVersion data, ObjectSerializationCache serializationCache, Writable out) {
        out.writeUnsignedVarint(data.message().apiKey());
        out.writeUnsignedVarint(data.version());
        data.message().write(out, serializationCache, data.version());
    }

    @Override
    public ApiMessageAndVersion read(Readable input, int size) {
        short apiKey = (short) input.readUnsignedVarint();
        short version = (short) input.readUnsignedVarint();
        MetadataRecordType recordType = MetadataRecordType.fromId(apiKey);
        ApiMessage record = recordType.newMetadataRecord();
        record.read(input, version);
        return new ApiMessageAndVersion(record, version);
    }

}
