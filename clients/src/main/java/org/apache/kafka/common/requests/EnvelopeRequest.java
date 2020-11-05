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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.EnvelopeRequestData;
import org.apache.kafka.common.message.EnvelopeResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.network.SendBuilder;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class EnvelopeRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<EnvelopeRequest> {

        private final EnvelopeRequestData data;

        public Builder(ByteBuffer requestData,
                       byte[] serializedPrincipal,
                       byte[] clientAddress) {
            super(ApiKeys.ENVELOPE);
            this.data = new EnvelopeRequestData()
                            .setRequestData(requestData)
                            .setRequestPrincipal(serializedPrincipal)
                            .setClientHostAddress(clientAddress);
        }

        @Override
        public EnvelopeRequest build(short version) {
            return new EnvelopeRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final EnvelopeRequestData data;

    public EnvelopeRequest(EnvelopeRequestData data, short version) {
        super(ApiKeys.ENVELOPE, version);
        this.data = data;
    }

    public EnvelopeRequest(Struct struct, short version) {
        super(ApiKeys.ENVELOPE, version);
        this.data = new EnvelopeRequestData(struct, version);
    }

    public ByteBuffer requestData() {
        return data.requestData();
    }

    public byte[] clientAddress() {
        return data.clientHostAddress();
    }

    public byte[] requestPrincipal() {
        return data.requestPrincipal();
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new EnvelopeResponse(new EnvelopeResponseData()
                                        .setErrorCode(Errors.forException(e).code()));
    }

    public static EnvelopeRequest parse(ByteBuffer buffer, short version) {
        return new EnvelopeRequest(ApiKeys.ENVELOPE.parseRequest(version, buffer), version);
    }

    public EnvelopeRequestData data() {
        return data;
    }

    @Override
    public Send toSend(String destination, RequestHeader header) {
        RequestHeaderData headerData = header.data();
        short headerVersion = header.headerVersion();
        short apiVersion = header.apiVersion();

        ObjectSerializationCache serializationCache = new ObjectSerializationCache();
        int totalSize = headerData.size(serializationCache, headerVersion)
            + this.data.size(serializationCache, apiVersion);
        int overheadSize = totalSize
            - this.data.requestData().remaining()
            - this.data.requestPrincipal().length
            - this.data.clientHostAddress().length;

        ByteBuffer buffer = ByteBuffer.allocate(overheadSize + 4);
        SendBuilder builder = new SendBuilder(buffer);
        builder.writeInt(totalSize);
        builder.writeApiMessage(headerData, serializationCache, headerVersion);
        builder.writeApiMessage(this.data, serializationCache, apiVersion);
        return builder.toSend(destination);
    }

}
