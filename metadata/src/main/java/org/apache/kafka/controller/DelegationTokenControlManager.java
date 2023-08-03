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

package org.apache.kafka.controller;

import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData.CreatableRenewers;
import org.apache.kafka.common.message.CreateDelegationTokenResponseData;
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData;
import org.apache.kafka.common.message.ExpireDelegationTokenResponseData;
import org.apache.kafka.common.message.RenewDelegationTokenRequestData;
import org.apache.kafka.common.message.RenewDelegationTokenResponseData;
import org.apache.kafka.common.metadata.DelegationTokenRecord;
import org.apache.kafka.common.metadata.RemoveDelegationTokenRecord;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.metadata.DelegationTokenData;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.common.utils.Time;

import java.nio.charset.StandardCharsets;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.Mac;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.apache.kafka.common.protocol.Errors.DELEGATION_TOKEN_AUTH_DISABLED;
import static org.apache.kafka.common.protocol.Errors.DELEGATION_TOKEN_EXPIRED;
import static org.apache.kafka.common.protocol.Errors.DELEGATION_TOKEN_NOT_FOUND;
import static org.apache.kafka.common.protocol.Errors.DELEGATION_TOKEN_OWNER_MISMATCH;
import static org.apache.kafka.common.protocol.Errors.INVALID_PRINCIPAL_TYPE;
import static org.apache.kafka.common.protocol.Errors.NONE;
import static org.apache.kafka.common.protocol.Errors.UNSUPPORTED_VERSION;

/**
 * Manages DelegationTokens.
 */
public class DelegationTokenControlManager {
    private Time time = Time.SYSTEM;

    static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;
        private DelegationTokenCache tokenCache = null;
        private String secretKeyString = null;
        private long tokenDefaultMaxLifetime = 0;
        private long tokenDefaultRenewLifetime = 0;
        private long tokenRemoverScanInterval = 0;

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        Builder setTokenCache(DelegationTokenCache tokenCache) {
            this.tokenCache = tokenCache;
            return this;
        }

        Builder setTokenKeyString(String secretKeyString) {
            this.secretKeyString = secretKeyString;
            return this;
        }

        Builder setDelegationTokenMaxLifeMs(long tokenDefaultMaxLifetime) {
            this.tokenDefaultMaxLifetime = tokenDefaultMaxLifetime;
            return this;
        }

        Builder setDelegationTokenExpiryTimeMs(long tokenDefaultRenewLifetime) {
            this.tokenDefaultRenewLifetime = tokenDefaultRenewLifetime;
            return this;
        }

        Builder setDelegationTokenExpiryCheckIntervalMs(long tokenRemoverScanInterval) {
            this.tokenRemoverScanInterval = tokenRemoverScanInterval;
            return this;
        }

        DelegationTokenControlManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            return new DelegationTokenControlManager(
              logContext,
              snapshotRegistry,
              tokenCache,
              secretKeyString,
              tokenDefaultMaxLifetime,
              tokenDefaultRenewLifetime,
              tokenRemoverScanInterval);
        }
    }

    private final Logger log;
    private final DelegationTokenCache tokenCache;
    private final String secretKeyString;
    private final long tokenDefaultMaxLifetime;
    private final long tokenDefaultRenewLifetime;
    private final long tokenRemoverScanInterval;
    long tokenRemoverScanLastTime;

    private DelegationTokenControlManager(
        LogContext logContext,
        SnapshotRegistry snapshotRegistry,
        DelegationTokenCache tokenCache,
        String secretKeyString,
        long tokenDefaultMaxLifetime,
        long tokenDefaultRenewLifetime,
        long tokenRemoverScanInterval
    ) {
        this.log = logContext.logger(DelegationTokenControlManager.class);
        this.tokenCache = tokenCache;
        this.secretKeyString = secretKeyString;
        this.tokenDefaultMaxLifetime = tokenDefaultMaxLifetime;
        this.tokenDefaultRenewLifetime = tokenDefaultRenewLifetime;
        this.tokenRemoverScanInterval = tokenRemoverScanInterval;
        this.tokenRemoverScanLastTime = time.milliseconds();
    }

    public static byte[] toBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] createHmac(String tokenId) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA512");
        SecretKeySpec secretKey = new SecretKeySpec(toBytes(secretKeyString), mac.getAlgorithm());

        mac.init(secretKey);
        return mac.doFinal(toBytes(tokenId));
    }

    private TokenInformation getToken(byte[] hmac) {
        String base64Pwd = Base64.getEncoder().encodeToString(hmac);
        return tokenCache.tokenForHmac(base64Pwd);
    }

    private boolean allowedToRenew(TokenInformation tokenInfo, KafkaPrincipal renewer) {
        if (tokenInfo.owner().equals(renewer)) {
            return true;
        }
        for (KafkaPrincipal validRenewer : tokenInfo.renewers()) {
            if (validRenewer.equals(renewer)) {
                return true;
            }
        }
        return false;
    }

    public boolean isEnabled() {
        if (secretKeyString != null) {
            return true;
        }
        return false;
    }

    /*
     * Pass in the MetadataVersion so that we can return a response to the caller 
     * if the current metadataVersion is too low.
     */
    public ControllerResult<CreateDelegationTokenResponseData> createDelegationToken(
        ControllerRequestContext context,
        CreateDelegationTokenRequestData requestData,
        MetadataVersion metadataVersion
    ) {
        long now = time.milliseconds();
        long maxLifeTime = tokenDefaultMaxLifetime;
        if (requestData.maxLifetimeMs() > 0) {
            maxLifeTime = Math.min(maxLifeTime, requestData.maxLifetimeMs());
        }

        long maxTimestamp = now + maxLifeTime;
        long expiryTimestamp = Math.min(maxTimestamp, now + tokenDefaultRenewLifetime);

        String tokenId = Uuid.randomUuid().toString();

        KafkaPrincipal owner = context.principal();
        if ((requestData.ownerPrincipalName() != null) && 
            (!requestData.ownerPrincipalName().isEmpty())) {

            owner = new KafkaPrincipal(requestData.ownerPrincipalType(), requestData.ownerPrincipalName());
        }
        CreateDelegationTokenResponseData responseData = new CreateDelegationTokenResponseData()
                .setPrincipalName(owner.getName())
                .setPrincipalType(owner.getPrincipalType())
                .setTokenRequesterPrincipalName(context.principal().getName())
                .setTokenRequesterPrincipalType(context.principal().getPrincipalType());

        List<ApiMessageAndVersion> records = new ArrayList<>();

        if (secretKeyString == null) {
            // DelegationTokens are not enabled
            return ControllerResult.atomicOf(records, responseData.setErrorCode(DELEGATION_TOKEN_AUTH_DISABLED.code()));
        }

        if (!metadataVersion.isDelegationTokenSupported()) {
            // DelegationTokens are not supported in this metadata version
            return ControllerResult.atomicOf(records, responseData.setErrorCode(UNSUPPORTED_VERSION.code()));
        }

        List<KafkaPrincipal> renewers = new ArrayList<KafkaPrincipal>();
        for (CreatableRenewers renewer : requestData.renewers()) {
            if (renewer.principalType().equals(KafkaPrincipal.USER_TYPE)) {
                renewers.add(new KafkaPrincipal(renewer.principalType(), renewer.principalName()));
            } else {
                return ControllerResult.atomicOf(records, responseData.setErrorCode(INVALID_PRINCIPAL_TYPE.code()));
            }
        }

        byte[] hmac;
        try {
            hmac = createHmac(tokenId);
        } catch (Throwable e) {
            return ControllerResult.atomicOf(records, responseData.setErrorCode(ApiError.fromThrowable(e).error().code()));
        }

        TokenInformation newTokenInformation = new TokenInformation(tokenId, owner,
            context.principal(), renewers, now, maxTimestamp, expiryTimestamp);

        DelegationTokenData newDelegationTokenData = new DelegationTokenData(newTokenInformation);

        responseData
                .setErrorCode(NONE.code())
                .setIssueTimestampMs(now)
                .setExpiryTimestampMs(expiryTimestamp)
                .setMaxTimestampMs(maxTimestamp)
                .setTokenId(tokenId)
                .setHmac(hmac);

        records.add(new ApiMessageAndVersion(newDelegationTokenData.toRecord(), (short) 0));
        return ControllerResult.atomicOf(records, responseData);
    }

    public ControllerResult<RenewDelegationTokenResponseData> renewDelegationToken(
        ControllerRequestContext context,
        RenewDelegationTokenRequestData requestData,
        MetadataVersion metadataVersion
    ) {
        long now = time.milliseconds();

        List<ApiMessageAndVersion> records = new ArrayList<>();
        RenewDelegationTokenResponseData responseData = new RenewDelegationTokenResponseData();

        TokenInformation myTokenInformation = getToken(requestData.hmac());

        if (myTokenInformation == null) {
            return ControllerResult.atomicOf(records, responseData.setErrorCode(DELEGATION_TOKEN_NOT_FOUND.code()));
        }

        if (myTokenInformation.maxTimestamp() < now || myTokenInformation.expiryTimestamp() < now) {
            return ControllerResult.atomicOf(records, responseData.setErrorCode(DELEGATION_TOKEN_EXPIRED.code()));
        }

        if (!allowedToRenew(myTokenInformation, context.principal())) {
            return ControllerResult.atomicOf(records, responseData.setErrorCode(DELEGATION_TOKEN_OWNER_MISMATCH.code()));
        }

        long renewLifeTime = tokenDefaultRenewLifetime;
        if (requestData.renewPeriodMs() > 0) {
            renewLifeTime = Math.min(renewLifeTime, requestData.renewPeriodMs());
        }
        long renewTimeStamp = now + renewLifeTime;
        long expiryTimestamp = Math.min(myTokenInformation.maxTimestamp(), renewTimeStamp);

        myTokenInformation.setExpiryTimestamp(expiryTimestamp);

        DelegationTokenData newDelegationTokenData = new DelegationTokenData(myTokenInformation);

        responseData
            .setErrorCode(NONE.code())
            .setExpiryTimestampMs(expiryTimestamp);

        records.add(new ApiMessageAndVersion(newDelegationTokenData.toRecord(), (short) 0));
        return ControllerResult.atomicOf(records, responseData);
    }

    public ControllerResult<ExpireDelegationTokenResponseData> expireDelegationToken(
        ControllerRequestContext context,
        ExpireDelegationTokenRequestData requestData,
        MetadataVersion metadataVersion
    ) {
        long now = time.milliseconds();

        List<ApiMessageAndVersion> records = new ArrayList<>();
        ExpireDelegationTokenResponseData responseData = new ExpireDelegationTokenResponseData();

        if (secretKeyString == null) {
            // DelegationTokens are not enabled
            return ControllerResult.atomicOf(records, responseData.setErrorCode(DELEGATION_TOKEN_AUTH_DISABLED.code()));
        }

        TokenInformation myTokenInformation = getToken(requestData.hmac());

        if (myTokenInformation == null) {
            return ControllerResult.atomicOf(records, responseData.setErrorCode(DELEGATION_TOKEN_NOT_FOUND.code()));
        }

        if (myTokenInformation.maxTimestamp() < now || myTokenInformation.expiryTimestamp() < now) {
            return ControllerResult.atomicOf(records, responseData.setErrorCode(DELEGATION_TOKEN_EXPIRED.code()));
        }

        if (!allowedToRenew(myTokenInformation, context.principal())) {
            return ControllerResult.atomicOf(records, responseData.setErrorCode(DELEGATION_TOKEN_OWNER_MISMATCH.code()));
        }

        if (requestData.expiryTimePeriodMs() < 0) { // expire immediately
            responseData
                .setErrorCode(NONE.code())
                .setExpiryTimestampMs(requestData.expiryTimePeriodMs());
            records.add(new ApiMessageAndVersion(new RemoveDelegationTokenRecord().
                setTokenId(myTokenInformation.tokenId()), (short) 0));
        } else {
            long expiryTimestamp = Math.min(myTokenInformation.maxTimestamp(),
                now + requestData.expiryTimePeriodMs());

            responseData
                .setErrorCode(NONE.code())
                .setExpiryTimestampMs(expiryTimestamp);

            myTokenInformation.setExpiryTimestamp(expiryTimestamp);

            DelegationTokenData newDelegationTokenData = new DelegationTokenData(myTokenInformation);
            records.add(new ApiMessageAndVersion(newDelegationTokenData.toRecord(), (short) 0));
        }

        return ControllerResult.atomicOf(records, responseData);
    }

    // Periodic call to remove expired DelegationTokens
    public List<ApiMessageAndVersion> expireDelegationTokens() {
        long now = time.milliseconds();
        List<ApiMessageAndVersion> records = new ArrayList<ApiMessageAndVersion>();

        for (TokenInformation oldTokenInformation: tokenCache.tokens()) {
            if ((oldTokenInformation.maxTimestamp() < now) ||
                (oldTokenInformation.expiryTimestamp() < now)) {
                System.out.println("Token: " + oldTokenInformation.tokenId() + " is expired");
                records.add(new ApiMessageAndVersion(new RemoveDelegationTokenRecord().
                    setTokenId(oldTokenInformation.tokenId()), (short) 0));
            }
        }
        return records;
    }

    public void replay(DelegationTokenRecord record) {
        log.info("Replayed DelegationTokenRecord for {} " + record.tokenId());
    }

    public void replay(RemoveDelegationTokenRecord record) {
        log.info("Replayed RemoveDelegationTokenRecord for {} " + record.tokenId());
    }
}
