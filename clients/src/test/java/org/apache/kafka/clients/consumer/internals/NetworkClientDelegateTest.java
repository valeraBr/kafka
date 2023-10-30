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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_GROUP_ID;
import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_REQUEST_TIMEOUT_MS;
import static org.apache.kafka.clients.consumer.internals.ConsumerTestBuilder.DEFAULT_RETRY_BACKOFF_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NetworkClientDelegateTest {

    private Time time;
    private MockClient client;

    @BeforeEach
    public void setup() {
        this.time = new MockTime(0);
        this.client = new MockClient(time, Collections.singletonList(mockNode()));
    }

    @Test
    public void testSuccessfulResponse() throws Exception {
        try (NetworkClientDelegate ncd = newNetworkClientDelegate()) {
            NetworkClientDelegate.UnsentRequest unsentRequest = newUnsentFindCoordinatorRequest();
            prepareFindCoordinatorResponse(Errors.NONE);

            ncd.send(unsentRequest);
            ncd.poll(0, time.milliseconds());

            assertTrue(unsentRequest.future().isDone());
            assertNotNull(unsentRequest.future().get());
        }
    }

    @Test
    public void testTimeoutBeforeSend() throws Exception {
        try (NetworkClientDelegate ncd = newNetworkClientDelegate()) {
            client.setUnreachable(mockNode(), DEFAULT_REQUEST_TIMEOUT_MS);
            NetworkClientDelegate.UnsentRequest unsentRequest = newUnsentFindCoordinatorRequest();
            ncd.send(unsentRequest);
            ncd.poll(0, time.milliseconds());
            time.sleep(DEFAULT_REQUEST_TIMEOUT_MS);
            ncd.poll(0, time.milliseconds());
            assertTrue(unsentRequest.future().isDone());
            TestUtils.assertFutureThrows(unsentRequest.future(), TimeoutException.class);
        }
    }

    @Test
    public void testTimeoutAfterSend() throws Exception {
        try (NetworkClientDelegate ncd = newNetworkClientDelegate()) {
            NetworkClientDelegate.UnsentRequest unsentRequest = newUnsentFindCoordinatorRequest();
            ncd.send(unsentRequest);
            ncd.poll(0, time.milliseconds());
            time.sleep(DEFAULT_REQUEST_TIMEOUT_MS);
            ncd.poll(0, time.milliseconds());
            assertTrue(unsentRequest.future().isDone());
            TestUtils.assertFutureThrows(unsentRequest.future(), DisconnectException.class);
        }
    }

    @Test
    public void testEnsureCorrectCompletionTimeOnFailure() {
        NetworkClientDelegate.UnsentRequest unsentRequest = newUnsentFindCoordinatorRequest();
        long timeMs = time.milliseconds();
        unsentRequest.handler().onFailure(timeMs, new TimeoutException());

        time.sleep(100);
        assertEquals(timeMs, unsentRequest.handler().completionTimeMs());
    }

    @Test
    public void testEnsureCorrectCompletionTimeOnComplete() {
        NetworkClientDelegate.UnsentRequest unsentRequest = newUnsentFindCoordinatorRequest();
        long timeMs = time.milliseconds();
        final ClientResponse response = mock(ClientResponse.class);
        when(response.receivedTimeMs()).thenReturn(timeMs);
        unsentRequest.handler().onComplete(response);
        time.sleep(100);
        assertEquals(timeMs, unsentRequest.handler().completionTimeMs());
    }

    public NetworkClientDelegate newNetworkClientDelegate() {
        LogContext logContext = new LogContext();
        return new NetworkClientDelegate(
                logContext,
                time,
                client,
                DEFAULT_REQUEST_TIMEOUT_MS,
                DEFAULT_RETRY_BACKOFF_MS
        );
    }

    public NetworkClientDelegate.UnsentRequest newUnsentFindCoordinatorRequest() {
        Objects.requireNonNull(DEFAULT_GROUP_ID);
        NetworkClientDelegate.UnsentRequest req = new NetworkClientDelegate.UnsentRequest(
                new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
                    .setKey(DEFAULT_GROUP_ID)
                    .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())
                ),
            Optional.empty()
        );
        req.setTimer(this.time, DEFAULT_REQUEST_TIMEOUT_MS);
        return req;
    }

    public void prepareFindCoordinatorResponse(Errors error) {
        FindCoordinatorResponse findCoordinatorResponse =
            FindCoordinatorResponse.prepareResponse(error, DEFAULT_GROUP_ID, mockNode());
        client.prepareResponse(findCoordinatorResponse);
    }

    private Node mockNode() {
        return new Node(0, "localhost", 99);
    }
}
