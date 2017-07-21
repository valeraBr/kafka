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
package kafka.common

import org.junit.{Assert, Test}
import kafka.utils.MockTime
import org.apache.kafka.clients.{ClientRequest, ClientResponse, NetworkClient, RequestCompletionHandler}
import org.apache.kafka.common.Node
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.utils.Utils
import org.easymock.EasyMock

import scala.collection.mutable

class InterBrokerSendThreadTest {
  private val time = new MockTime()
  private val networkClient = EasyMock.createMock(classOf[NetworkClient])
  private val completionHandler = new StubCompletionHandler

  @Test
  def shouldNotSendAnythingWhenNoRequests(): Unit = {
    val sendThread = new InterBrokerSendThread("name", networkClient, time) {
      override def generateRequests() = mutable.Iterable.empty
    }

    // poll is always called but there should be no further invocations on NetworkClient
    EasyMock.expect(networkClient.poll(EasyMock.anyLong(), EasyMock.anyLong()))
    .andReturn(Utils.mkList())

    EasyMock.replay(networkClient)

    sendThread.doWork()

    EasyMock.verify(networkClient)
  }

  @Test
  def shouldCreateClientRequestAndSendWhenNodeIsReady(): Unit = {
    val request = new StubRequestBuilder()
    val node = new Node(1, "", 8080)
    val handler = RequestAndCompletionHandler(node, request, completionHandler)
    val sendThread = new InterBrokerSendThread("name", networkClient, time) {
      override def generateRequests() = List[RequestAndCompletionHandler](handler)
    }

    val clientRequest = new ClientRequest("dest", request, 0, "1", 0, 10, true, handler.handler)

    EasyMock.expect(networkClient.newClientRequest(EasyMock.eq("1"),
      EasyMock.same(handler.request),
      EasyMock.anyLong(),
      EasyMock.eq(true),
      EasyMock.same(handler.handler)))
    .andReturn(clientRequest)

    EasyMock.expect(networkClient.ready(node, time.milliseconds()))
    .andReturn(true)

    EasyMock.expect(networkClient.send(clientRequest, time.milliseconds()))

    EasyMock.expect(networkClient.poll(EasyMock.anyLong(), EasyMock.anyLong()))
      .andReturn(Utils.mkList())

    EasyMock.replay(networkClient)

    sendThread.doWork()

    EasyMock.verify(networkClient)
  }


  @Test
  def shouldCallCompletionHandlerWithDisconnectedResponseWhenNodeNotReady(): Unit = {
    val request = new StubRequestBuilder
    val node = new Node(1, "", 8080)
    val requestAndCompletionHandler = RequestAndCompletionHandler(node, request, completionHandler)
    val sendThread = new InterBrokerSendThread("name", networkClient, time) {
      override def generateRequests() = List[RequestAndCompletionHandler](requestAndCompletionHandler)
    }

    val clientRequest = new ClientRequest("dest", request, 0, "1", 0, 10, true, requestAndCompletionHandler.handler)

    EasyMock.expect(networkClient.newClientRequest(EasyMock.eq("1"),
      EasyMock.same(requestAndCompletionHandler.request),
      EasyMock.anyLong(),
      EasyMock.eq(true),
      EasyMock.same(requestAndCompletionHandler.handler)))
      .andReturn(clientRequest)

    EasyMock.expect(networkClient.ready(node, time.milliseconds()))
      .andReturn(false)

    EasyMock.expect(networkClient.connectionDelay(EasyMock.anyObject(), EasyMock.anyLong()))
    .andReturn(0)

    EasyMock.expect(networkClient.poll(EasyMock.anyLong(), EasyMock.anyLong()))
      .andReturn(Utils.mkList())

    EasyMock.replay(networkClient)

    sendThread.doWork()

    EasyMock.verify(networkClient)
    Assert.assertTrue(completionHandler.response.wasDisconnected())
  }


  private class StubRequestBuilder extends AbstractRequest.Builder(ApiKeys.END_TXN) {
    override def build(version: Short): Nothing = ???
  }

  private class StubCompletionHandler extends RequestCompletionHandler {
    var response: ClientResponse = _
    override def onComplete(response: ClientResponse): Unit = {
      this.response = response
    }
  }

}
