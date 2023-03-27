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

import kafka.utils.Logging
import org.apache.kafka.clients.{ClientRequest, ClientResponse, KafkaClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.errors.{AuthenticationException, DisconnectException}
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.util.ShutdownableThread

import java.util.Map.Entry
import java.util.{ArrayDeque, ArrayList, Collection, Collections, HashMap, Iterator}
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

/**
 *  Class for inter-broker send thread that utilize a non-blocking network client.
 */
class GenericInterBrokerSendThread(
  name: String,
  @volatile var networkClient: KafkaClient,
  requestTimeoutMs: Int,
  time: Time,
  isInterruptible: Boolean = true
) extends ShutdownableThread(name, isInterruptible) with Logging {

  this.logIdent = logPrefix

  private val unsentRequests = new UnsentRequests2
  
  private val requestManagers = new ArrayList[InterBrokerRequestManager]()

  def hasUnsentRequests: Boolean = unsentRequests.iterator().hasNext
  
  def addRequestManager(requestManager: InterBrokerRequestManager): Unit = {
    requestManagers.add(requestManager)
  }

  override def shutdown(): Unit = {
    initiateShutdown()
    networkClient.initiateClose()
    awaitShutdown()
    networkClient.close()
  }

  private def drainGeneratedRequests(): Unit = {
    requestManagers.forEach { manager => 
      manager.generateRequests().foreach { request =>
        unsentRequests.put(request.destination,
          networkClient.newClientRequest(
            request.destination.idString,
            request.request,
            request.creationTimeMs,
            true,
            requestTimeoutMs,
            request.handler
          ),
          manager)
      }
    }
  }

  protected def pollOnce(maxTimeoutMs: Long): Unit = {
    try {
      drainGeneratedRequests()
      var now = time.milliseconds()
      val timeout = sendRequests(now, maxTimeoutMs)
      networkClient.poll(timeout, now)
      now = time.milliseconds()
      checkDisconnects(now)
      failExpiredRequests(now)
      unsentRequests.clean()
    } catch {
      case _: DisconnectException if !networkClient.active() =>
        // DisconnectException is expected when NetworkClient#initiateClose is called
      case e: FatalExitError => throw e
      case t: Throwable =>
        error(s"unhandled exception caught in InterBrokerSendThread", t)
        // rethrow any unhandled exceptions as FatalExitError so the JVM will be terminated
        // as we will be in an unknown state with potentially some requests dropped and not
        // being able to make progress. Known and expected Errors should have been appropriately
        // dealt with already.
        throw new FatalExitError()
    }
  }

  override def doWork(): Unit = {
    pollOnce(Long.MaxValue)
  }

  private def sendRequests(now: Long, maxTimeoutMs: Long): Long = {
    var pollTimeout = maxTimeoutMs
    for (node <- unsentRequests.nodes.asScala) {
      val requestIterator = unsentRequests.requestIterator(node)
      while (requestIterator.hasNext) {
        val requestInfo = requestIterator.next
        if (networkClient.ready(node, now) && requestInfo.requestManager.maybeIncrementInflightRequests()) {
          networkClient.send(requestInfo.request, now)
          requestIterator.remove()
        } else
          pollTimeout = Math.min(pollTimeout, networkClient.connectionDelay(node, now))
      }
    }
    pollTimeout
  }

  private def checkDisconnects(now: Long): Unit = {
    // any disconnects affecting requests that have already been transmitted will be handled
    // by NetworkClient, so we just need to check whether connections for any of the unsent
    // requests have been disconnected; if they have, then we complete the corresponding future
    // and set the disconnect flag in the ClientResponse
    val iterator = unsentRequests.iterator()
    while (iterator.hasNext) {
      val entry = iterator.next
      val (node, requests) = (entry.getKey, entry.getValue)
      if (!requests.isEmpty && networkClient.connectionFailed(node)) {
        iterator.remove()
        for (request <- requests.asScala) {
          val authenticationException = networkClient.authenticationException(node)
          if (authenticationException != null)
            error(s"Failed to send the following request due to authentication error: $request")
          completeWithDisconnect(request.request, now, authenticationException)
        }
      }
    }
  }

  private def failExpiredRequests(now: Long): Unit = {
    // clear all expired unsent requests
    val timedOutRequests = unsentRequests.removeAllTimedOut(now)
    for (request <- timedOutRequests.asScala) {
      debug(s"Failed to send the following request after ${request.requestTimeoutMs} ms: $request")
      completeWithDisconnect(request, now, null)
    }
  }

  def completeWithDisconnect(request: ClientRequest,
                             now: Long,
                             authenticationException: AuthenticationException): Unit = {
    val handler = request.callback
    handler.onComplete(new ClientResponse(request.makeHeader(request.requestBuilder().latestAllowedVersion()),
      handler, request.destination, now /* createdTimeMs */ , now /* receivedTimeMs */ , true /* disconnected */ ,
      null /* versionMismatch */ , authenticationException, null))
  }

  def wakeup(): Unit = networkClient.wakeup()
}


abstract class InterBrokerRequestManager(interBrokerSendThread: GenericInterBrokerSendThread, val maxInflightRequests: Int) {
  
  val inflightRequests = new AtomicInteger(0)
  
  def generateRequests(): Iterable[RequestAndCompletionHandler]

  def wakeup(): Unit = interBrokerSendThread.wakeup()
  
  def maybeIncrementInflightRequests(): Boolean = {
    inflightRequests.synchronized {
      val canSend = inflightRequests.get() < maxInflightRequests
      if (canSend)
        inflightRequests.incrementAndGet()
      canSend
    }
  }

}


case class RequestInfo(
  request: ClientRequest,
  requestManager: InterBrokerRequestManager
)

private class UnsentRequests2 {
  private val unsent = new HashMap[Node, ArrayDeque[RequestInfo]]

  def put(node: Node, request: ClientRequest, manager: InterBrokerRequestManager): Unit = {
    var requestInfo = unsent.get(node)
    if (requestInfo == null) {
      requestInfo = new ArrayDeque[RequestInfo]
      unsent.put(node, requestInfo)
    }
    requestInfo.add(new RequestInfo(request, manager))
  }

  def removeAllTimedOut(now: Long): Collection[ClientRequest] = {
    val expiredRequests = new ArrayList[ClientRequest]
    for (requests <- unsent.values.asScala) {
      val requestIterator = requests.iterator
      var foundExpiredRequest = false
      while (requestIterator.hasNext && !foundExpiredRequest) {
        val requestInfo = requestIterator.next
        val elapsedMs = Math.max(0, now - requestInfo.request.createdTimeMs)
        if (elapsedMs > requestInfo.request.requestTimeoutMs) {
          expiredRequests.add(requestInfo.request)
          requestInfo.requestManager.inflightRequests.decrementAndGet()
          requestIterator.remove()
          foundExpiredRequest = true
        }
      }
    }
    expiredRequests
  }

  def clean(): Unit = {
    val iterator = unsent.values.iterator
    while (iterator.hasNext) {
      val requests = iterator.next
      if (requests.isEmpty)
        iterator.remove()
    }
  }

  def iterator(): Iterator[Entry[Node, ArrayDeque[RequestInfo]]] = {
    unsent.entrySet().iterator()
  }

  def requestIterator(node: Node): Iterator[RequestInfo] = {
    val requests = unsent.get(node)
    if (requests == null)
      Collections.emptyIterator[RequestInfo]
    else
      requests.iterator
  }

  def nodes: java.util.Set[Node] = unsent.keySet
}