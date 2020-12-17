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
 */
package kafka.zookeeper

import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentLinkedQueue, CountDownLatch, Executors, Semaphore, TimeUnit}

import scala.collection.Seq
import com.yammer.metrics.core.{Gauge, Meter, MetricName}
import kafka.server.KafkaConfig
import kafka.metrics.KafkaYammerMetrics
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.KeeperException.{Code, NoNodeException}
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.ZooKeeper.States
import org.apache.zookeeper.client.ZKClientConfig
import org.apache.zookeeper.{CreateMode, WatchedEvent, ZooDefs}
import org.junit.Assert.{assertArrayEquals, assertEquals, assertFalse, assertTrue}
import org.junit.{After, Before, Test}
import org.scalatest.Assertions.{fail, intercept}

import scala.jdk.CollectionConverters._

class ZooKeeperClientTest extends ZooKeeperTestHarness {
  private val mockPath = "/foo"
  private val time = Time.SYSTEM

  private var zooKeeperClient: ZooKeeperClient = _

  @Before
  override def setUp(): Unit = {
    ZooKeeperTestHarness.verifyNoUnexpectedThreads("@Before")
    cleanMetricsRegistry()
    super.setUp()
    zooKeeperClient = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, zkMaxInFlightRequests,
      Time.SYSTEM, "testMetricGroup", "testMetricType")
  }

  @After
  override def tearDown(): Unit = {
    if (zooKeeperClient != null)
      zooKeeperClient.close()
    super.tearDown()
    System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    ZooKeeperTestHarness.verifyNoUnexpectedThreads("@After")
  }

  @Test
  def testUnresolvableConnectString(): Unit = {
    try {
      new ZooKeeperClient("some.invalid.hostname.foo.bar.local", zkSessionTimeout, connectionTimeoutMs = 10,
        Int.MaxValue, time, "testMetricGroup", "testMetricType")
    } catch {
      case e: ZooKeeperClientTimeoutException =>
        assertEquals("ZooKeeper client threads still running", Set.empty,  runningZkSendThreads)
    }
  }

  private def runningZkSendThreads: collection.Set[String] = Thread.getAllStackTraces.keySet.asScala
    .filter(_.isAlive)
    .map(_.getName)
    .filter(t => t.contains("SendThread()"))

  @Test(expected = classOf[ZooKeeperClientTimeoutException])
  def testConnectionTimeout(): Unit = {
    zookeeper.shutdown()
    new ZooKeeperClient(zkConnect, zkSessionTimeout, connectionTimeoutMs = 10, Int.MaxValue, time, "testMetricGroup",
      "testMetricType").close()
  }

  @Test
  def testConnection(): Unit = {
    val client = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, Int.MaxValue, time, "testMetricGroup",
      "testMetricType")
    try {
      // Verify ZooKeeper event thread name. This is used in ZooKeeperTestHarness to verify that tests have closed ZK clients
      val threads = Thread.getAllStackTraces.keySet.asScala.map(_.getName)
      assertTrue(s"ZooKeeperClient event thread not found, threads=$threads",
        threads.exists(_.contains(ZooKeeperTestHarness.ZkClientEventThreadSuffix)))
    } finally {
      client.close()
    }
  }

  @Test
  def testConnectionViaNettyClient(): Unit = {
    // Confirm that we can explicitly set client connection configuration, which is necessary for TLS.
    // TLS connectivity itself is tested in system tests rather than here to avoid having to add TLS support
    // to kafka.zk.EmbeddedZoopeeper
    val clientConfig = new ZKClientConfig()
    val propKey = KafkaConfig.ZkClientCnxnSocketProp
    val propVal = "org.apache.zookeeper.ClientCnxnSocketNetty"
    KafkaConfig.setZooKeeperClientProperty(clientConfig, propKey, propVal)
    val client = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, Int.MaxValue, time, "testMetricGroup",
      "testMetricType", None, Some(clientConfig))
    try {
      assertEquals(Some(propVal), KafkaConfig.getZooKeeperClientProperty(client.getClientConfig, propKey))
      // For a sanity check, make sure a bad client connection socket class name generates an exception
      val badClientConfig = new ZKClientConfig()
      KafkaConfig.setZooKeeperClientProperty(badClientConfig, propKey, propVal + "BadClassName")
      intercept[Exception] {
          new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, Int.MaxValue, time, "testMetricGroup",
            "testMetricType", None, Some(badClientConfig))
      }
    } finally {
      client.close()
    }
  }

  @Test
  def testDeleteNonExistentZNode(): Unit = {
    val deleteResponse = zooKeeperClient.handleRequest(DeleteRequest(mockPath, -1))
    assertEquals("Response code should be NONODE", Code.NONODE, deleteResponse.resultCode)
    intercept[NoNodeException] {
      deleteResponse.maybeThrow()
    }
  }

  @Test
  def testDeleteExistingZNode(): Unit = {
    val createResponse = zooKeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    val deleteResponse = zooKeeperClient.handleRequest(DeleteRequest(mockPath, -1))
    assertEquals("Response code for delete should be OK", Code.OK, deleteResponse.resultCode)
  }

  @Test
  def testExistsNonExistentZNode(): Unit = {
    val existsResponse = zooKeeperClient.handleRequest(ExistsRequest(mockPath))
    assertEquals("Response code should be NONODE", Code.NONODE, existsResponse.resultCode)
  }

  @Test
  def testExistsExistingZNode(): Unit = {
    val createResponse = zooKeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    val existsResponse = zooKeeperClient.handleRequest(ExistsRequest(mockPath))
    assertEquals("Response code for exists should be OK", Code.OK, existsResponse.resultCode)
  }

  @Test
  def testGetDataNonExistentZNode(): Unit = {
    val getDataResponse = zooKeeperClient.handleRequest(GetDataRequest(mockPath))
    assertEquals("Response code should be NONODE", Code.NONODE, getDataResponse.resultCode)
  }

  @Test
  def testGetDataExistingZNode(): Unit = {
    val data = bytes
    val createResponse = zooKeeperClient.handleRequest(CreateRequest(mockPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala,
      CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    val getDataResponse = zooKeeperClient.handleRequest(GetDataRequest(mockPath))
    assertEquals("Response code for getData should be OK", Code.OK, getDataResponse.resultCode)
    assertArrayEquals("Data for getData should match created znode data", data, getDataResponse.data)
  }

  @Test
  def testSetDataNonExistentZNode(): Unit = {
    val setDataResponse = zooKeeperClient.handleRequest(SetDataRequest(mockPath, Array.empty[Byte], -1))
    assertEquals("Response code should be NONODE", Code.NONODE, setDataResponse.resultCode)
  }

  @Test
  def testSetDataExistingZNode(): Unit = {
    val data = bytes
    val createResponse = zooKeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    val setDataResponse = zooKeeperClient.handleRequest(SetDataRequest(mockPath, data, -1))
    assertEquals("Response code for setData should be OK", Code.OK, setDataResponse.resultCode)
    val getDataResponse = zooKeeperClient.handleRequest(GetDataRequest(mockPath))
    assertEquals("Response code for getData should be OK", Code.OK, getDataResponse.resultCode)
    assertArrayEquals("Data for getData should match setData's data", data, getDataResponse.data)
  }

  @Test
  def testGetAclNonExistentZNode(): Unit = {
    val getAclResponse = zooKeeperClient.handleRequest(GetAclRequest(mockPath))
    assertEquals("Response code should be NONODE", Code.NONODE, getAclResponse.resultCode)
  }

  @Test
  def testGetAclExistingZNode(): Unit = {
    val createResponse = zooKeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    val getAclResponse = zooKeeperClient.handleRequest(GetAclRequest(mockPath))
    assertEquals("Response code for getAcl should be OK", Code.OK, getAclResponse.resultCode)
    assertEquals("ACL should be " + ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, getAclResponse.acl)
  }

  @Test
  def testSetAclNonExistentZNode(): Unit = {
    val setAclResponse = zooKeeperClient.handleRequest(SetAclRequest(mockPath, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, -1))
    assertEquals("Response code should be NONODE", Code.NONODE, setAclResponse.resultCode)
  }

  @Test
  def testGetChildrenNonExistentZNode(): Unit = {
    val getChildrenResponse = zooKeeperClient.handleRequest(GetChildrenRequest(mockPath, registerWatch = true))
    assertEquals("Response code should be NONODE", Code.NONODE, getChildrenResponse.resultCode)
  }

  @Test
  def testGetChildrenExistingZNode(): Unit = {
    val createResponse = zooKeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    val getChildrenResponse = zooKeeperClient.handleRequest(GetChildrenRequest(mockPath, registerWatch = true))
    assertEquals("Response code for getChildren should be OK", Code.OK, getChildrenResponse.resultCode)
    assertEquals("getChildren should return no children", Seq.empty[String], getChildrenResponse.children)
  }

  @Test
  def testGetChildrenExistingZNodeWithChildren(): Unit = {
    val child1 = "child1"
    val child2 = "child2"
    val child1Path = mockPath + "/" + child1
    val child2Path = mockPath + "/" + child2
    val createResponse = zooKeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    val createResponseChild1 = zooKeeperClient.handleRequest(CreateRequest(child1Path, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create child1 should be OK", Code.OK, createResponseChild1.resultCode)
    val createResponseChild2 = zooKeeperClient.handleRequest(CreateRequest(child2Path, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create child2 should be OK", Code.OK, createResponseChild2.resultCode)

    val getChildrenResponse = zooKeeperClient.handleRequest(GetChildrenRequest(mockPath, registerWatch = true))
    assertEquals("Response code for getChildren should be OK", Code.OK, getChildrenResponse.resultCode)
    assertEquals("getChildren should return two children", Seq(child1, child2), getChildrenResponse.children.sorted)
  }

  @Test
  def testPipelinedGetData(): Unit = {
    val createRequests = (1 to 3).map(x => CreateRequest("/" + x, (x * 2).toString.getBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    val createResponses = createRequests.map(zooKeeperClient.handleRequest)
    createResponses.foreach(createResponse => assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode))
    val getDataRequests = (1 to 3).map(x => GetDataRequest("/" + x))
    val getDataResponses = zooKeeperClient.handleRequests(getDataRequests)
    getDataResponses.foreach(getDataResponse => assertEquals("Response code for getData should be OK", Code.OK,
      getDataResponse.resultCode))
    getDataResponses.zipWithIndex.foreach { case (getDataResponse, i) =>
      assertEquals("Response code for getData should be OK", Code.OK, getDataResponse.resultCode)
      assertEquals("Data for getData should match", ((i + 1) * 2), Integer.valueOf(new String(getDataResponse.data)))
    }
  }

  @Test
  def testMixedPipeline(): Unit = {
    val createResponse = zooKeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    val getDataRequest = GetDataRequest(mockPath)
    val setDataRequest = SetDataRequest("/nonexistent", Array.empty[Byte], -1)
    val responses = zooKeeperClient.handleRequests(Seq(getDataRequest, setDataRequest))
    assertEquals("Response code for getData should be OK", Code.OK, responses.head.resultCode)
    assertArrayEquals("Data for getData should be empty", Array.empty[Byte], responses.head.asInstanceOf[GetDataResponse].data)
    assertEquals("Response code for setData should be NONODE", Code.NONODE, responses.last.resultCode)
  }

  @Test
  def testZNodeChangeHandlerForCreation(): Unit = {
    val znodeChangeHandlerCountDownLatch = new CountDownLatch(1)
    val zNodeChangeHandler = new ZNodeChangeHandler {
      override def handleCreation(): Unit = {
        znodeChangeHandlerCountDownLatch.countDown()
      }
      override val path: String = mockPath
    }

    zooKeeperClient.registerZNodeChangeHandler(zNodeChangeHandler)
    val existsRequest = ExistsRequest(mockPath)
    val createRequest = CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT)
    val responses = zooKeeperClient.handleRequests(Seq(existsRequest, createRequest))
    assertEquals("Response code for exists should be NONODE", Code.NONODE, responses.head.resultCode)
    assertEquals("Response code for create should be OK", Code.OK, responses.last.resultCode)
    assertTrue("Failed to receive create notification", znodeChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS))
  }

  @Test
  def testZNodeChangeHandlerForDeletion(): Unit = {
    val znodeChangeHandlerCountDownLatch = new CountDownLatch(1)
    val zNodeChangeHandler = new ZNodeChangeHandler {
      override def handleDeletion(): Unit = {
        znodeChangeHandlerCountDownLatch.countDown()
      }
      override val path: String = mockPath
    }

    zooKeeperClient.registerZNodeChangeHandler(zNodeChangeHandler)
    val existsRequest = ExistsRequest(mockPath)
    val createRequest = CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT)
    val responses = zooKeeperClient.handleRequests(Seq(createRequest, existsRequest))
    assertEquals("Response code for create should be OK", Code.OK, responses.last.resultCode)
    assertEquals("Response code for exists should be OK", Code.OK, responses.head.resultCode)
    val deleteResponse = zooKeeperClient.handleRequest(DeleteRequest(mockPath, -1))
    assertEquals("Response code for delete should be OK", Code.OK, deleteResponse.resultCode)
    assertTrue("Failed to receive delete notification", znodeChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS))
  }

  @Test
  def testZNodeChangeHandlerForDataChange(): Unit = {
    val znodeChangeHandlerCountDownLatch = new CountDownLatch(1)
    val zNodeChangeHandler = new ZNodeChangeHandler {
      override def handleDataChange(): Unit = {
        znodeChangeHandlerCountDownLatch.countDown()
      }
      override val path: String = mockPath
    }

    zooKeeperClient.registerZNodeChangeHandler(zNodeChangeHandler)
    val existsRequest = ExistsRequest(mockPath)
    val createRequest = CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT)
    val responses = zooKeeperClient.handleRequests(Seq(createRequest, existsRequest))
    assertEquals("Response code for create should be OK", Code.OK, responses.last.resultCode)
    assertEquals("Response code for exists should be OK", Code.OK, responses.head.resultCode)
    val setDataResponse = zooKeeperClient.handleRequest(SetDataRequest(mockPath, Array.empty[Byte], -1))
    assertEquals("Response code for setData should be OK", Code.OK, setDataResponse.resultCode)
    assertTrue("Failed to receive data change notification", znodeChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS))
  }

  @Test
  def testBlockOnRequestCompletionFromStateChangeHandler(): Unit = {
    // This tests the scenario exposed by KAFKA-6879 in which the expiration callback awaits
    // completion of a request which is handled by another thread

    val latch = new CountDownLatch(1)
    val stateChangeHandler = new StateChangeHandler {
      override val name = this.getClass.getName
      override def beforeInitializingSession(): Unit = {
        latch.await()
      }
    }

    zooKeeperClient.close()
    zooKeeperClient = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, Int.MaxValue, time,
      "testMetricGroup", "testMetricType")
    zooKeeperClient.registerStateChangeHandler(stateChangeHandler)

    val requestThread = new Thread() {
      override def run(): Unit = {
        try
          zooKeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
            ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
        finally
          latch.countDown()
      }
    }

    val reinitializeThread = new Thread() {
      override def run(): Unit = {
        zooKeeperClient.forceReinitialize()
      }
    }

    reinitializeThread.start()

    // sleep briefly before starting the request thread so that the initialization
    // thread is blocking on the latch
    Thread.sleep(100)
    requestThread.start()

    reinitializeThread.join()
    requestThread.join()
  }

  @Test
  def testExceptionInBeforeInitializingSession(): Unit = {
    val faultyHandler = new StateChangeHandler {
      override val name = this.getClass.getName
      override def beforeInitializingSession(): Unit = {
        throw new RuntimeException()
      }
    }

    val goodCalls = new AtomicInteger(0)
    val goodHandler = new StateChangeHandler {
      override val name = this.getClass.getName
      override def beforeInitializingSession(): Unit = {
        goodCalls.incrementAndGet()
      }
    }

    zooKeeperClient.close()
    zooKeeperClient = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, Int.MaxValue, time,
      "testMetricGroup", "testMetricType")
    zooKeeperClient.registerStateChangeHandler(faultyHandler)
    zooKeeperClient.registerStateChangeHandler(goodHandler)

    zooKeeperClient.forceReinitialize()

    assertEquals(1, goodCalls.get)

    // Client should be usable even if the callback throws an error
    val createResponse = zooKeeperClient.handleRequest(CreateRequest(mockPath, Array.empty[Byte],
      ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
  }

  @Test
  def testZNodeChildChangeHandlerForChildChange(): Unit = {
    val zNodeChildChangeHandlerCountDownLatch = new CountDownLatch(1)
    val zNodeChildChangeHandler = new ZNodeChildChangeHandler {
      override def handleChildChange(): Unit = {
        zNodeChildChangeHandlerCountDownLatch.countDown()
      }
      override val path: String = mockPath
    }

    val child1 = "child1"
    val child1Path = mockPath + "/" + child1
    val createResponse = zooKeeperClient.handleRequest(
      CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    zooKeeperClient.registerZNodeChildChangeHandler(zNodeChildChangeHandler)
    val getChildrenResponse = zooKeeperClient.handleRequest(GetChildrenRequest(mockPath, registerWatch = true))
    assertEquals("Response code for getChildren should be OK", Code.OK, getChildrenResponse.resultCode)
    val createResponseChild1 = zooKeeperClient.handleRequest(
      CreateRequest(child1Path, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create child1 should be OK", Code.OK, createResponseChild1.resultCode)
    assertTrue("Failed to receive child change notification",
      zNodeChildChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS))
  }

  @Test
  def testZNodeChildChangeHandlerForChildChangeNotTriggered(): Unit = {
    val zNodeChildChangeHandlerCountDownLatch = new CountDownLatch(1)
    val zNodeChildChangeHandler = new ZNodeChildChangeHandler {
      override def handleChildChange(): Unit = {
        zNodeChildChangeHandlerCountDownLatch.countDown()
      }
      override val path: String = mockPath
    }

    val child1 = "child1"
    val child1Path = mockPath + "/" + child1
    val createResponse = zooKeeperClient.handleRequest(
      CreateRequest(mockPath, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create should be OK", Code.OK, createResponse.resultCode)
    zooKeeperClient.registerZNodeChildChangeHandler(zNodeChildChangeHandler)
    val getChildrenResponse = zooKeeperClient.handleRequest(GetChildrenRequest(mockPath, registerWatch = false))
    assertEquals("Response code for getChildren should be OK", Code.OK, getChildrenResponse.resultCode)
    val createResponseChild1 = zooKeeperClient.handleRequest(
      CreateRequest(child1Path, Array.empty[Byte], ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala, CreateMode.PERSISTENT))
    assertEquals("Response code for create child1 should be OK", Code.OK, createResponseChild1.resultCode)
    assertFalse("Child change notification received",
      zNodeChildChangeHandlerCountDownLatch.await(100, TimeUnit.MILLISECONDS))
  }

  @Test
  def testStateChangeHandlerForAuthFailure(): Unit = {
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, "no-such-file-exists.conf")
    val stateChangeHandlerCountDownLatch = new CountDownLatch(1)
    val stateChangeHandler = new StateChangeHandler {
      override val name: String =  this.getClass.getName

      override def onAuthFailure(): Unit = {
        stateChangeHandlerCountDownLatch.countDown()
      }
    }

    val zooKeeperClient = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, Int.MaxValue, time,
      "testMetricGroup", "testMetricType")
    try {
      zooKeeperClient.registerStateChangeHandler(stateChangeHandler)
      zooKeeperClient.forceReinitialize()

      assertTrue("Failed to receive auth failed notification", stateChangeHandlerCountDownLatch.await(5, TimeUnit.SECONDS))
    } finally zooKeeperClient.close()
  }

  @Test
  def testConnectionLossRequestTermination(): Unit = {
    val batchSize = 10
    val zooKeeperClient = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, 2, time,
      "testGroupType", "testGroupName")
    zookeeper.shutdown()
    try {
      val requests = (1 to batchSize).map(i => GetDataRequest(s"/$i"))
      val countDownLatch = new CountDownLatch(1)
      val running = new AtomicBoolean(true)
      val unexpectedResponses = new ArrayBlockingQueue[GetDataResponse](batchSize)
      val requestThread = new Thread {
        override def run(): Unit = {
          while (running.get()) {
            val responses = zooKeeperClient.handleRequests(requests)
            val suffix = responses.dropWhile(response => response.resultCode != Code.CONNECTIONLOSS)
            if (!suffix.forall(response => response.resultCode == Code.CONNECTIONLOSS))
              responses.foreach(unexpectedResponses.add)
            if (!unexpectedResponses.isEmpty || suffix.nonEmpty)
              running.set(false)
          }
          countDownLatch.countDown()
        }
      }
      requestThread.start()
      val requestThreadTerminated = countDownLatch.await(30, TimeUnit.SECONDS)
      if (!requestThreadTerminated) {
        running.set(false)
        requestThread.join(5000)
        fail("Failed to receive a CONNECTIONLOSS response code after zookeeper has shutdown.")
      } else if (!unexpectedResponses.isEmpty) {
        fail(s"Received an unexpected non-CONNECTIONLOSS response code after a CONNECTIONLOSS response code from a single batch: $unexpectedResponses")
      }
    } finally zooKeeperClient.close()
  }

  /**
    * Tests that if session expiry notification is received while a thread is processing requests,
    * session expiry is handled and the request thread completes with responses to all requests,
    * even though some requests may fail due to session expiry or disconnection.
    *
    * Sequence of events on different threads:
    *   Request thread:
    *       - Sends `maxInflightRequests` requests (these may complete before session is expired)
    *   Main thread:
    *       - Waits for at least one request to be processed (this should succeed)
    *       - Expires session by creating new client with same session id
    *       - Unblocks another `maxInflightRequests` requests before and after new client is closed (these may fail)
    *   ZooKeeperClient Event thread:
    *       - Delivers responses and session expiry (no ordering guarantee between these, both are processed asynchronously)
    *   Response executor thread:
    *       - Blocks subsequent sends by delaying response until session expiry is processed
    *   ZooKeeperClient Session Expiry Handler:
    *       - Unblocks subsequent sends
    *   Main thread:
    *       - Waits for all sends to complete. The requests sent after session expiry processing should succeed.
    */
  @Test
  def testSessionExpiry(): Unit = {
    val maxInflightRequests = 2
    val responseExecutor = Executors.newSingleThreadExecutor
    val sendSemaphore = new Semaphore(0)
    val sendCompleteSemaphore = new Semaphore(0)
    val sendSize = maxInflightRequests * 5
    @volatile var resultCodes: Seq[Code] = null
    val stateChanges = new ConcurrentLinkedQueue[String]()
    val zooKeeperClient = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, maxInflightRequests,
      time, "testGroupType", "testGroupName") {
      override def send[Req <: AsyncRequest](request: Req)(processResponse: Req#Response => Unit): Unit = {
        super.send(request)( response => {
          responseExecutor.submit(new Runnable {
            override def run(): Unit = {
              sendCompleteSemaphore.release()
              sendSemaphore.acquire()
              processResponse(response)
            }
          })
        })
      }
    }
    try {
      zooKeeperClient.registerStateChangeHandler(new StateChangeHandler {
        override val name: String ="test-state-change-handler"
        override def afterInitializingSession(): Unit = {
          verifyHandlerThread()
          stateChanges.add("afterInitializingSession")
        }
        override def beforeInitializingSession(): Unit = {
          verifyHandlerThread()
          stateChanges.add("beforeInitializingSession")
          sendSemaphore.release(sendSize) // Resume remaining sends
        }
        private def verifyHandlerThread(): Unit = {
          val threadName = Thread.currentThread.getName
          assertTrue(s"Unexpected thread + $threadName", threadName.startsWith(zooKeeperClient.reinitializeScheduler.threadNamePrefix))
        }
      })

      val requestThread = new Thread {
        override def run(): Unit = {
          val requests = (1 to sendSize).map(i => GetDataRequest(s"/$i"))
          resultCodes = zooKeeperClient.handleRequests(requests).map(_.resultCode)
        }
      }
      requestThread.start()
      sendCompleteSemaphore.acquire() // Wait for request thread to start processing requests

      val anotherZkClient = createZooKeeperClientToTriggerSessionExpiry(zooKeeperClient.currentZooKeeper)
      sendSemaphore.release(maxInflightRequests) // Resume a few more sends which may fail
      anotherZkClient.close()
      sendSemaphore.release(maxInflightRequests) // Resume a few more sends which may fail

      requestThread.join(10000)
      if (requestThread.isAlive) {
        requestThread.interrupt()
        fail("Request thread did not complete")
      }
      assertEquals(Seq("beforeInitializingSession", "afterInitializingSession"), stateChanges.asScala.toSeq)

      assertEquals(resultCodes.size, sendSize)
      val connectionLostCount = resultCodes.count(_ == Code.CONNECTIONLOSS)
      assertTrue(s"Unexpected connection lost requests $resultCodes", connectionLostCount <= maxInflightRequests)
      val expiredCount = resultCodes.count(_ == Code.SESSIONEXPIRED)
      assertTrue(s"Unexpected session expired requests $resultCodes", expiredCount <= maxInflightRequests)
      assertTrue(s"No connection lost or expired requests $resultCodes", connectionLostCount + expiredCount > 0)
      assertEquals(Code.NONODE, resultCodes.head)
      assertEquals(Code.NONODE, resultCodes.last)
      assertTrue(s"Unexpected result code $resultCodes",
        resultCodes.filterNot(Set(Code.NONODE, Code.SESSIONEXPIRED, Code.CONNECTIONLOSS).contains).isEmpty)

    } finally {
      zooKeeperClient.close()
      responseExecutor.shutdownNow()
    }
    assertFalse("Expiry executor not shutdown", zooKeeperClient.reinitializeScheduler.isStarted)
  }

  @Test
  def testSessionExpiryDuringClose(): Unit = {
    val semaphore = new Semaphore(0)
    val closeExecutor = Executors.newSingleThreadExecutor
    try {
      zooKeeperClient.reinitializeScheduler.schedule("test", () => semaphore.acquireUninterruptibly())
      zooKeeperClient.scheduleReinitialize("session-expired", "Session expired.", delayMs = 0L)
      val closeFuture = closeExecutor.submit(new Runnable {
        override def run(): Unit = {
          zooKeeperClient.close()
        }
      })
      assertFalse("Close completed without shutting down expiry scheduler gracefully", closeFuture.isDone)
      assertTrue(zooKeeperClient.currentZooKeeper.getState.isAlive) // Client should be closed after expiry handler
      semaphore.release()
      closeFuture.get(10, TimeUnit.SECONDS)
      assertFalse("Expiry executor not shutdown", zooKeeperClient.reinitializeScheduler.isStarted)
    } finally {
      closeExecutor.shutdownNow()
    }
  }

  @Test
  def testReinitializeAfterAuthFailure(): Unit = {
    val sessionInitializedCountDownLatch = new CountDownLatch(1)
    val changeHandler = new StateChangeHandler {
      override val name = this.getClass.getName
      override def beforeInitializingSession(): Unit = {
        sessionInitializedCountDownLatch.countDown()
      }
    }

    zooKeeperClient.close()
    zooKeeperClient = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, Int.MaxValue, time,
      "testMetricGroup", "testMetricType")
    zooKeeperClient.registerStateChangeHandler(changeHandler)

    zooKeeperClient.ZooKeeperClientWatcher.process(new WatchedEvent(EventType.None, KeeperState.AuthFailed, null))
    assertTrue("Failed to receive session initializing notification", sessionInitializedCountDownLatch.await(5, TimeUnit.SECONDS))
  }

  def isExpectedMetricName(metricName: MetricName, name: String): Boolean =
    metricName.getName == name && metricName.getGroup == "testMetricGroup" && metricName.getType == "testMetricType"

  @Test
  def testZooKeeperStateChangeRateMetrics(): Unit = {
    def checkMeterCount(name: String, expected: Long): Unit = {
      val meter = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.collectFirst {
        case (metricName, meter: Meter) if isExpectedMetricName(metricName, name) => meter
      }.getOrElse(sys.error(s"Unable to find meter with name $name"))
      assertEquals(s"Unexpected meter count for $name", expected, meter.count)
    }

    val expiresPerSecName = "ZooKeeperExpiresPerSec"
    val disconnectsPerSecName = "ZooKeeperDisconnectsPerSec"
    checkMeterCount(expiresPerSecName, 0)
    checkMeterCount(disconnectsPerSecName, 0)

    zooKeeperClient.ZooKeeperClientWatcher.process(new WatchedEvent(EventType.None, KeeperState.Expired, null))
    checkMeterCount(expiresPerSecName, 1)
    checkMeterCount(disconnectsPerSecName, 0)

    zooKeeperClient.ZooKeeperClientWatcher.process(new WatchedEvent(EventType.None, KeeperState.Disconnected, null))
    checkMeterCount(expiresPerSecName, 1)
    checkMeterCount(disconnectsPerSecName, 1)
  }

  @Test
  def testZooKeeperSessionStateMetric(): Unit = {
    def gaugeValue(name: String): Option[String] = {
      KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.collectFirst {
        case (metricName, gauge: Gauge[_]) if isExpectedMetricName(metricName, name) => gauge.value.asInstanceOf[String]
      }
    }

    assertEquals(Some(States.CONNECTED.toString), gaugeValue("SessionState"))
    assertEquals(States.CONNECTED, zooKeeperClient.connectionState)

    zooKeeperClient.close()

    assertEquals(None, gaugeValue("SessionState"))
    assertEquals(States.CLOSED, zooKeeperClient.connectionState)
  }

  private def cleanMetricsRegistry(): Unit = {
    val metrics = KafkaYammerMetrics.defaultRegistry
    metrics.allMetrics.keySet.forEach(metrics.removeMetric)
  }

  private def bytes = UUID.randomUUID().toString.getBytes(StandardCharsets.UTF_8)
}
