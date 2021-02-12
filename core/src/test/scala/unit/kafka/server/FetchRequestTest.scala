/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server

import java.io.DataInputStream
import java.util
import java.util.{Optional, Properties}

import kafka.api.KAFKA_0_11_0_IV2
import kafka.log.LogConfig
import kafka.message.{GZIPCompressionCodec, ProducerCompressionCodec, ZStdCompressionCodec}
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{MemoryRecords, Record, RecordBatch}
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, FetchMetadata => JFetchMetadata}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.apache.kafka.common.{IsolationLevel, TopicPartition, Uuid}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}

import scala.jdk.CollectionConverters._
import scala.collection.Seq
import scala.util.Random

/**
  * Subclasses of `BaseConsumerTest` exercise the consumer and fetch request/response. This class
  * complements those classes with tests that require lower-level access to the protocol.
  */
class FetchRequestTest extends BaseRequestTest {

  private var producer: KafkaProducer[String, String] = null

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.FetchMaxBytes, Int.MaxValue.toString)
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (producer != null)
      producer.close()
    super.tearDown()
  }

  private def createFetchRequest(maxResponseBytes: Int, maxPartitionBytes: Int, topicPartitions: Seq[TopicPartition],
                                 offsetMap: Map[TopicPartition, Long],
                                 version: Short): FetchRequest = {
    val topicIds = getTopicIds().asJava
    FetchRequest.Builder.forConsumer(version, Int.MaxValue, 0, createPartitionMap(maxPartitionBytes, topicPartitions, offsetMap), topicIds)
      .setMaxBytes(maxResponseBytes).build()
  }

  private def createPartitionMap(maxPartitionBytes: Int, topicPartitions: Seq[TopicPartition],
                                 offsetMap: Map[TopicPartition, Long] = Map.empty): util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData] = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    topicPartitions.foreach { tp =>
      partitionMap.put(tp, new FetchRequest.PartitionData(offsetMap.getOrElse(tp, 0), 0L, maxPartitionBytes,
        Optional.empty()))
    }
    partitionMap
  }

  private def sendFetchRequest(leaderId: Int, request: FetchRequest): FetchResponse[MemoryRecords] = {
    connectAndReceive[FetchResponse[MemoryRecords]](request, destination = brokerSocketServer(leaderId))
  }

  private def initProducer(): Unit = {
    producer = TestUtils.createProducer(TestUtils.getBrokerListStrFromServers(servers),
      keySerializer = new StringSerializer, valueSerializer = new StringSerializer)
  }

  @Test
  def testBrokerRespectsPartitionsOrderAndSizeLimits(): Unit = {
    initProducer()

    val messagesPerPartition = 9
    val maxResponseBytes = 800
    val maxPartitionBytes = 190

    def createFetchRequest(topicPartitions: Seq[TopicPartition], offsetMap: Map[TopicPartition, Long] = Map.empty,
                           version: Short = ApiKeys.FETCH.latestVersion()): FetchRequest =
      this.createFetchRequest(maxResponseBytes, maxPartitionBytes, topicPartitions, offsetMap, version)

    val topicPartitionToLeader = createTopics(numTopics = 5, numPartitions = 6)
    val random = new Random(0)
    val topicPartitions = topicPartitionToLeader.keySet
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava
    produceData(topicPartitions, messagesPerPartition)

    val leaderId = servers.head.config.brokerId
    val partitionsForLeader = topicPartitionToLeader.toVector.collect {
      case (tp, partitionLeaderId) if partitionLeaderId == leaderId => tp
    }

    val partitionsWithLargeMessages = partitionsForLeader.takeRight(2)
    val partitionWithLargeMessage1 = partitionsWithLargeMessages.head
    val partitionWithLargeMessage2 = partitionsWithLargeMessages(1)
    producer.send(new ProducerRecord(partitionWithLargeMessage1.topic, partitionWithLargeMessage1.partition,
      "larger than partition limit", new String(new Array[Byte](maxPartitionBytes + 1)))).get
    producer.send(new ProducerRecord(partitionWithLargeMessage2.topic, partitionWithLargeMessage2.partition,
      "larger than response limit", new String(new Array[Byte](maxResponseBytes + 1)))).get

    val partitionsWithoutLargeMessages = partitionsForLeader.filterNot(partitionsWithLargeMessages.contains)

    // 1. Partitions with large messages at the end
    val shuffledTopicPartitions1 = random.shuffle(partitionsWithoutLargeMessages) ++ partitionsWithLargeMessages
    val fetchRequest1 = createFetchRequest(shuffledTopicPartitions1)
    val fetchResponse1 = sendFetchRequest(leaderId, fetchRequest1)
    checkFetchResponse(shuffledTopicPartitions1, fetchResponse1, maxPartitionBytes, maxResponseBytes, messagesPerPartition)
    val fetchRequest1V12 = createFetchRequest(shuffledTopicPartitions1, version = 12)
    val fetchResponse1V12 = sendFetchRequest(leaderId, fetchRequest1V12)
    checkFetchResponse(shuffledTopicPartitions1, fetchResponse1V12, maxPartitionBytes, maxResponseBytes, messagesPerPartition, 12)

    // 2. Same as 1, but shuffled again
    val shuffledTopicPartitions2 = random.shuffle(partitionsWithoutLargeMessages) ++ partitionsWithLargeMessages
    val fetchRequest2 = createFetchRequest(shuffledTopicPartitions2)
    val fetchResponse2 = sendFetchRequest(leaderId, fetchRequest2)
    checkFetchResponse(shuffledTopicPartitions2, fetchResponse2, maxPartitionBytes, maxResponseBytes, messagesPerPartition)
    val fetchRequest2V12 = createFetchRequest(shuffledTopicPartitions2, version = 12)
    val fetchResponse2V12 = sendFetchRequest(leaderId, fetchRequest2V12)
    checkFetchResponse(shuffledTopicPartitions2, fetchResponse2V12, maxPartitionBytes, maxResponseBytes, messagesPerPartition, 12)

    // 3. Partition with message larger than the partition limit at the start of the list
    val shuffledTopicPartitions3 = Seq(partitionWithLargeMessage1, partitionWithLargeMessage2) ++
      random.shuffle(partitionsWithoutLargeMessages)
    val fetchRequest3 = createFetchRequest(shuffledTopicPartitions3, Map(partitionWithLargeMessage1 -> messagesPerPartition))
    val fetchResponse3 = sendFetchRequest(leaderId, fetchRequest3)
    val fetchRequest3V12 = createFetchRequest(shuffledTopicPartitions3, Map(partitionWithLargeMessage1 -> messagesPerPartition), 12)
    val fetchResponse3V12 = sendFetchRequest(leaderId, fetchRequest3V12)
    def evaluateResponse3(response: FetchResponse[MemoryRecords], version: Short = ApiKeys.FETCH.latestVersion()) = {
      val responseData = response.responseData(topicNames, version)
      assertEquals(shuffledTopicPartitions3, responseData.keySet.asScala.toSeq)
      val responseSize = responseData.asScala.values.map { partitionData =>
        records(partitionData).map(_.sizeInBytes).sum
      }.sum
      assertTrue(responseSize <= maxResponseBytes)
      val partitionData = responseData.get(partitionWithLargeMessage1)
      assertEquals(Errors.NONE, partitionData.error)
      assertTrue(partitionData.highWatermark > 0)
      val size3 = records(partitionData).map(_.sizeInBytes).sum
      assertTrue(size3 <= maxResponseBytes, s"Expected $size3 to be smaller than $maxResponseBytes")
      assertTrue(size3 > maxPartitionBytes, s"Expected $size3 to be larger than $maxPartitionBytes")
      assertTrue(maxPartitionBytes < partitionData.records.sizeInBytes)
    }
    evaluateResponse3(fetchResponse3)
    evaluateResponse3(fetchResponse3V12, 12)

    // 4. Partition with message larger than the response limit at the start of the list
    val shuffledTopicPartitions4 = Seq(partitionWithLargeMessage2, partitionWithLargeMessage1) ++
      random.shuffle(partitionsWithoutLargeMessages)
    val fetchRequest4 = createFetchRequest(shuffledTopicPartitions4, Map(partitionWithLargeMessage2 -> messagesPerPartition))
    val fetchResponse4 = sendFetchRequest(leaderId, fetchRequest4)
    val fetchRequest4V12 = createFetchRequest(shuffledTopicPartitions4, Map(partitionWithLargeMessage2 -> messagesPerPartition), 12)
    val fetchResponse4V12 = sendFetchRequest(leaderId, fetchRequest4V12)
    def evaluateResponse4(response: FetchResponse[MemoryRecords], version: Short = ApiKeys.FETCH.latestVersion()) = {
      val responseData = response.responseData(topicNames, version)
      assertEquals(shuffledTopicPartitions4, responseData.keySet.asScala.toSeq)
      val nonEmptyPartitions = responseData.asScala.toSeq.collect {
        case (tp, partitionData) if records(partitionData).map(_.sizeInBytes).sum > 0 => tp
      }
      assertEquals(Seq(partitionWithLargeMessage2), nonEmptyPartitions)
      val partitionData = responseData.get(partitionWithLargeMessage2)
      assertEquals(Errors.NONE, partitionData.error)
      assertTrue(partitionData.highWatermark > 0)
      val size4 = records(partitionData).map(_.sizeInBytes).sum
      assertTrue(size4 > maxResponseBytes, s"Expected $size4 to be larger than $maxResponseBytes")
      assertTrue(maxResponseBytes < partitionData.records.sizeInBytes)
    }
    evaluateResponse4(fetchResponse4)
    evaluateResponse4(fetchResponse4V12, 12)
  }


  @Test
  def testIdErrorsAtEnd(): Unit = {
    def createFetchRequest(topicPartitions: Seq[TopicPartition],
                           metadata: JFetchMetadata,
                           toForget: Seq[TopicPartition],
                           topicIds: scala.collection.Map[String, Uuid]): FetchRequest = {
      FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion(), Int.MaxValue, 0,
        createPartitionMap(Integer.MAX_VALUE, topicPartitions, Map.empty), topicIds.asJava)
        .toForget(toForget.asJava)
        .metadata(metadata)
        .build()
    }
    initProducer()

    val messagesPerPartition = 9

    val topicPartitionToLeader = createTopics(numTopics = 5, numPartitions = 6)
    val topicPartitions = topicPartitionToLeader.keySet
    produceData(topicPartitions, messagesPerPartition)
    
    val topicIds = getTopicIds() ++ Map("bar" -> Uuid.randomUuid())
    val unknownTp = new TopicPartition("bar", 1)
    val topicPartitionsWithUnknown = Set(unknownTp) ++ topicPartitions
    val topicNames = topicIds.map(_.swap).asJava
    val req1 = createFetchRequest(topicPartitionsWithUnknown.toList, JFetchMetadata.INITIAL, Nil, topicIds)
    val resp1 = sendFetchRequest(0, req1).responseData(topicNames, ApiKeys.FETCH.latestVersion())

    // The last topic partition should be the unknown one.
    val respIterator = resp1.keySet().iterator()
    assertTrue(respIterator.hasNext)
    var tp = respIterator.next()
    while (respIterator.hasNext)
      tp = respIterator.next()
    assertEquals(unknownTp, tp)
  }


  @Test
  def testFetchRequestV2WithOversizedMessage(): Unit = {
    initProducer()
    val maxPartitionBytes = 200
    val (topicPartition, leaderId) = createTopics(numTopics = 1, numPartitions = 1).head
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava
    producer.send(new ProducerRecord(topicPartition.topic, topicPartition.partition,
      "key", new String(new Array[Byte](maxPartitionBytes + 1)))).get
    val fetchRequest = FetchRequest.Builder.forConsumer(2, Int.MaxValue, 0, createPartitionMap(maxPartitionBytes,
      Seq(topicPartition)), topicIds).build(2)
    val fetchResponse = sendFetchRequest(leaderId, fetchRequest)
    val partitionData = fetchResponse.responseData(topicNames, 2).get(topicPartition)
    assertEquals(Errors.NONE, partitionData.error)
    assertTrue(partitionData.highWatermark > 0)
    assertEquals(maxPartitionBytes, partitionData.records.sizeInBytes)
    assertEquals(0, records(partitionData).map(_.sizeInBytes).sum)
  }

  @Test
  def testFetchRequestV4WithReadCommitted(): Unit = {
    initProducer()
    val maxPartitionBytes = 200
    val (topicPartition, leaderId) = createTopics(numTopics = 1, numPartitions = 1).head
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava
    producer.send(new ProducerRecord(topicPartition.topic, topicPartition.partition,
      "key", new String(new Array[Byte](maxPartitionBytes + 1)))).get
    val fetchRequest = FetchRequest.Builder.forConsumer(4, Int.MaxValue, 0, createPartitionMap(maxPartitionBytes,
      Seq(topicPartition)), topicIds).isolationLevel(IsolationLevel.READ_COMMITTED).build(4)
    val fetchResponse = sendFetchRequest(leaderId, fetchRequest)
    val partitionData = fetchResponse.responseData(topicNames, 4).get(topicPartition)
    assertEquals(Errors.NONE, partitionData.error)
    assertTrue(partitionData.lastStableOffset > 0)
    assertTrue(records(partitionData).map(_.sizeInBytes).sum > 0)
  }

  @Test
  def testFetchRequestToNonReplica(): Unit = {
    val topic = "topic"
    val partition = 0
    val topicPartition = new TopicPartition(topic, partition)

    // Create a single-partition topic and find a broker which is not the leader
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, 1, servers)
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava
    val leader = partitionToLeader(partition)
    val nonReplicaOpt = servers.find(_.config.brokerId != leader)
    assertTrue(nonReplicaOpt.isDefined)
    val nonReplicaId =  nonReplicaOpt.get.config.brokerId

    // Send the fetch request to the non-replica and verify the error code
    val fetchRequest = FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion, Int.MaxValue, 0, createPartitionMap(1024,
      Seq(topicPartition)), topicIds).build()
    val fetchResponse = sendFetchRequest(nonReplicaId, fetchRequest)
    val partitionData = fetchResponse.responseData(topicNames, ApiKeys.FETCH.latestVersion).get(topicPartition)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, partitionData.error)

    // Repeat with request that does not use topic IDs
    val oldFetchRequest = FetchRequest.Builder.forConsumer(12, Int.MaxValue, 0, createPartitionMap(1024,
      Seq(topicPartition)), topicIds).build()
    val oldFetchResponse = sendFetchRequest(nonReplicaId, oldFetchRequest)
    val oldPartitionData = oldFetchResponse.responseData(topicNames, 12).get(topicPartition)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, oldPartitionData.error)
  }

  @Test
  def testLastFetchedEpochValidation(): Unit = {
    checkLastFetchedEpochValidation(ApiKeys.FETCH.latestVersion())
  }

  @Test
  def testLastFetchedEpochValidationV12(): Unit = {
    checkLastFetchedEpochValidation(12)
  }

  private def checkLastFetchedEpochValidation(version: Short): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 3, servers)
    val firstLeaderId = partitionToLeader(topicPartition.partition)
    val firstLeaderEpoch = TestUtils.findLeaderEpoch(firstLeaderId, topicPartition, servers)

    initProducer()

    // Write some data in epoch 0
    val firstEpochResponses = produceData(Seq(topicPartition), 100)
    val firstEpochEndOffset = firstEpochResponses.lastOption.get.offset + 1
    // Force a leader change
    killBroker(firstLeaderId)
    // Write some more data in epoch 1
    val secondLeaderId = TestUtils.awaitLeaderChange(servers, topicPartition, firstLeaderId)
    val secondLeaderEpoch = TestUtils.findLeaderEpoch(secondLeaderId, topicPartition, servers)
    val secondEpochResponses = produceData(Seq(topicPartition), 100)
    val secondEpochEndOffset = secondEpochResponses.lastOption.get.offset + 1
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava

    // Build a fetch request in the middle of the second epoch, but with the first epoch
    val fetchOffset = secondEpochEndOffset + (secondEpochEndOffset - firstEpochEndOffset) / 2
    val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    partitionMap.put(topicPartition, new FetchRequest.PartitionData(fetchOffset, 0L, 1024,
      Optional.of(secondLeaderEpoch), Optional.of(firstLeaderEpoch)))
    val fetchRequest = FetchRequest.Builder.forConsumer(version, 0, 1, partitionMap, topicIds).build()

    // Validate the expected truncation
    val fetchResponse = sendFetchRequest(secondLeaderId, fetchRequest)
    val partitionData = fetchResponse.responseData(topicNames, version).get(topicPartition)
    assertEquals(Errors.NONE, partitionData.error)
    assertEquals(0L, partitionData.records.sizeInBytes())
    assertTrue(partitionData.divergingEpoch.isPresent)

    val divergingEpoch = partitionData.divergingEpoch.get()
    assertEquals(firstLeaderEpoch, divergingEpoch.epoch)
    assertEquals(firstEpochEndOffset, divergingEpoch.endOffset)
  }

  @Test
  def testCurrentEpochValidation(): Unit = {
    checkCurrentEpochValidation(ApiKeys.FETCH.latestVersion())
  }

  @Test
  def testCurrentEpochValidationV12(): Unit = {
    checkCurrentEpochValidation(12)
  }

  private def checkCurrentEpochValidation(version: Short): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 3, servers)
    val firstLeaderId = partitionToLeader(topicPartition.partition)

    def assertResponseErrorForEpoch(error: Errors, brokerId: Int, leaderEpoch: Optional[Integer]): Unit = {
      val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
      val topicIds = getTopicIds().asJava
      val topicNames = topicIds.asScala.map(_.swap).asJava
      partitionMap.put(topicPartition, new FetchRequest.PartitionData(0L, 0L, 1024, leaderEpoch))
      val fetchRequest = FetchRequest.Builder.forConsumer(version, 0, 1, partitionMap, topicIds).build()
      val fetchResponse = sendFetchRequest(brokerId, fetchRequest)
      val partitionData = fetchResponse.responseData(topicNames, version).get(topicPartition)
      assertEquals(error, partitionData.error)
    }

    // We need a leader change in order to check epoch fencing since the first epoch is 0 and
    // -1 is treated as having no epoch at all
    killBroker(firstLeaderId)

    // Check leader error codes
    val secondLeaderId = TestUtils.awaitLeaderChange(servers, topicPartition, firstLeaderId)
    val secondLeaderEpoch = TestUtils.findLeaderEpoch(secondLeaderId, topicPartition, servers)
    assertResponseErrorForEpoch(Errors.NONE, secondLeaderId, Optional.empty())
    assertResponseErrorForEpoch(Errors.NONE, secondLeaderId, Optional.of(secondLeaderEpoch))
    assertResponseErrorForEpoch(Errors.FENCED_LEADER_EPOCH, secondLeaderId, Optional.of(secondLeaderEpoch - 1))
    assertResponseErrorForEpoch(Errors.UNKNOWN_LEADER_EPOCH, secondLeaderId, Optional.of(secondLeaderEpoch + 1))

    // Check follower error codes
    val followerId = TestUtils.findFollowerId(topicPartition, servers)
    assertResponseErrorForEpoch(Errors.NONE, followerId, Optional.empty())
    assertResponseErrorForEpoch(Errors.NONE, followerId, Optional.of(secondLeaderEpoch))
    assertResponseErrorForEpoch(Errors.UNKNOWN_LEADER_EPOCH, followerId, Optional.of(secondLeaderEpoch + 1))
    assertResponseErrorForEpoch(Errors.FENCED_LEADER_EPOCH, followerId, Optional.of(secondLeaderEpoch - 1))
  }

  @Test
  def testEpochValidationWithinFetchSession(): Unit = {
    checkEpochValidationWithinFetchSession(ApiKeys.FETCH.latestVersion())
  }

  @Test
  def testEpochValidationWithinFetchSessionV12(): Unit = {
    checkEpochValidationWithinFetchSession(12)
  }

  private def checkEpochValidationWithinFetchSession(version: Short): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val partitionToLeader = TestUtils.createTopic(zkClient, topic, numPartitions = 1, replicationFactor = 3, servers)
    val firstLeaderId = partitionToLeader(topicPartition.partition)

    // We need a leader change in order to check epoch fencing since the first epoch is 0 and
    // -1 is treated as having no epoch at all
    killBroker(firstLeaderId)

    val secondLeaderId = TestUtils.awaitLeaderChange(servers, topicPartition, firstLeaderId)
    val secondLeaderEpoch = TestUtils.findLeaderEpoch(secondLeaderId, topicPartition, servers)
    verifyFetchSessionErrors(topicPartition, secondLeaderEpoch, secondLeaderId, version)

    val followerId = TestUtils.findFollowerId(topicPartition, servers)
    verifyFetchSessionErrors(topicPartition, secondLeaderEpoch, followerId, version)
  }

  private def verifyFetchSessionErrors(topicPartition: TopicPartition,
                                       leaderEpoch: Int,
                                       destinationBrokerId: Int,
                                       version: Short): Unit = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    partitionMap.put(topicPartition, new FetchRequest.PartitionData(0L, 0L, 1024,
      Optional.of(leaderEpoch)))
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava
    val fetchRequest = FetchRequest.Builder.forConsumer(version, 0, 1, partitionMap, topicIds)
      .metadata(JFetchMetadata.INITIAL)
      .build()
    val fetchResponse = sendFetchRequest(destinationBrokerId, fetchRequest)
    val sessionId = fetchResponse.sessionId

    def assertResponseErrorForEpoch(expectedError: Errors,
                                    sessionFetchEpoch: Int,
                                    leaderEpoch: Optional[Integer]): Unit = {
      val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
      partitionMap.put(topicPartition, new FetchRequest.PartitionData(0L, 0L, 1024, leaderEpoch))
      val fetchRequest = FetchRequest.Builder.forConsumer(version, 0, 1, partitionMap, topicIds)
        .metadata(new JFetchMetadata(sessionId, sessionFetchEpoch))
        .build()
      val fetchResponse = sendFetchRequest(destinationBrokerId, fetchRequest)
      val partitionData = fetchResponse.responseData(topicNames, version).get(topicPartition)
      assertEquals(expectedError, partitionData.error)
    }

    // We only check errors because we do not expect the partition in the response otherwise
    assertResponseErrorForEpoch(Errors.FENCED_LEADER_EPOCH, 1, Optional.of(leaderEpoch - 1))
    assertResponseErrorForEpoch(Errors.UNKNOWN_LEADER_EPOCH, 2, Optional.of(leaderEpoch + 1))
  }

  /**
   * Tests that down-conversions don't leak memory. Large down conversions are triggered
   * in the server. The client closes its connection after reading partial data when the
   * channel is muted in the server. If buffers are not released this will result in OOM.
   */
  @Test
  def testDownConversionWithConnectionFailure(): Unit = {
    val (topicPartition, leaderId) = createTopics(numTopics = 1, numPartitions = 1).head
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava

    val msgValueLen = 100 * 1000
    val batchSize = 4 * msgValueLen
    val producer = TestUtils.createProducer(TestUtils.getBrokerListStrFromServers(servers),
      lingerMs = Int.MaxValue,
      deliveryTimeoutMs = Int.MaxValue,
      batchSize = batchSize,
      keySerializer = new StringSerializer,
      valueSerializer = new ByteArraySerializer)
    val bytes = new Array[Byte](msgValueLen)
    val futures = try {
      (0 to 1000).map { _ =>
        producer.send(new ProducerRecord(topicPartition.topic, topicPartition.partition, "key", bytes))
      }
    } finally {
      producer.close()
    }
    // Check futures to ensure sends succeeded, but do this after close since the last
    // batch is not complete, but sent when the producer is closed
    futures.foreach(_.get)

    def fetch(version: Short, maxPartitionBytes: Int, closeAfterPartialResponse: Boolean): Option[FetchResponse[MemoryRecords]] = {
      val fetchRequest = FetchRequest.Builder.forConsumer(version, Int.MaxValue, 0, createPartitionMap(maxPartitionBytes,
        Seq(topicPartition)), topicIds).build(version)

      val socket = connect(brokerSocketServer(leaderId))
      try {
        send(fetchRequest, socket)
        if (closeAfterPartialResponse) {
          // read some data to ensure broker has muted this channel and then close socket
          val size = new DataInputStream(socket.getInputStream).readInt()
          // Check that we have received almost `maxPartitionBytes` (minus a tolerance) since in
          // the case of OOM, the size will be significantly smaller. We can't check for exactly
          // maxPartitionBytes since we use approx message sizes that include only the message value.
          assertTrue(size > maxPartitionBytes - batchSize,
              s"Fetch size too small $size, broker may have run out of memory")
          None
        } else {
          Some(receive[FetchResponse[MemoryRecords]](socket, ApiKeys.FETCH, version))
        }
      } finally {
        socket.close()
      }
    }

    val version = 1.toShort
    (0 to 15).foreach(_ => fetch(version, maxPartitionBytes = msgValueLen * 1000, closeAfterPartialResponse = true))

    val response = fetch(version, maxPartitionBytes = batchSize, closeAfterPartialResponse = false)
    val fetchResponse = response.getOrElse(throw new IllegalStateException("No fetch response"))
    val partitionData = fetchResponse.responseData(topicNames, version).get(topicPartition)
    assertEquals(Errors.NONE, partitionData.error)
    val batches = partitionData.records.batches.asScala.toBuffer
    assertEquals(3, batches.size) // size is 3 (not 4) since maxPartitionBytes=msgValueSize*4, excluding key and headers
  }

  /**
    * Ensure that we respect the fetch offset when returning records that were converted from an uncompressed v2
    * record batch to multiple v0/v1 record batches with size 1. If the fetch offset points to inside the record batch,
    * some records have to be dropped during the conversion.
    */
  @Test
  def testDownConversionFromBatchedToUnbatchedRespectsOffset(): Unit = {
    // Increase linger so that we have control over the batches created
    producer = TestUtils.createProducer(TestUtils.getBrokerListStrFromServers(servers),
      retries = 5,
      keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer,
      lingerMs = 30 * 1000,
      deliveryTimeoutMs = 60 * 1000)

    val topicConfig = Map(LogConfig.MessageFormatVersionProp -> KAFKA_0_11_0_IV2.version)
    val (topicPartition, leaderId) = createTopics(numTopics = 1, numPartitions = 1, topicConfig).head
    val topic = topicPartition.topic
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava

    val firstBatchFutures = (0 until 10).map(i => producer.send(new ProducerRecord(topic, s"key-$i", s"value-$i")))
    producer.flush()
    val secondBatchFutures = (10 until 25).map(i => producer.send(new ProducerRecord(topic, s"key-$i", s"value-$i")))
    producer.flush()

    firstBatchFutures.foreach(_.get)
    secondBatchFutures.foreach(_.get)

    def check(fetchOffset: Long, requestVersion: Short, expectedOffset: Long, expectedNumBatches: Int, expectedMagic: Byte): Unit = {
      var batchesReceived = 0
      var currentFetchOffset = fetchOffset
      var currentExpectedOffset = expectedOffset

      // With KIP-283, we might not receive all batches in a single fetch request so loop through till we have consumed
      // all batches we are interested in.
      while (batchesReceived < expectedNumBatches) {
        val fetchRequest = FetchRequest.Builder.forConsumer(requestVersion, Int.MaxValue, 0, createPartitionMap(Int.MaxValue,
          Seq(topicPartition), Map(topicPartition -> currentFetchOffset)), topicIds).build(requestVersion)
        val fetchResponse = sendFetchRequest(leaderId, fetchRequest)

        // validate response
        val partitionData = fetchResponse.responseData(topicNames, requestVersion).get(topicPartition)
        assertEquals(Errors.NONE, partitionData.error)
        assertTrue(partitionData.highWatermark > 0)
        val batches = partitionData.records.batches.asScala.toBuffer
        val batch = batches.head
        assertEquals(expectedMagic, batch.magic)
        assertEquals(currentExpectedOffset, batch.baseOffset)

        currentFetchOffset = batches.last.lastOffset + 1
        currentExpectedOffset += (batches.last.lastOffset - batches.head.baseOffset + 1)
        batchesReceived += batches.size
      }

      assertEquals(expectedNumBatches, batchesReceived)
    }

    // down conversion to message format 0, batches of 1 message are returned so we receive the exact offset we requested
    check(fetchOffset = 3, expectedOffset = 3, requestVersion = 1, expectedNumBatches = 22,
      expectedMagic = RecordBatch.MAGIC_VALUE_V0)
    check(fetchOffset = 15, expectedOffset = 15, requestVersion = 1, expectedNumBatches = 10,
      expectedMagic = RecordBatch.MAGIC_VALUE_V0)

    // down conversion to message format 1, batches of 1 message are returned so we receive the exact offset we requested
    check(fetchOffset = 3, expectedOffset = 3, requestVersion = 3, expectedNumBatches = 22,
      expectedMagic = RecordBatch.MAGIC_VALUE_V1)
    check(fetchOffset = 15, expectedOffset = 15, requestVersion = 3, expectedNumBatches = 10,
      expectedMagic = RecordBatch.MAGIC_VALUE_V1)

    // no down conversion, we receive a single batch so the received offset won't necessarily be the same
    check(fetchOffset = 3, expectedOffset = 0, requestVersion = 4, expectedNumBatches = 2,
      expectedMagic = RecordBatch.MAGIC_VALUE_V2)
    check(fetchOffset = 15, expectedOffset = 10, requestVersion = 4, expectedNumBatches = 1,
      expectedMagic = RecordBatch.MAGIC_VALUE_V2)

    // no down conversion, we receive a single batch and the exact offset we requested because it happens to be the
    // offset of the first record in the batch
    check(fetchOffset = 10, expectedOffset = 10, requestVersion = 4, expectedNumBatches = 1,
      expectedMagic = RecordBatch.MAGIC_VALUE_V2)
  }

  /**
   * Test that when an incremental fetch session contains partitions with an error,
   * those partitions are returned in all incremental fetch requests.
   * This tests using FetchRequests that don't use topic IDs
   */
  @Test
  def testCreateIncrementalFetchWithPartitionsInErrorV12(): Unit = {
    def createFetchRequest(topicPartitions: Seq[TopicPartition],
                           metadata: JFetchMetadata,
                           toForget: Seq[TopicPartition]): FetchRequest =
      FetchRequest.Builder.forConsumer(12, Int.MaxValue, 0,
        createPartitionMap(Integer.MAX_VALUE, topicPartitions, Map.empty), Map[String, Uuid]().asJava)
        .toForget(toForget.asJava)
        .metadata(metadata)
        .build()
    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)
    // topicNames can be empty because we are using old requests
    val topicNames = Map[Uuid, String]().asJava
    createTopic("foo", Map(0 -> List(0, 1), 1 -> List(0, 2)))
    val bar0 = new TopicPartition("bar", 0)
    val req1 = createFetchRequest(List(foo0, foo1, bar0), JFetchMetadata.INITIAL, Nil)
    val resp1 = sendFetchRequest(0, req1)
    assertEquals(Errors.NONE, resp1.error())
    assertTrue(resp1.sessionId() > 0, "Expected the broker to create a new incremental fetch session")
    debug(s"Test created an incremental fetch session ${resp1.sessionId}")
    val responseData1 = resp1.responseData(topicNames, 12)
    assertTrue(responseData1.containsKey(foo0))
    assertTrue(responseData1.containsKey(foo1))
    assertTrue(responseData1.containsKey(bar0))
    assertEquals(Errors.NONE, responseData1.get(foo0).error)
    assertEquals(Errors.NONE, responseData1.get(foo1).error)
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, responseData1.get(bar0).error)
    val req2 = createFetchRequest(Nil, new JFetchMetadata(resp1.sessionId(), 1), Nil)
    val resp2 = sendFetchRequest(0, req2)
    assertEquals(Errors.NONE, resp2.error())
    assertEquals(resp1.sessionId(),
      resp2.sessionId(), "Expected the broker to continue the incremental fetch session")
    val responseData2 = resp2.responseData(topicNames, 12)
    assertFalse(responseData2.containsKey(foo0))
    assertFalse(responseData2.containsKey(foo1))
    assertTrue(responseData2.containsKey(bar0))
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, responseData2.get(bar0).error)
    createTopic("bar", Map(0 -> List(0, 1)))
    val req3 = createFetchRequest(Nil, new JFetchMetadata(resp1.sessionId(), 2), Nil)
    val resp3 = sendFetchRequest(0, req3)
    assertEquals(Errors.NONE, resp3.error())
    val responseData3 = resp3.responseData(topicNames, 12)
    assertFalse(responseData3.containsKey(foo0))
    assertFalse(responseData3.containsKey(foo1))
    assertTrue(responseData3.containsKey(bar0))
    assertEquals(Errors.NONE, responseData3.get(bar0).error)
    val req4 = createFetchRequest(Nil, new JFetchMetadata(resp1.sessionId(), 3), Nil)
    val resp4 = sendFetchRequest(0, req4)
    assertEquals(Errors.NONE, resp4.error())
    val responseData4 = resp4.responseData(topicNames, 12)
    assertFalse(responseData4.containsKey(foo0))
    assertFalse(responseData4.containsKey(foo1))
    assertFalse(responseData4.containsKey(bar0))
  }

  /**
   * Test that when an incremental fetch session contains partitions with an ID error,
   * those partitions are returned in all incremental fetch requests. Tests for
   * resolving topic IDs can be found in FetchSessionTest.
   */
  @Test
  def testCreateIncrementalFetchWithPartitionsWithIdError(): Unit = {
    def createFetchRequest(topicPartitions: Seq[TopicPartition],
                           metadata: JFetchMetadata,
                           toForget: Seq[TopicPartition],
                           topicIds: scala.collection.Map[String, Uuid]): FetchRequest = {
      FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion(), Int.MaxValue, 0,
        createPartitionMap(Integer.MAX_VALUE, topicPartitions, Map.empty), topicIds.asJava)
        .toForget(toForget.asJava)
        .metadata(metadata)
        .build()
    }
    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)
    createTopic("foo", Map(0 -> List(0, 1), 1 -> List(0, 2)))
    val topicIds = getTopicIds()
    val topicIDsWithUnknown = topicIds ++ Map("bar" -> Uuid.randomUuid())
    val bar0 = new TopicPartition("bar", 0)
    val req1 = createFetchRequest(List(foo0, foo1, bar0), JFetchMetadata.INITIAL, Nil, topicIDsWithUnknown)
    val resp1 = sendFetchRequest(0, req1)
    assertEquals(Errors.NONE, resp1.error())
    assertTrue(resp1.sessionId() > 0, "Expected the broker to create a new incremental fetch session")
    debug(s"Test created an incremental fetch session ${resp1.sessionId}")
    val topicNames1 = topicIDsWithUnknown.map(_.swap).asJava
    val responseData1 = resp1.responseData(topicNames1, ApiKeys.FETCH.latestVersion())
    assertTrue(responseData1.containsKey(foo0))
    assertTrue(responseData1.containsKey(foo1))
    assertTrue(responseData1.containsKey(bar0))
    assertEquals(Errors.NONE, responseData1.get(foo0).error)
    assertEquals(Errors.NONE, responseData1.get(foo1).error)
    assertEquals(Errors.UNKNOWN_TOPIC_ID, responseData1.get(bar0).error)
    val topicIds2 = topicIds
    val req2 = createFetchRequest(Nil, new JFetchMetadata(resp1.sessionId(), 1), Nil, topicIds2)
    val resp2 = sendFetchRequest(0, req2)
    assertEquals(Errors.NONE, resp2.error())
    assertEquals(resp1.sessionId(),
      resp2.sessionId(), "Expected the broker to continue the incremental fetch session")
    val topicNames2 = topicNames1
    val responseData2 = resp2.responseData(topicNames2, ApiKeys.FETCH.latestVersion())
    assertFalse(responseData2.containsKey(foo0))
    assertFalse(responseData2.containsKey(foo1))
    assertTrue(responseData1.containsKey(bar0))
    assertEquals(Errors.UNKNOWN_TOPIC_ID, responseData1.get(bar0).error)
  }

  @Test
  def testZStdCompressedTopic(): Unit = {
    // ZSTD compressed topic
    val topicConfig = Map(LogConfig.CompressionTypeProp -> ZStdCompressionCodec.name)
    val (topicPartition, leaderId) = createTopics(numTopics = 1, numPartitions = 1, configs = topicConfig).head
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava

    // Produce messages (v2)
    producer = TestUtils.createProducer(TestUtils.getBrokerListStrFromServers(servers),
      keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer)
    producer.send(new ProducerRecord(topicPartition.topic, topicPartition.partition,
      "key1", "value1")).get
    producer.send(new ProducerRecord(topicPartition.topic, topicPartition.partition,
      "key2", "value2")).get
    producer.send(new ProducerRecord(topicPartition.topic, topicPartition.partition,
      "key3", "value3")).get
    producer.close()

    // fetch request with version below v10: UNSUPPORTED_COMPRESSION_TYPE error occurs
    val req0 = new FetchRequest.Builder(0, 9, -1, Int.MaxValue, 0,
      createPartitionMap(300, Seq(topicPartition), Map.empty), topicIds)
      .setMaxBytes(800).build()

    val res0 = sendFetchRequest(leaderId, req0)
    val data0 = res0.responseData(topicNames, 9).get(topicPartition)
    assertEquals(Errors.UNSUPPORTED_COMPRESSION_TYPE, data0.error)

    // fetch request with version 10: works fine!
    val req1= new FetchRequest.Builder(0, 10, -1, Int.MaxValue, 0,
      createPartitionMap(300, Seq(topicPartition), Map.empty), topicIds)
      .setMaxBytes(800).build()
    val res1 = sendFetchRequest(leaderId, req1)
    val data1 = res1.responseData(topicNames, 10).get(topicPartition)
    assertEquals(Errors.NONE, data1.error)
    assertEquals(3, records(data1).size)

    val req2 = new FetchRequest.Builder(ApiKeys.FETCH.latestVersion(), ApiKeys.FETCH.latestVersion(), -1, Int.MaxValue, 0,
      createPartitionMap(300, Seq(topicPartition), Map.empty), topicIds)
      .setMaxBytes(800).build()
    val res2 = sendFetchRequest(leaderId, req2)
    val data2 = res2.responseData(topicNames, ApiKeys.FETCH.latestVersion()).get(topicPartition)
    assertEquals(Errors.NONE, data2.error)
    assertEquals(3, records(data2).size)
  }

  @Test
  def testPartitionDataEquals(): Unit = {
    assertEquals(new FetchRequest.PartitionData(300, 0L, 300, Optional.of(300)),
    new FetchRequest.PartitionData(300, 0L, 300, Optional.of(300)));
  }

  @Test
  def testZStdCompressedRecords(): Unit = {
    // Producer compressed topic
    val topicConfig = Map(LogConfig.CompressionTypeProp -> ProducerCompressionCodec.name,
      LogConfig.MessageFormatVersionProp -> "2.0.0")
    val (topicPartition, leaderId) = createTopics(numTopics = 1, numPartitions = 1, configs = topicConfig).head
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava

    // Produce GZIP compressed messages (v2)
    val producer1 = TestUtils.createProducer(TestUtils.getBrokerListStrFromServers(servers),
      compressionType = GZIPCompressionCodec.name,
      keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer)
    producer1.send(new ProducerRecord(topicPartition.topic, topicPartition.partition,
      "key1", "value1")).get
    producer1.close()
    // Produce ZSTD compressed messages (v2)
    val producer2 = TestUtils.createProducer(TestUtils.getBrokerListStrFromServers(servers),
      compressionType = ZStdCompressionCodec.name,
      keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer)
    producer2.send(new ProducerRecord(topicPartition.topic, topicPartition.partition,
      "key2", "value2")).get
    producer2.send(new ProducerRecord(topicPartition.topic, topicPartition.partition,
      "key3", "value3")).get
    producer2.close()

    // fetch request with fetch version v1 (magic 0):
    // gzip compressed record is returned with down-conversion.
    // zstd compressed record raises UNSUPPORTED_COMPRESSION_TYPE error.
    val req0 = new FetchRequest.Builder(0, 1, -1, Int.MaxValue, 0,
      createPartitionMap(300, Seq(topicPartition), Map.empty), topicIds)
      .setMaxBytes(800)
      .build()

    val res0 = sendFetchRequest(leaderId, req0)
    val data0 = res0.responseData(topicNames, 1).get(topicPartition)
    assertEquals(Errors.NONE, data0.error)
    assertEquals(1, records(data0).size)

    val req1 = new FetchRequest.Builder(0, 1, -1, Int.MaxValue, 0,
      createPartitionMap(300, Seq(topicPartition), Map(topicPartition -> 1L)), topicIds)
      .setMaxBytes(800).build()

    val res1 = sendFetchRequest(leaderId, req1)
    val data1 = res1.responseData(topicNames, 1).get(topicPartition)
    assertEquals(Errors.UNSUPPORTED_COMPRESSION_TYPE, data1.error)

    // fetch request with fetch version v3 (magic 1):
    // gzip compressed record is returned with down-conversion.
    // zstd compressed record raises UNSUPPORTED_COMPRESSION_TYPE error.
    val req2 = new FetchRequest.Builder(2, 3, -1, Int.MaxValue, 0,
      createPartitionMap(300, Seq(topicPartition), Map.empty), topicIds)
      .setMaxBytes(800).build()

    val res2 = sendFetchRequest(leaderId, req2)
    val data2 = res2.responseData(topicNames, 3).get(topicPartition)
    assertEquals(Errors.NONE, data2.error)
    assertEquals(1, records(data2).size)

    val req3 = new FetchRequest.Builder(0, 1, -1, Int.MaxValue, 0,
      createPartitionMap(300, Seq(topicPartition), Map(topicPartition -> 1L)), topicIds)
      .setMaxBytes(800).build()

    val res3 = sendFetchRequest(leaderId, req3)
    val data3 = res3.responseData(topicNames, 1).get(topicPartition)
    assertEquals(Errors.UNSUPPORTED_COMPRESSION_TYPE, data3.error)

    // fetch request with version 10: works fine!
    val req4 = new FetchRequest.Builder(0, 10, -1, Int.MaxValue, 0,
      createPartitionMap(300, Seq(topicPartition), Map.empty), topicIds)
      .setMaxBytes(800).build()
    val res4 = sendFetchRequest(leaderId, req4)
    val data4 = res4.responseData(topicNames, 10).get(topicPartition)
    assertEquals(Errors.NONE, data4.error)
    assertEquals(3, records(data4).size)

    val req5 = new FetchRequest.Builder(0, ApiKeys.FETCH.latestVersion(), -1, Int.MaxValue, 0,
      createPartitionMap(300, Seq(topicPartition), Map.empty), topicIds)
      .setMaxBytes(800).build()
    val res5 = sendFetchRequest(leaderId, req5)
    val data5 = res5.responseData(topicNames, ApiKeys.FETCH.latestVersion()).get(topicPartition)
    assertEquals(Errors.NONE, data5.error)
    assertEquals(3, records(data5).size)
  }

  private def records(partitionData: FetchResponse.PartitionData[MemoryRecords]): Seq[Record] = {
    partitionData.records.records.asScala.toBuffer
  }

  private def checkFetchResponse(expectedPartitions: Seq[TopicPartition], fetchResponse: FetchResponse[MemoryRecords],
                                 maxPartitionBytes: Int, maxResponseBytes: Int, numMessagesPerPartition: Int,
                                 responseVersion: Short = ApiKeys.FETCH.latestVersion()): Unit = {
    val topicNames = getTopicIds().map(_.swap).asJava
    val responseData = fetchResponse.responseData(topicNames, responseVersion)
    assertEquals(expectedPartitions, responseData.keySet.asScala.toSeq)
    var emptyResponseSeen = false
    var responseSize = 0
    var responseBufferSize = 0

    expectedPartitions.foreach { tp =>
      val partitionData = responseData.get(tp)
      assertEquals(Errors.NONE, partitionData.error)
      assertTrue(partitionData.highWatermark > 0)

      val records = partitionData.records
      responseBufferSize += records.sizeInBytes

      val batches = records.batches.asScala.toBuffer
      assertTrue(batches.size < numMessagesPerPartition)
      val batchesSize = batches.map(_.sizeInBytes).sum
      responseSize += batchesSize
      if (batchesSize == 0 && !emptyResponseSeen) {
        assertEquals(0, records.sizeInBytes)
        emptyResponseSeen = true
      }
      else if (batchesSize != 0 && !emptyResponseSeen) {
        assertTrue(batchesSize <= maxPartitionBytes)
        assertEquals(maxPartitionBytes, records.sizeInBytes)
      }
      else if (batchesSize != 0 && emptyResponseSeen)
        fail(s"Expected partition with size 0, but found $tp with size $batchesSize")
      else if (records.sizeInBytes != 0 && emptyResponseSeen)
        fail(s"Expected partition buffer with size 0, but found $tp with size ${records.sizeInBytes}")
    }

    assertEquals(maxResponseBytes - maxResponseBytes % maxPartitionBytes, responseBufferSize)
    assertTrue(responseSize <= maxResponseBytes)
  }

  private def createTopics(numTopics: Int, numPartitions: Int, configs: Map[String, String] = Map.empty): Map[TopicPartition, Int] = {
    val topics = (0 until numTopics).map(t => s"topic$t")
    val topicConfig = new Properties
    topicConfig.setProperty(LogConfig.MinInSyncReplicasProp, 2.toString)
    configs.foreach { case (k, v) => topicConfig.setProperty(k, v) }
    topics.flatMap { topic =>
      val partitionToLeader = createTopic(topic, numPartitions = numPartitions, replicationFactor = 2,
        topicConfig = topicConfig)
      partitionToLeader.map { case (partition, leader) => new TopicPartition(topic, partition) -> leader }
    }.toMap
  }

  private def produceData(topicPartitions: Iterable[TopicPartition], numMessagesPerPartition: Int): Seq[RecordMetadata] = {
    val records = for {
      tp <- topicPartitions.toSeq
      messageIndex <- 0 until numMessagesPerPartition
    } yield {
      val suffix = s"$tp-$messageIndex"
      new ProducerRecord(tp.topic, tp.partition, s"key $suffix", s"value $suffix")
    }
    records.map(producer.send(_).get)
  }
}
