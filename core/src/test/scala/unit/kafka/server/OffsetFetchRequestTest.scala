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

package kafka.server

import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData
import org.apache.kafka.common.requests.{AbstractResponse, OffsetFetchRequest, OffsetFetchResponse}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

import java.util
import java.util.Collections.singletonList
import scala.jdk.CollectionConverters._
import java.util.{Optional, Properties}

class OffsetFetchRequestTest extends BaseRequestTest{

  override def brokerCount: Int = 1

  val brokerId: Integer = 0
  val offset = 15L
  val leaderEpoch: Optional[Integer] = Optional.of(3)
  val metadata = "metadata"
  val topic = "topic"
  val groupId = "groupId"

  override def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.put(KafkaConfig.BrokerIdProp, brokerId.toString)
    properties.put(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
    properties.put(KafkaConfig.TransactionsTopicPartitionsProp, "1")
    properties.put(KafkaConfig.TransactionsTopicReplicationFactorProp, "1")
    properties.put(KafkaConfig.TransactionsTopicMinISRProp, "1")
  }

  @BeforeEach
  override def setUp(): Unit = {
    doSetup(createOffsetsTopic = false)

    TestUtils.createOffsetsTopic(zkClient, servers)
  }

  @Test
  def testOffsetFetchRequestSingleGroup(): Unit = {
    createTopic(topic)

    val tpList = singletonList(new TopicPartition(topic, 0))
    val topicOffsets = tpList.asScala.map{
      tp => (tp, new OffsetAndMetadata(offset, leaderEpoch, metadata))
    }.toMap.asJava

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    commitOffsets(tpList, topicOffsets)

    // testing from version 1 onward since version 0 read offsets from ZK
    for (version <- 1 to ApiKeys.OFFSET_FETCH.latestVersion()) {
      if (version < 8) {
        val request =
          if (version < 7) {
            new OffsetFetchRequest.Builder(
              groupId, false, tpList, false)
              .build(version.asInstanceOf[Short])
          } else {
            new OffsetFetchRequest.Builder(
              groupId, false, tpList, true)
              .build(version.asInstanceOf[Short])
          }
        val response = connectAndReceive[OffsetFetchResponse](request)
        val topicData = response.data().topics().get(0)
        val partitionData = topicData.partitions().get(0)
        if (version < 3) {
          assertEquals(AbstractResponse.DEFAULT_THROTTLE_TIME, response.throttleTimeMs())
        }
        verifySingleGroupResponse(version.asInstanceOf[Short],
          response.error().code(), partitionData.errorCode(), topicData.name(),
          partitionData.partitionIndex(), partitionData.committedOffset(),
          partitionData.committedLeaderEpoch(), partitionData.metadata())
      } else {
        val request = new OffsetFetchRequest.Builder(
          Map(groupId -> tpList).asJava, false, false)
          .build(version.asInstanceOf[Short])
        val response = connectAndReceive[OffsetFetchResponse](request)
        val groupData = response.data().groupIds().get(0)
        val topicData = groupData.topics().get(0)
        val partitionData = topicData.partitions().get(0)
        verifySingleGroupResponse(version.asInstanceOf[Short],
          groupData.errorCode(), partitionData.errorCode(), topicData.name(),
          partitionData.partitionIndex(), partitionData.committedOffset(),
          partitionData.committedLeaderEpoch(), partitionData.metadata())
      }
    }
  }

  @Test
  def testOffsetFetchRequestV8AndAbove(): Unit = {
    val groupOne = "group1"
    val groupTwo = "group2"
    val groupThree = "group3"
    val groupFour = "group4"
    val groupFive = "group5"

    val topic1 = "topic1"
    val topic1List = singletonList(new TopicPartition(topic1, 0))
    val topic2 = "topic2"
    val topic1And2List = util.Arrays.asList(
      new TopicPartition(topic1, 0),
      new TopicPartition(topic2, 0),
      new TopicPartition(topic2, 1))
    val topic3 = "topic3"
    val allTopicsList = util.Arrays.asList(
      new TopicPartition(topic1, 0),
      new TopicPartition(topic2, 0),
      new TopicPartition(topic2, 1),
      new TopicPartition(topic3, 0),
      new TopicPartition(topic3, 1),
      new TopicPartition(topic3, 2))

    // create group to partition map to build batched offsetFetch request
    val groupToPartitionMap: util.Map[String, util.List[TopicPartition]] =
      new util.HashMap[String, util.List[TopicPartition]]()
    groupToPartitionMap.put(groupOne, topic1List)
    groupToPartitionMap.put(groupTwo, topic1And2List)
    groupToPartitionMap.put(groupThree, allTopicsList)
    groupToPartitionMap.put(groupFour, null)
    groupToPartitionMap.put(groupFive, null)

    createTopic(topic1)
    createTopic(topic2, numPartitions = 2)
    createTopic(topic3, numPartitions = 3)

    val topicOneOffsets = topic1List.asScala.map{
      tp => (tp, new OffsetAndMetadata(offset, leaderEpoch, metadata))
    }.toMap.asJava
    val topicOneAndTwoOffsets = topic1And2List.asScala.map{
      tp => (tp, new OffsetAndMetadata(offset, leaderEpoch, metadata))
    }.toMap.asJava
    val allTopicOffsets = allTopicsList.asScala.map{
      tp => (tp, new OffsetAndMetadata(offset, leaderEpoch, metadata))
    }.toMap.asJava

    // create 5 consumers to commit offsets so we can fetch them later
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupOne)
    commitOffsets(topic1List, topicOneOffsets)

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupTwo)
    commitOffsets(topic1And2List, topicOneAndTwoOffsets)

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupThree)
    commitOffsets(allTopicsList, allTopicOffsets)

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupFour)
    commitOffsets(allTopicsList, allTopicOffsets)

    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupFive)
    commitOffsets(allTopicsList, allTopicOffsets)

    for (version <- 8 to ApiKeys.OFFSET_FETCH.latestVersion()) {
      val request =  new OffsetFetchRequest.Builder(groupToPartitionMap, false, false)
        .build(version.asInstanceOf[Short])
      val response = connectAndReceive[OffsetFetchResponse](request)
      response.data().groupIds().forEach(g =>
        g.groupId() match {
          case "group1" =>
            verifyResponse(response.groupLevelError(groupOne),
              response.partitionDataMap(groupOne), topic1List)
          case "group2" =>
            verifyResponse(response.groupLevelError(groupTwo),
              response.partitionDataMap(groupTwo), topic1And2List)
          case "group3" =>
            verifyResponse(response.groupLevelError(groupThree),
              response.partitionDataMap(groupThree), allTopicsList)
          case "group4" =>
            verifyResponse(response.groupLevelError(groupFour),
              response.partitionDataMap(groupFour), allTopicsList)
          case "group5" =>
            verifyResponse(response.groupLevelError(groupFive),
              response.partitionDataMap(groupFive), allTopicsList)
        })
    }
  }

  private def verifySingleGroupResponse(version: Short,
                                        responseError: Short,
                                        partitionError: Short,
                                        topicName: String,
                                        partitionIndex: Integer,
                                        committedOffset: Long,
                                        committedLeaderEpoch: Integer,
                                        partitionMetadata: String): Unit = {
    assertEquals(Errors.NONE.code(), responseError)
    assertEquals(topic, topicName)
    assertEquals(0, partitionIndex)
    assertEquals(offset, committedOffset)
    if (version >= 5) {
      assertEquals(leaderEpoch.get(), committedLeaderEpoch)
    }
    assertEquals(metadata, partitionMetadata)
    assertEquals(Errors.NONE.code(), partitionError)
  }

  private def verifyPartitionData(partitionData: OffsetFetchResponse.PartitionData): Unit = {
    assertTrue(!partitionData.hasError)
    assertEquals(offset, partitionData.offset)
    assertEquals(metadata, partitionData.metadata)
    assertEquals(leaderEpoch.get(), partitionData.leaderEpoch.get())
  }

  private def verifyResponse(groupLevelResponse: Errors,
                             partitionData: util.Map[TopicPartition, PartitionData],
                             topicList: util.List[TopicPartition]): Unit = {
    assertEquals(Errors.NONE, groupLevelResponse)
    assertTrue(partitionData.size() == topicList.size())
    topicList.forEach(t => verifyPartitionData(partitionData.get(t)))
  }

  private def commitOffsets(tpList: util.List[TopicPartition],
                            offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    val consumer = createConsumer()
    consumer.assign(tpList)
    consumer.commitSync(offsets)
    consumer.close()
  }
}
