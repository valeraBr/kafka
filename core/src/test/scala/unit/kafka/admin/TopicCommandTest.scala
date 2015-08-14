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
package kafka.admin

import junit.framework.Assert._
import org.junit.Test
import kafka.utils.Logging
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import kafka.server.ConfigType
import kafka.admin.TopicCommand.TopicCommandOptions
import kafka.utils.ZkUtils
import kafka.coordinator.ConsumerCoordinator

class TopicCommandTest extends ZooKeeperTestHarness with Logging with RackAwareTest {

  @Test
  def testConfigPreservationAcrossPartitionAlteration() {
    val topic = "test"
    val numPartitionsOriginal = 1
    val cleanupKey = "cleanup.policy"
    val cleanupVal = "compact"
    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)
    // create the topic
    val createOpts = new TopicCommandOptions(Array("--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1",
      "--config", cleanupKey + "=" + cleanupVal,
      "--topic", topic))
    TopicCommand.createTopic(zkClient, createOpts)
    val props = AdminUtils.fetchEntityConfig(zkClient, ConfigType.Topic, topic)
    assertTrue("Properties after creation don't contain " + cleanupKey, props.containsKey(cleanupKey))
    assertTrue("Properties after creation have incorrect value", props.getProperty(cleanupKey).equals(cleanupVal))

    // pre-create the topic config changes path to avoid a NoNodeException
    ZkUtils.createPersistentPath(zkClient, ZkUtils.EntityConfigChangesPath)

    // modify the topic to add new partitions
    val numPartitionsModified = 3
    val alterOpts = new TopicCommandOptions(Array("--partitions", numPartitionsModified.toString, "--topic", topic))
    TopicCommand.alterTopic(zkClient, alterOpts)
    val newProps = AdminUtils.fetchEntityConfig(zkClient, ConfigType.Topic, topic)
    assertTrue("Updated properties do not contain " + cleanupKey, newProps.containsKey(cleanupKey))
    assertTrue("Updated properties have incorrect value", newProps.getProperty(cleanupKey).equals(cleanupVal))
  }

  @Test
  def testTopicDeletion() {
    val normalTopic = "test"

    val numPartitionsOriginal = 1

    // create brokers
    val brokers = List(0, 1, 2)
    TestUtils.createBrokersInZk(zkClient, brokers)

    // create the NormalTopic
    val createOpts = new TopicCommandOptions(Array("--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1",
      "--topic", normalTopic))
    TopicCommand.createTopic(zkClient, createOpts)

    // delete the NormalTopic
    val deleteOpts = new TopicCommandOptions(Array("--topic", normalTopic))
    val deletePath = ZkUtils.getDeleteTopicPath(normalTopic)
    assertFalse("Delete path for topic shouldn't exist before deletion.", zkClient.exists(deletePath))
    TopicCommand.deleteTopic(zkClient, deleteOpts)
    assertTrue("Delete path for topic should exist after deletion.", zkClient.exists(deletePath))

    // create the offset topic
    val createOffsetTopicOpts = new TopicCommandOptions(Array("--partitions", numPartitionsOriginal.toString,
      "--replication-factor", "1",
      "--topic", ConsumerCoordinator.OffsetsTopicName))
    TopicCommand.createTopic(zkClient, createOffsetTopicOpts)

    // try to delete the OffsetManager.OffsetsTopicName and make sure it doesn't
    val deleteOffsetTopicOpts = new TopicCommandOptions(Array("--topic", ConsumerCoordinator.OffsetsTopicName))
    val deleteOffsetTopicPath = ZkUtils.getDeleteTopicPath(ConsumerCoordinator.OffsetsTopicName)
    assertFalse("Delete path for topic shouldn't exist before deletion.", zkClient.exists(deleteOffsetTopicPath))
    intercept[AdminOperationException] {
        TopicCommand.deleteTopic(zkClient, deleteOffsetTopicOpts)
    }
    assertFalse("Delete path for topic shouldn't exist after deletion.", zkClient.exists(deleteOffsetTopicPath))
  }

  @Test
  def testCreateAlterTopicWithRackAware() {
    val brokers = 0 to 5
    TestUtils.createBrokersInZk(zkClient, brokers)

    val createOpts = new TopicCommandOptions(Array(
      "--partitions", "18",
      "--replication-factor", "3",
      "--rack-locator-class", "kafka.admin.SimpleRackLocator",
      "--rack-locator-properties", "0=rack1,1=rack2,2=rack2,3=rack1,4=rack3,5=rack3",
      "--topic", "foo"))
    TopicCommand.createTopic(zkClient, createOpts)

    var assignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, Seq("foo"))
      .map(p => p._1.partition -> p._2)
    val rackInfo: Map[Int, String] = Map(0 -> "rack1", 1 -> "rack2",2 -> "rack2",3 -> "rack1", 4 -> "rack3",5 -> "rack3")
    ensureRackAwareAndEvenDistribution(assignment, rackInfo, 6, 18, 3)

    // verify that adding partitions will also be rack aware
    val alterOpts = new TopicCommandOptions(Array(
      "--partitions", "36",
      "--rack-locator-class", "kafka.admin.SimpleRackLocator",
      "--rack-locator-properties", "0=rack1,1=rack2,2=rack2,3=rack1,4=rack3,5=rack3",
      "--topic", "foo"))
    TopicCommand.alterTopic(zkClient, alterOpts)
    assignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, Seq("foo"))
      .map(p => p._1.partition -> p._2)
    ensureRackAwareAndEvenDistribution(assignment, rackInfo, 6, 36, 3)
  }
}
