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

package kafka.admin


import java.util.Properties

import joptsimple.{OptionParser, OptionSpec}
import kafka.api.{OffsetFetchRequest, OffsetFetchResponse, OffsetRequest, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.common.{TopicAndPartition, _}
import kafka.consumer.SimpleConsumer
import kafka.utils._
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConversions._
import scala.collection.{Set, mutable}

object ConsumerGroupCommand {

  def main(args: Array[String]) {
    val opts = new ConsumerGroupCommandOptions(args)

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "List all consumer groups, describe a consumer group, or delete consumer group info.")

    // should have exactly one action
    val actions = Seq(opts.listOpt, opts.describeOpt, opts.deleteOpt).count(opts.options.has _)
    if(actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --list, --describe, --delete")

    opts.checkArgs()

    val zkUtils = ZkUtils(opts.options.valueOf(opts.zkConnectOpt), 
                          30000,
                          30000,
                          JaasUtils.isZkSecurityEnabled())

    try {
      if (opts.options.has(opts.listOpt))
        list(zkUtils, opts)
      else if (opts.options.has(opts.describeOpt))
        describe(zkUtils, opts)
      else if (opts.options.has(opts.deleteOpt))
        delete(zkUtils, opts)
    } catch {
      case e: Throwable =>
        println("Error while executing consumer group command " + e.getMessage)
        println(Utils.stackTrace(e))
    } finally {
      zkUtils.close()
    }
  }

  def list(zkUtils: ZkUtils, opts: ConsumerGroupCommandOptions) {
    val useNewConsumer = opts.options.has(opts.newConsumerOpt)
    if (!useNewConsumer)
      zkUtils.getConsumerGroups().foreach(println)
    else {
      val adminClient = createAndGetAdminClient(opts)
      adminClient.listAllConsumerGroupsFlattened().foreach(x => println(x.groupId))
    }
  }

  def createAndGetAdminClient(opts: ConsumerGroupCommandOptions): AdminClient = {
    AdminClient.createSimplePlaintext(opts.options.valueOf(opts.bootstrapServerOpt))
  }

  def describe(zkUtils: ZkUtils, opts: ConsumerGroupCommandOptions) {
    val useNewConsumer = opts.options.has(opts.newConsumerOpt)
    val group = opts.options.valueOf(opts.groupOpt)
    val configs = parseConfigs(opts)
    val channelSocketTimeoutMs = configs.getProperty("channelSocketTimeoutMs", "600").toInt
    val channelRetryBackoffMs = configs.getProperty("channelRetryBackoffMsOpt", "300").toInt
    def warnNoTopicsForGroupFound: Unit = {
      println("No topic available for consumer group provided")
    }

    println("%s, %s, %s, %s, %s, %s, %s"
      .format("GROUP", "TOPIC", "PARTITION", "CURRENT OFFSET", "LOG END OFFSET", "LAG", "OWNER"))

    if (!useNewConsumer) {
      val topics = zkUtils.getTopicsByConsumerGroup(group)
      if (topics.isEmpty) {
        warnNoTopicsForGroupFound
      }
      topics.foreach(topic => describeTopic(zkUtils, group, topic, channelSocketTimeoutMs, channelRetryBackoffMs, opts))
    } else {
      val (owners, groupAndTopicPartitions) = createAndGetAdminClient(opts).describeConsumerGroup(group)

      if (groupAndTopicPartitions.isEmpty)
        warnNoTopicsForGroupFound
      groupAndTopicPartitions.foreach(x => describeTopicPartition(zkUtils, group, channelSocketTimeoutMs, channelRetryBackoffMs, opts, x._2.map(tp => new TopicAndPartition(tp.topic(), tp.partition())), owners))
    }
  }

  def delete(zkUtils: ZkUtils, opts: ConsumerGroupCommandOptions) {
    if (opts.options.has(opts.groupOpt) && opts.options.has(opts.topicOpt)) {
      deleteForTopic(zkUtils, opts)
    }
    else if (opts.options.has(opts.groupOpt)) {
      deleteForGroup(zkUtils, opts)
    }
    else if (opts.options.has(opts.topicOpt)) {
      deleteAllForTopic(zkUtils, opts)
    }
  }

  private def deleteForGroup(zkUtils: ZkUtils, opts: ConsumerGroupCommandOptions) {
    val groups = opts.options.valuesOf(opts.groupOpt)
    groups.foreach { group =>
      try {
        if (AdminUtils.deleteConsumerGroupInZK(zkUtils, group))
          println("Deleted all consumer group information for group %s in zookeeper.".format(group))
        else
          println("Delete for group %s failed because its consumers are still active.".format(group))
      }
      catch {
        case e: ZkNoNodeException =>
          println("Delete for group %s failed because group does not exist.".format(group))
      }
    }
  }

  private def deleteForTopic(zkUtils: ZkUtils, opts: ConsumerGroupCommandOptions) {
    val groups = opts.options.valuesOf(opts.groupOpt)
    val topic = opts.options.valueOf(opts.topicOpt)
    Topic.validate(topic)
    groups.foreach { group =>
      try {
        if (AdminUtils.deleteConsumerGroupInfoForTopicInZK(zkUtils, group, topic))
          println("Deleted consumer group information for group %s topic %s in zookeeper.".format(group, topic))
        else
          println("Delete for group %s topic %s failed because its consumers are still active.".format(group, topic))
      }
      catch {
        case e: ZkNoNodeException =>
          println("Delete for group %s topic %s failed because group does not exist.".format(group, topic))
      }
    }
  }

  private def deleteAllForTopic(zkUtils: ZkUtils, opts: ConsumerGroupCommandOptions) {
    val topic = opts.options.valueOf(opts.topicOpt)
    Topic.validate(topic)
    AdminUtils.deleteAllConsumerGroupInfoForTopicInZK(zkUtils, topic)
    println("Deleted consumer group information for all inactive consumer groups for topic %s in zookeeper.".format(topic))
  }

  private def parseConfigs(opts: ConsumerGroupCommandOptions): Properties = {
    val configsToBeAdded = opts.options.valuesOf(opts.configOpt).map(_.split("""\s*=\s*"""))
    require(configsToBeAdded.forall(config => config.length == 2),
            "Invalid config: all configs to be added must be in the format \"key=val\".")
    val props = new Properties
    configsToBeAdded.foreach(pair => props.setProperty(pair(0).trim, pair(1).trim))
    props
  }

  private def describeTopic(zkUtils: ZkUtils,
                            group: String,
                            topic: String,
                            channelSocketTimeoutMs: Int,
                            channelRetryBackoffMs: Int,
                            opts: ConsumerGroupCommandOptions) {
    val topicPartitions = getTopicPartitions(zkUtils, topic)
    describeTopicPartition(zkUtils, group, channelSocketTimeoutMs, channelRetryBackoffMs, opts, topicPartitions)
  }

  def describeTopicPartition(zkUtils: ZkUtils, group: String, channelSocketTimeoutMs: Int, channelRetryBackoffMs: Int, opts: ConsumerGroupCommandOptions, topicPartitions: Seq[TopicAndPartition], owners: Map[TopicPartition, String] = null): Unit = {
    val partitionOffsets = getPartitionOffsets(zkUtils, group, topicPartitions, channelSocketTimeoutMs, channelRetryBackoffMs)
    topicPartitions
      .sortBy { case topicPartition => topicPartition.partition }
      .foreach { topicPartition =>
      describePartition(zkUtils, group, topicPartition.topic, topicPartition.partition, partitionOffsets.get(topicPartition), opts, owners)
    }
  }

  private def getTopicPartitions(zkUtils: ZkUtils, topic: String) = {
    val topicPartitionMap = zkUtils.getPartitionsForTopics(Seq(topic))
    val partitions = topicPartitionMap.getOrElse(topic, Seq.empty)
    partitions.map(TopicAndPartition(topic, _))
  }

  private def getPartitionOffsets(zkUtils: ZkUtils,
                                  group: String,
                                  topicPartitions: Seq[TopicAndPartition],
                                  channelSocketTimeoutMs: Int,
                                  channelRetryBackoffMs: Int): Map[TopicAndPartition, Long] = {
    val offsetMap = mutable.Map[TopicAndPartition, Long]()
    val channel = ClientUtils.channelToOffsetManager(group, zkUtils, channelSocketTimeoutMs, channelRetryBackoffMs)
    channel.send(OffsetFetchRequest(group, topicPartitions))
    val offsetFetchResponse = OffsetFetchResponse.readFrom(channel.receive().payload())

    offsetFetchResponse.requestInfo.foreach { case (topicAndPartition, offsetAndMetadata) =>
      if (offsetAndMetadata == OffsetMetadataAndError.NoOffset) {
        val topicDirs = new ZKGroupTopicDirs(group, topicAndPartition.topic)
        // this group may not have migrated off zookeeper for offsets storage (we don't expose the dual-commit option in this tool
        // (meaning the lag may be off until all the consumers in the group have the same setting for offsets storage)
        try {
          val offset = zkUtils.readData(topicDirs.consumerOffsetDir + "/" + topicAndPartition.partition)._1.toLong
          offsetMap.put(topicAndPartition, offset)
        } catch {
          case z: ZkNoNodeException =>
            println("Could not fetch offset from zookeeper for group %s partition %s due to missing offset data in zookeeper."
              .format(group, topicAndPartition))
        }
      }
      else if (offsetAndMetadata.error == ErrorMapping.NoError)
        offsetMap.put(topicAndPartition, offsetAndMetadata.offset)
      else
        println("Could not fetch offset from kafka for group %s partition %s due to %s."
          .format(group, topicAndPartition, ErrorMapping.exceptionFor(offsetAndMetadata.error)))
    }
    channel.disconnect()
    offsetMap.toMap
  }

  private def describePartition(zkUtils: ZkUtils,
                                group: String,
                                topic: String,
                                partition: Int,
                                offsetOpt: Option[Long],
                                opts: ConsumerGroupCommandOptions,
                                owners: Map[TopicPartition, String] = null) {
    val topicPartition = new TopicPartition(topic, partition)
    val groupDirs = new ZKGroupTopicDirs(group, topic)
    val useNewConsumer: Boolean = opts.options.has(opts.newConsumerOpt)
    val owner = if (useNewConsumer) owners.get(new TopicPartition(topic, partition)) else zkUtils.readDataMaybeNull(groupDirs.consumerOwnerDir + "/" + partition)._1
    def print(logEndOffset: Long): Unit = {
      val lag = offsetOpt.filter(_ != -1).map(logEndOffset - _)
      println("%s, %s, %s, %s, %s, %s, %s"
        .format(group, topic, partition, offsetOpt.getOrElse("unknown"), logEndOffset, lag.getOrElse("unknown"), owner.getOrElse("none")))
    }
    zkUtils.getLeaderForPartition(topic, partition) match {
      case Some(-1) =>
        println("%s, %s, %s, %s, %s, %s, %s"
          .format(group, topic, partition, offsetOpt.getOrElse("unknown"), "unknown", "unknown", owner.getOrElse("none")))
      case Some(brokerId) =>
        if (useNewConsumer) {
          val consumerOpt = getNewConsumer(zkUtils, brokerId)
          consumerOpt match {
            case Some(consumer) =>
              consumer.assign(List(topicPartition))
              consumer.seekToEnd(topicPartition)
              val logEndOffset = consumer.position(topicPartition)
              consumer.close()
              print(logEndOffset)
            case None => // ignore
          }
        } else {
          val consumerOpt = getZkConsumer(zkUtils, brokerId)
          consumerOpt match {
            case Some(consumer) =>
              val topicAndPartition: TopicAndPartition = new TopicAndPartition(topicPartition.topic(), topicPartition.partition())
              val request =
                OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
              val logEndOffset = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head
              consumer.close()
              print(logEndOffset)
            case None => // ignore
          }
        }
      case None =>
        println("No broker for partition %s".format(topicPartition))
    }
  }

  private def getNewConsumer(zkUtils: ZkUtils, brokerId: Int): Option[KafkaConsumer[String, String]] = {
    try {
      zkUtils.readDataMaybeNull(ZkUtils.BrokerIdsPath + "/" + brokerId)._1 match {
        case Some(brokerInfoString) =>
          Json.parseFull(brokerInfoString) match {
            case Some(m) =>
              val brokerInfo = m.asInstanceOf[Map[String, Any]]
              val host = brokerInfo.get("host").get.asInstanceOf[String]
              val port = brokerInfo.get("port").get.asInstanceOf[Int]
              val deserializer: String = (new StringDeserializer).getClass.getName
              val properties: Properties = new Properties()
              properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":" + port)
              properties.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerGroupCommand")
              properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
              properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
              properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer)
              properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)
              Some(new KafkaConsumer[String, String](properties))
            case None =>
              throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
          }
        case None =>
          throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
      }
    } catch {
      case t: Throwable =>
        println("Could not parse broker info due to " + t.getMessage)
        None
    }
  }

  private def getZkConsumer(zkUtils: ZkUtils, brokerId: Int): Option[SimpleConsumer] = {
    try {
      zkUtils.readDataMaybeNull(ZkUtils.BrokerIdsPath + "/" + brokerId)._1 match {
        case Some(brokerInfoString) =>
          Json.parseFull(brokerInfoString) match {
            case Some(m) =>
              val brokerInfo = m.asInstanceOf[Map[String, Any]]
              val host = brokerInfo.get("host").get.asInstanceOf[String]
              val port = brokerInfo.get("port").get.asInstanceOf[Int]
              Some(new SimpleConsumer(host, port, 10000, 100000, "ConsumerGroupCommand"))
            case None =>
              throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
          }
        case None =>
          throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId))
      }
    } catch {
      case t: Throwable =>
        println("Could not parse broker info due to " + t.getMessage)
        None
    }
  }

  class ConsumerGroupCommandOptions(args: Array[String]) {
    val ZkConnectDoc = "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
      "Multiple URLS can be given to allow fail-over."
    val BootstrapServerDoc = "REQUIRED (only when using new-consumer): The server to connect to."
    val GroupDoc = "The consumer group we wish to act on."
    val TopicDoc = "The topic whose consumer group information should be deleted."
    val ConfigDoc = "Configuration for timeouts. For instance --config channelSocketTimeoutMs=600"
    val ListDoc = "List all consumer groups."
    val DescribeDoc = "Describe consumer group and list offset lag related to given group."
    val nl = System.getProperty("line.separator")
    val DeleteDoc = "Pass in groups to delete topic partition offsets and ownership information " +
      "over the entire consumer group. For instance --group g1 --group g2" + nl +
      "Pass in groups with a single topic to just delete the given topic's partition offsets and ownership " +
      "information for the given consumer groups. For instance --group g1 --group g2 --topic t1" + nl +
      "Pass in just a topic to delete the given topic's partition offsets and ownership information " +
      "for every consumer group. For instance --topic t1" + nl +
      "WARNING: Group deletion only works for old ZK-based consumer groups, and one has to use it carefully to only delete groups that are not active."
    val NewConsumerDoc = "Use new consumer."
    val parser = new OptionParser
    val zkConnectOpt = parser.accepts("zookeeper", ZkConnectDoc)
                             .withRequiredArg
                             .describedAs("urls")
                             .ofType(classOf[String])
    val bootstrapServerOpt = parser.accepts("bootstrap-server", BootstrapServerDoc)
                                   .withRequiredArg
                                   .describedAs("server to connect to")
                                   .ofType(classOf[String])
    val groupOpt = parser.accepts("group", GroupDoc)
                         .withRequiredArg
                         .describedAs("consumer group")
                         .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", TopicDoc)
                         .withRequiredArg
                         .describedAs("topic")
                         .ofType(classOf[String])
    val configOpt = parser.accepts("config", ConfigDoc)
                          .withRequiredArg
                          .describedAs("name=value")
                          .ofType(classOf[String])
    val listOpt = parser.accepts("list", ListDoc)
    val describeOpt = parser.accepts("describe", DescribeDoc)
    val deleteOpt = parser.accepts("delete", DeleteDoc)
    val newConsumerOpt = parser.accepts("new-consumer", NewConsumerDoc)
    val options = parser.parse(args : _*)

    val allConsumerGroupLevelOpts: Set[OptionSpec[_]] = Set(listOpt, describeOpt, deleteOpt)

    def checkArgs() {
      // check required args
      if (options.has(newConsumerOpt)) {
        CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)
        if (options.has(deleteOpt))
          CommandLineUtils.printUsageAndDie(parser, "Option %s does not work with %s".format(deleteOpt, newConsumerOpt))
      } else
        CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt)
      if (options.has(describeOpt))
        CommandLineUtils.checkRequiredArgs(parser, options, groupOpt)
      if (options.has(deleteOpt) && !options.has(groupOpt) && !options.has(topicOpt))
        CommandLineUtils.printUsageAndDie(parser, "Option %s either takes %s, %s, or both".format(deleteOpt, groupOpt, topicOpt))

      // check invalid args
      CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, allConsumerGroupLevelOpts - describeOpt - deleteOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, topicOpt, allConsumerGroupLevelOpts - deleteOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, configOpt, allConsumerGroupLevelOpts - describeOpt)
    }
  }
}
