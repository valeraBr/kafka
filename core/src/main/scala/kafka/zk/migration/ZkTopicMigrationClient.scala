package kafka.zk.migration

import kafka.api.LeaderAndIsr
import kafka.controller.{LeaderIsrAndControllerEpoch, ReplicaAssignment}
import kafka.server.ConfigType
import kafka.utils.Logging
import kafka.zk.TopicZNode.TopicIdReplicaAssignment
import kafka.zk.ZkMigrationClient.wrapZkException
import kafka.zk._
import kafka.zookeeper.{CreateRequest, DeleteRequest, GetChildrenRequest, SetDataRequest}
import org.apache.kafka.common.metadata.PartitionRecord
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.metadata.migration.TopicMigrationClient.TopicVisitorInterest
import org.apache.kafka.metadata.migration.{MigrationClientException, TopicMigrationClient, ZkMigrationLeadershipState}
import org.apache.kafka.metadata.{LeaderRecoveryState, PartitionRegistration}
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.Code

import java.util
import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._


class ZkTopicMigrationClient(zkClient: KafkaZkClient) extends TopicMigrationClient with Logging {
  override def iterateTopics(
    interests: util.EnumSet[TopicVisitorInterest],
    visitor: TopicMigrationClient.TopicVisitor,
  ): Unit = wrapZkException {
    if (!interests.contains(TopicVisitorInterest.TOPICS)) {
      throw new IllegalArgumentException("Must specify at least TOPICS in topic visitor interests.")
    }
    val topics = zkClient.getAllTopicsInCluster()
    val topicConfigs = zkClient.getEntitiesConfigs(ConfigType.Topic, topics)
    val replicaAssignmentAndTopicIds = zkClient.getReplicaAssignmentAndTopicIdForTopics(topics)
    replicaAssignmentAndTopicIds.foreach { case TopicIdReplicaAssignment(topic, topicIdOpt, partitionAssignments) =>
      val topicAssignment = partitionAssignments.map { case (partition, assignment) =>
        partition.partition().asInstanceOf[Integer] -> assignment.replicas.map(Integer.valueOf).asJava
      }.toMap.asJava
      visitor.visitTopic(topic, topicIdOpt.get, topicAssignment)
      if (interests.contains(TopicVisitorInterest.PARTITIONS)) {
        val partitions = partitionAssignments.keys.toSeq
        val leaderIsrAndControllerEpochs = zkClient.getTopicPartitionStates(partitions)
        partitionAssignments.foreach { case (topicPartition, replicaAssignment) =>
          val replicaList = replicaAssignment.replicas.map(Integer.valueOf).asJava
          val record = new PartitionRecord()
            .setTopicId(topicIdOpt.get)
            .setPartitionId(topicPartition.partition)
            .setReplicas(replicaList)
            .setAddingReplicas(replicaAssignment.addingReplicas.map(Integer.valueOf).asJava)
            .setRemovingReplicas(replicaAssignment.removingReplicas.map(Integer.valueOf).asJava)
          leaderIsrAndControllerEpochs.get(topicPartition) match {
            case Some(leaderIsrAndEpoch) =>
              record
                .setIsr(leaderIsrAndEpoch.leaderAndIsr.isr.map(Integer.valueOf).asJava)
                .setLeader(leaderIsrAndEpoch.leaderAndIsr.leader)
                .setLeaderEpoch(leaderIsrAndEpoch.leaderAndIsr.leaderEpoch)
                .setPartitionEpoch(leaderIsrAndEpoch.leaderAndIsr.partitionEpoch)
                .setLeaderRecoveryState(leaderIsrAndEpoch.leaderAndIsr.leaderRecoveryState.value())
            case None =>
              warn(s"Could not find partition state in ZK for $topicPartition. Initializing this partition " +
                s"with ISR={$replicaList} and leaderEpoch=0.")
              record
                .setIsr(replicaList)
                .setLeader(replicaList.get(0))
                .setLeaderEpoch(0)
                .setPartitionEpoch(0)
                .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value())
          }
          visitor.visitPartition(new TopicIdPartition(topicIdOpt.get, topicPartition), new PartitionRegistration(record))
        }
      }
      if (interests.contains(TopicVisitorInterest.CONFIGS)) {
        val props = topicConfigs(topic)
        visitor.visitConfigs(topic, props)
      }
    }
  }

  override def createTopic(
    topicName: String,
    topicId: Uuid,
    partitions: util.Map[Integer, PartitionRegistration],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {

    val assignments = partitions.asScala.map { case (partitionId, partition) =>
      new TopicPartition(topicName, partitionId) ->
        ReplicaAssignment(partition.replicas, partition.addingReplicas, partition.removingReplicas)
    }

    val createTopicZNode = {
      val path = TopicZNode.path(topicName)
      CreateRequest(
        path,
        TopicZNode.encode(Some(topicId), assignments),
        zkClient.defaultAcls(path),
        CreateMode.PERSISTENT)
    }
    val createPartitionsZNode = {
      val path = TopicPartitionsZNode.path(topicName)
      CreateRequest(
        path,
        null,
        zkClient.defaultAcls(path),
        CreateMode.PERSISTENT)
    }

    val createPartitionZNodeReqs = partitions.asScala.flatMap { case (partitionId, partition) =>
      val topicPartition = new TopicPartition(topicName, partitionId)
      Seq(
        createTopicPartition(topicPartition),
        createTopicPartitionState(topicPartition, partition, state.kraftControllerEpoch())
      )
    }

    val requests = Seq(createTopicZNode, createPartitionsZNode) ++ createPartitionZNodeReqs
    val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(requests, state)
    val resultCodes = responses.map { response => response.path -> response.resultCode }.toMap
    if (resultCodes(TopicZNode.path(topicName)).equals(Code.NODEEXISTS)) {
      // topic already created, just return
      state
    } else if (resultCodes.forall { case (_, code) => code.equals(Code.OK) }) {
      // ok
      state.withMigrationZkVersion(migrationZkVersion)
    } else {
      // not ok
      throw new MigrationClientException(s"Failed to create or update topic $topicName. ZK operations had results $resultCodes")
    }
  }

  private def recursiveChildren(path: String, acc: ArrayBuffer[String]): Unit = {
    val topicChildZNodes = zkClient.retryRequestUntilConnected(GetChildrenRequest(path, registerWatch = false))
    topicChildZNodes.children.foreach { child =>
      recursiveChildren(s"$path/$child", acc)
      acc.addOne(s"$path/$child")
    }
  }

  private def recursiveChildren(path: String): Seq[String] = {
    val buffer = new ArrayBuffer[String]()
    recursiveChildren(path, buffer)
    buffer.toSeq
  }

  override def deleteTopic(
    topicName: String,
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
    // Delete the partition state ZNodes recursively, then topic config, and finally the topic znode
    val topicPath = TopicZNode.path(topicName)
    val topicChildZNodes = recursiveChildren(topicPath)
    val deleteRequests = topicChildZNodes.map { childPath =>
      DeleteRequest(childPath, ZkVersion.MatchAnyVersion)
    } ++ Seq(
      DeleteRequest(ConfigEntityZNode.path(ConfigType.Topic, topicName), ZkVersion.MatchAnyVersion),
      DeleteRequest(TopicZNode.path(topicName), ZkVersion.MatchAnyVersion)
    )
    val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(deleteRequests, state)
    val resultCodes = responses.map { response => response.path -> response.resultCode }.toMap
    if (responses.last.resultCode.equals(Code.OK)) {
      state.withMigrationZkVersion(migrationZkVersion)
    } else {
      throw new MigrationClientException(s"Failed to delete topic $topicName. ZK operations had results $resultCodes")
    }
  }

  override def updateTopicPartitions(
    topicPartitions: util.Map[String, util.Map[Integer, PartitionRegistration]],
    state: ZkMigrationLeadershipState
  ): ZkMigrationLeadershipState = wrapZkException {
    val requests = topicPartitions.asScala.flatMap { case (topicName, partitionRegistrations) =>
      partitionRegistrations.asScala.flatMap { case (partitionId, partitionRegistration) =>
        val topicPartition = new TopicPartition(topicName, partitionId)
        Seq(updateTopicPartitionState(topicPartition, partitionRegistration, state.kraftControllerEpoch()))
      }
    }
    if (requests.isEmpty) {
      state
    } else {
      val (migrationZkVersion, responses) = zkClient.retryMigrationRequestsUntilConnected(requests.toSeq, state)
      val resultCodes = responses.map { response => response.path -> response.resultCode }.toMap
      if (resultCodes.forall { case (_, code) => code.equals(Code.OK) }) {
        state.withMigrationZkVersion(migrationZkVersion)
      } else {
        throw new MigrationClientException(s"Failed to update partition states: $topicPartitions. ZK transaction had results $resultCodes")
      }
    }
  }

  private def createTopicPartition(
    topicPartition: TopicPartition
  ): CreateRequest = wrapZkException {
    val path = TopicPartitionZNode.path(topicPartition)
    CreateRequest(path, null, zkClient.defaultAcls(path), CreateMode.PERSISTENT, Some(topicPartition))
  }

  private def partitionStatePathAndData(
    topicPartition: TopicPartition,
    partitionRegistration: PartitionRegistration,
    controllerEpoch: Int
  ): (String, Array[Byte]) = {
    val path = TopicPartitionStateZNode.path(topicPartition)
    val data = TopicPartitionStateZNode.encode(LeaderIsrAndControllerEpoch(new LeaderAndIsr(
      partitionRegistration.leader,
      partitionRegistration.leaderEpoch,
      partitionRegistration.isr.toList,
      partitionRegistration.leaderRecoveryState,
      partitionRegistration.partitionEpoch), controllerEpoch))
    (path, data)
  }

  private def createTopicPartitionState(
    topicPartition: TopicPartition,
    partitionRegistration: PartitionRegistration,
    controllerEpoch: Int
  ): CreateRequest = {
    val (path, data) = partitionStatePathAndData(topicPartition, partitionRegistration, controllerEpoch)
    CreateRequest(path, data, zkClient.defaultAcls(path), CreateMode.PERSISTENT, Some(topicPartition))
  }

  private def updateTopicPartitionState(
    topicPartition: TopicPartition,
    partitionRegistration: PartitionRegistration,
    controllerEpoch: Int
  ): SetDataRequest = {
    val (path, data) = partitionStatePathAndData(topicPartition, partitionRegistration, controllerEpoch)
    SetDataRequest(path, data, ZkVersion.MatchAnyVersion, Some(topicPartition))
  }
}
