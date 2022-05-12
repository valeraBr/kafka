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

package kafka.server

import kafka.api.Request
import kafka.server.AbstractFetcherThread.{ReplicaFetch, ResultWithPartitions}
import kafka.server.QuotaFactory.UnboundedQuota
import kafka.utils.Logging
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, RequestUtils}

import java.util
import java.util.Optional
import scala.collection.{Map, Seq, Set, mutable}
import scala.compat.java8.OptionConverters.RichOptionForJava8
import scala.jdk.CollectionConverters._

/**
 * Facilitates fetches from a local replica leader.
 *
 * @param brokerConfig A config file with broker related configurations
 * @param replicaMgr A ReplicaManager
 * @param quota The quota, used when building a fetch request
 */
class LocalLeaderEndPoint(brokerConfig: KafkaConfig,
                          replicaMgr: ReplicaManager,
                          quota: ReplicaQuota) extends LeaderEndPoint with Logging {

  private val replicaId = brokerConfig.brokerId
  private val maxBytes = brokerConfig.replicaFetchResponseMaxBytes
  private val fetchSize = brokerConfig.replicaFetchMaxBytes
  private var inProgressPartition: Option[TopicPartition] = None

  override val isTruncationOnFetchSupported: Boolean = false

  override def initiateClose(): Unit = {} // do nothing

  override def close(): Unit = {} // do nothing

  override def fetch(fetchRequest: FetchRequest.Builder): collection.Map[TopicPartition, FetchData] = {
    var partitionData: Seq[(TopicPartition, FetchData)] = null
    val request = fetchRequest.build()

    // We can build the map from the request since it contains topic IDs and names.
    // Only one ID can be associated with a name and vice versa.
    val topicNames = new mutable.HashMap[Uuid, String]()
    request.data.topics.forEach { topic =>
      topicNames.put(topic.topicId, topic.topic)
    }


    def processResponseCallback(responsePartitionData: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      partitionData = responsePartitionData.map { case (tp, data) =>
        val abortedTransactions = data.abortedTransactions.map(_.asJava).orNull
        val lastStableOffset = data.lastStableOffset.getOrElse(FetchResponse.INVALID_LAST_STABLE_OFFSET)
        tp.topicPartition -> new FetchResponseData.PartitionData()
          .setPartitionIndex(tp.topicPartition.partition)
          .setErrorCode(data.error.code)
          .setHighWatermark(data.highWatermark)
          .setLastStableOffset(lastStableOffset)
          .setLogStartOffset(data.logStartOffset)
          .setAbortedTransactions(abortedTransactions)
          .setRecords(data.records)
      }
    }

    val fetchData = request.fetchData(topicNames.asJava)

    val fetchParams = FetchParams(
      requestVersion = request.version,
      maxWaitMs = 0L, // timeout is 0 so that the callback will be executed immediately
      replicaId = Request.FutureLocalReplicaId,
      minBytes = request.minBytes,
      maxBytes = request.maxBytes,
      isolation = FetchLogEnd,
      clientMetadata = None
    )

    replicaMgr.fetchMessages(
      params = fetchParams,
      fetchInfos = fetchData.asScala.toSeq,
      quota = UnboundedQuota,
      responseCallback = processResponseCallback
    )

    if (partitionData == null)
      throw new IllegalStateException(s"Failed to fetch data for partitions ${fetchData.keySet().toArray.mkString(",")}")

    partitionData.toMap
  }

  override def fetchEarliestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.localLogOrException.logStartOffset
  }

  override def fetchLatestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.localLogOrException.logEndOffset
  }

  override def fetchEpochEndOffsets(partitions: collection.Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset] = {
    partitions.map { case (tp, epochData) =>
      try {
        val endOffset = if (epochData.leaderEpoch == UNDEFINED_EPOCH) {
          new EpochEndOffset()
            .setPartition(tp.partition)
            .setErrorCode(Errors.NONE.code)
        } else {
          val partition = replicaMgr.getPartitionOrException(tp)
          partition.lastOffsetForLeaderEpoch(
            currentLeaderEpoch = RequestUtils.getLeaderEpoch(epochData.currentLeaderEpoch),
            leaderEpoch = epochData.leaderEpoch,
            fetchOnlyFromLeader = false)
        }
        tp -> endOffset
      } catch {
        case t: Throwable =>
          warn(s"Error when getting EpochEndOffset for $tp", t)
          tp -> new EpochEndOffset()
            .setPartition(tp.partition)
            .setErrorCode(Errors.forException(t).code)
      }
    }
  }

  override def buildFetch(partitionMap: Map[TopicPartition, PartitionFetchState]): ResultWithPartitions[Option[ReplicaFetch]] = {
    // Only include replica in the fetch request if it is not throttled.
    if (quota.isQuotaExceeded) {
      ResultWithPartitions(None, Set.empty)
    } else {
      selectPartitionToFetch(partitionMap) match {
        case Some((tp, fetchState)) =>
          buildFetchForPartition(tp, fetchState)
        case None =>
          ResultWithPartitions(None, Set.empty)
      }
    }
  }

  private def selectPartitionToFetch(partitionMap: Map[TopicPartition, PartitionFetchState]): Option[(TopicPartition, PartitionFetchState)] = {
    // Only move one partition at a time to increase its catch-up rate and thus reduce the time spent on
    // moving any given replica. Replicas are selected in ascending order (lexicographically by topic) from the
    // partitions that are ready to fetch. Once selected, we will continue fetching the same partition until it
    // becomes unavailable or is removed.

    inProgressPartition.foreach { tp =>
      val fetchStateOpt = partitionMap.get(tp)
      fetchStateOpt.filter(_.isReadyForFetch).foreach { fetchState =>
        return Some((tp, fetchState))
      }
    }

    inProgressPartition = None

    val nextPartitionOpt = nextReadyPartition(partitionMap)
    nextPartitionOpt.foreach { case (tp, fetchState) =>
      inProgressPartition = Some(tp)
      info(s"Beginning/resuming copy of partition $tp from offset ${fetchState.fetchOffset}. " +
        s"Including this partition, there are ${partitionMap.size} remaining partitions to copy by this thread.")
    }
    nextPartitionOpt
  }

  private def buildFetchForPartition(tp: TopicPartition, fetchState: PartitionFetchState): ResultWithPartitions[Option[ReplicaFetch]] = {
    val requestMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val partitionsWithError = mutable.Set[TopicPartition]()

    try {
      val logStartOffset = replicaMgr.futureLocalLogOrException(tp).logStartOffset
      val lastFetchedEpoch = if (isTruncationOnFetchSupported)
        fetchState.lastFetchedEpoch.map(_.asInstanceOf[Integer]).asJava
      else
        Optional.empty[Integer]
      val topicId = fetchState.topicId.getOrElse(Uuid.ZERO_UUID)
      requestMap.put(tp, new FetchRequest.PartitionData(topicId, fetchState.fetchOffset, logStartOffset,
        fetchSize, Optional.of(fetchState.currentLeaderEpoch), lastFetchedEpoch))
    } catch {
      case e: KafkaStorageException =>
        debug(s"Failed to build fetch for $tp", e)
        partitionsWithError += tp
    }

    val fetchRequestOpt = if (requestMap.isEmpty) {
      None
    } else {
      val version: Short = if (fetchState.topicId.isEmpty)
        12
      else
        ApiKeys.FETCH.latestVersion
      // Set maxWait and minBytes to 0 because the response should return immediately if
      // the future log has caught up with the current log of the partition
      val requestBuilder = FetchRequest.Builder.forReplica(version, replicaId, 0, 0, requestMap).setMaxBytes(maxBytes)
      Some(ReplicaFetch(requestMap, requestBuilder))
    }

    ResultWithPartitions(fetchRequestOpt, partitionsWithError)
  }

  private def nextReadyPartition(partitionMap: Map[TopicPartition, PartitionFetchState]): Option[(TopicPartition, PartitionFetchState)] = {
    partitionMap.filter { case (_, partitionFetchState) =>
      partitionFetchState.isReadyForFetch
    }.reduceLeftOption { (left, right) =>
      if ((left._1.topic < right._1.topic) || (left._1.topic == right._1.topic && left._1.partition < right._1.partition))
        left
      else
        right
    }
  }

  override def toString: String = s"LocalLeaderEndPoint with ReplicaManager $replicaMgr"
}
