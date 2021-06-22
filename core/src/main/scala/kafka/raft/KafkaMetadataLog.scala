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
package kafka.raft

import kafka.api.ApiVersion
import kafka.log.{AppendOrigin, Log, LogConfig, LogOffsetSnapshot, LogSegment, RetentionSizeBreach}
import kafka.server.{BrokerTopicStats, FetchHighWatermark, FetchLogEnd, KafkaConfig, LogDirFailureChannel, RequestLocal}
import kafka.utils.{CoreUtils, Logging, Scheduler}
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{KafkaException, TopicPartition, Uuid}
import org.apache.kafka.raft.{Isolation, LogAppendInfo, LogFetchInfo, LogOffsetMetadata, OffsetAndEpoch, OffsetMetadata, ReplicatedLog, ValidOffsetAndEpoch}
import org.apache.kafka.snapshot.{FileRawSnapshotReader, FileRawSnapshotWriter, RawSnapshotReader, RawSnapshotWriter, SnapshotPath, Snapshots}

import java.io.File
import java.nio.file.{Files, NoSuchFileException, Path}
import java.util.{Optional, Properties}
import scala.annotation.nowarn
import scala.collection.mutable
import scala.compat.java8.OptionConverters._

final class KafkaMetadataLog private (
  val log: Log,
  time: Time,
  scheduler: Scheduler,
  // Access to this object needs to be synchronized because it is used by the snapshotting thread to notify the
  // polling thread when snapshots are created. This object is also used to store any opened snapshot reader.
  val snapshots: mutable.TreeMap[OffsetAndEpoch, Option[FileRawSnapshotReader]],
  topicPartition: TopicPartition,
  maxFetchSizeInBytes: Int,
  // Visible for testing
  val fileDeleteDelayMs: Long,
  val retentionSize: Long,
  val retentionMs: Long,
) extends ReplicatedLog with Logging {

  override def read(startOffset: Long, readIsolation: Isolation): LogFetchInfo = {
    val isolation = readIsolation match {
      case Isolation.COMMITTED => FetchHighWatermark
      case Isolation.UNCOMMITTED => FetchLogEnd
      case _ => throw new IllegalArgumentException(s"Unhandled read isolation $readIsolation")
    }

    val fetchInfo = log.read(startOffset,
      maxLength = maxFetchSizeInBytes,
      isolation = isolation,
      minOneMessage = true)

    new LogFetchInfo(
      fetchInfo.records,

      new LogOffsetMetadata(
        fetchInfo.fetchOffsetMetadata.messageOffset,
        Optional.of(SegmentPosition(
          fetchInfo.fetchOffsetMetadata.segmentBaseOffset,
          fetchInfo.fetchOffsetMetadata.relativePositionInSegment))
        )
    )
  }

  override def appendAsLeader(records: Records, epoch: Int): LogAppendInfo = {
    if (records.sizeInBytes == 0)
      throw new IllegalArgumentException("Attempt to append an empty record set")

    handleAndConvertLogAppendInfo(
      log.appendAsLeader(records.asInstanceOf[MemoryRecords],
        leaderEpoch = epoch,
        origin = AppendOrigin.RaftLeader,
        requestLocal = RequestLocal.NoCaching
      )
    )
  }

  override def appendAsFollower(records: Records): LogAppendInfo = {
    if (records.sizeInBytes == 0)
      throw new IllegalArgumentException("Attempt to append an empty record set")

    handleAndConvertLogAppendInfo(log.appendAsFollower(records.asInstanceOf[MemoryRecords]))
  }

  private def handleAndConvertLogAppendInfo(appendInfo: kafka.log.LogAppendInfo): LogAppendInfo = {
    appendInfo.firstOffset match {
      case Some(firstOffset) =>
        if (firstOffset.relativePositionInSegment == 0) {
          // Assume that a new segment was created if the relative position is 0
          log.deleteOldSegments()
        }
        new LogAppendInfo(firstOffset.messageOffset, appendInfo.lastOffset)
      case None =>
        throw new KafkaException(s"Append failed unexpectedly: ${appendInfo.errorMessage}")
    }
  }

  override def lastFetchedEpoch: Int = {
    log.latestEpoch.getOrElse {
      latestSnapshotId().map[Int] { snapshotId =>
        val logEndOffset = endOffset().offset
        if (snapshotId.offset == startOffset && snapshotId.offset == logEndOffset) {
          // Return the epoch of the snapshot when the log is empty
          snapshotId.epoch
        } else {
          throw new KafkaException(
            s"Log doesn't have a last fetch epoch and there is a snapshot ($snapshotId). " +
            s"Expected the snapshot's end offset to match the log's end offset ($logEndOffset) " +
            s"and the log start offset ($startOffset)"
          )
        }
      }.orElse(0)
    }
  }

  override def endOffsetForEpoch(epoch: Int): OffsetAndEpoch = {
    (log.endOffsetForEpoch(epoch), earliestSnapshotId().asScala) match {
      case (Some(offsetAndEpoch), Some(snapshotId)) if (
        offsetAndEpoch.offset == snapshotId.offset &&
        offsetAndEpoch.leaderEpoch == epoch) =>

        // The epoch is smaller than the smallest epoch on the log. Override the diverging
        // epoch to the oldest snapshot which should be the snapshot at the log start offset
        new OffsetAndEpoch(snapshotId.offset, snapshotId.epoch)

      case (Some(offsetAndEpoch), _) =>
        new OffsetAndEpoch(offsetAndEpoch.offset, offsetAndEpoch.leaderEpoch)

      case (None, _) =>
        new OffsetAndEpoch(endOffset.offset, lastFetchedEpoch)
    }
  }

  override def endOffset: LogOffsetMetadata = {
    val endOffsetMetadata = log.logEndOffsetMetadata
    new LogOffsetMetadata(
      endOffsetMetadata.messageOffset,
      Optional.of(SegmentPosition(
        endOffsetMetadata.segmentBaseOffset,
        endOffsetMetadata.relativePositionInSegment)
      )
    )
  }

  override def startOffset: Long = {
    log.logStartOffset
  }

  override def truncateTo(offset: Long): Unit = {
    if (offset < highWatermark.offset) {
      throw new IllegalArgumentException(s"Attempt to truncate to offset $offset, which is below " +
        s"the current high watermark ${highWatermark.offset}")
    }
    log.truncateTo(offset)
  }

  override def truncateToLatestSnapshot(): Boolean = {
    val latestEpoch = log.latestEpoch.getOrElse(0)
    val (truncated, forgottenSnapshots) = latestSnapshotId().asScala match {
      case Some(snapshotId) if (
          snapshotId.epoch > latestEpoch ||
          (snapshotId.epoch == latestEpoch && snapshotId.offset > endOffset().offset)
        ) =>
        // Truncate the log fully if the latest snapshot is greater than the log end offset
        log.truncateFullyAndStartAt(snapshotId.offset)

        // Forget snapshots less than the log start offset
        snapshots synchronized {
          (true, forgetSnapshotsBefore(snapshotId))
        }
      case _ =>
        (false, mutable.TreeMap.empty[OffsetAndEpoch, Option[FileRawSnapshotReader]])
    }

    removeSnapshots(forgottenSnapshots)
    truncated
  }

  override def initializeLeaderEpoch(epoch: Int): Unit = {
    log.maybeAssignEpochStartOffset(epoch, log.logEndOffset)
  }

  override def updateHighWatermark(offsetMetadata: LogOffsetMetadata): Unit = {
    offsetMetadata.metadata.asScala match {
      case Some(segmentPosition: SegmentPosition) => log.updateHighWatermark(
        new kafka.server.LogOffsetMetadata(
          offsetMetadata.offset,
          segmentPosition.baseOffset,
          segmentPosition.relativePosition)
      )
      case _ =>
        // FIXME: This API returns the new high watermark, which may be different from the passed offset
        log.updateHighWatermark(offsetMetadata.offset)
    }
  }

  override def highWatermark: LogOffsetMetadata = {
    val LogOffsetSnapshot(_, _, hwm, _) = log.fetchOffsetSnapshot
    val segmentPosition: Optional[OffsetMetadata] = if (hwm.messageOffsetOnly) {
      Optional.of(SegmentPosition(hwm.segmentBaseOffset, hwm.relativePositionInSegment))
    } else {
      Optional.empty()
    }

    new LogOffsetMetadata(hwm.messageOffset, segmentPosition)
  }

  override def flush(): Unit = {
    log.flush()
  }

  override def lastFlushedOffset(): Long = {
    log.recoveryPoint
  }

  /**
   * Return the topic partition associated with the log.
   */
  override def topicPartition(): TopicPartition = {
    topicPartition
  }

  /**
   * Return the topic ID associated with the log.
   */
  override def topicId(): Uuid = {
    log.topicId.get
  }

  override def createNewSnapshot(snapshotId: OffsetAndEpoch): Optional[RawSnapshotWriter] = {
    val highWatermarkOffset = highWatermark.offset
    if (snapshotId.offset > highWatermarkOffset) {
      throw new IllegalArgumentException(
        s"Cannot create a snapshot for an end offset ($endOffset) greater than the high-watermark ($highWatermarkOffset)"
      )
    }

    if (snapshotId.offset < startOffset) {
      throw new IllegalArgumentException(
        s"Cannot create a snapshot for an end offset ($endOffset) less than the log start offset ($startOffset)"
      )
    }

    val validOffsetAndEpoch = validateOffsetAndEpoch(snapshotId.offset, snapshotId.epoch)
    if (validOffsetAndEpoch.kind() != ValidOffsetAndEpoch.Kind.VALID) {
      throw new IllegalArgumentException(
        s"Snapshot id ($snapshotId) is not valid according to the log: $validOffsetAndEpoch"
      )
    }

    storeSnapshot(snapshotId)
  }

  override def storeSnapshot(snapshotId: OffsetAndEpoch): Optional[RawSnapshotWriter] = {
    if (snapshots.contains(snapshotId)) {
      Optional.empty()
    } else {
      Optional.of(FileRawSnapshotWriter.create(log.dir.toPath, snapshotId, Optional.of(this)))
    }
  }

  override def readSnapshot(snapshotId: OffsetAndEpoch): Optional[RawSnapshotReader] = {
    snapshots synchronized {
      val reader = snapshots.get(snapshotId) match {
        case None =>
          // Snapshot doesn't exists
          None
        case Some(None) =>
          // Snapshot exists but has never been read before
          try {
            val snapshotReader = Some(FileRawSnapshotReader.open(log.dir.toPath, snapshotId))
            snapshots.put(snapshotId, snapshotReader)
            snapshotReader
          } catch {
            case _: NoSuchFileException =>
              // Snapshot doesn't exists in the data dir; remove
              val path = Snapshots.snapshotPath(log.dir.toPath, snapshotId)
              warn(s"Couldn't read $snapshotId; expected to find snapshot file $path")
              snapshots.remove(snapshotId)
              None
          }
        case Some(value) =>
          // Snapshot exists and it is already open; do nothing
          value
      }

      reader.asJava.asInstanceOf[Optional[RawSnapshotReader]]
    }
  }

  override def latestSnapshotId(): Optional[OffsetAndEpoch] = {
    snapshots synchronized {
      snapshots.lastOption.map { case (snapshotId, _) => snapshotId }.asJava
    }
  }

  override def earliestSnapshotId(): Optional[OffsetAndEpoch] = {
    snapshots synchronized {
      snapshots.headOption.map { case (snapshotId, _) => snapshotId }.asJava
    }
  }

  override def onSnapshotFrozen(snapshotId: OffsetAndEpoch): Unit = {
    snapshots synchronized {
      snapshots.put(snapshotId, None)
    }
  }


  /**
   * Delete a given snapshot ID from our cache and schedule its removal from disk. If the given snapshot is also
   * the latest snapshot, this method does nothing since we must retain at least one snapshot at all times.
   * @param snapshotId
   * @return
   */
  private[raft] def deleteSnapshot(snapshotId: OffsetAndEpoch): Boolean = {
    val (deleted, forgottenSnapshot) = snapshots synchronized {
      val forgotten = mutable.TreeMap.empty[OffsetAndEpoch, Option[FileRawSnapshotReader]]
      latestSnapshotId().asScala match {
        case Some(latestSnapshot) if latestSnapshot != snapshotId =>
          snapshots.remove(snapshotId) match {
            case Some(value) =>
              forgotten.put(snapshotId, value)
              (true, forgotten)
            case None =>
              (false, forgotten)
          }
          case Some(latestSnapshot) =>
            debug(s"Refusing to delete the latest snapshot $latestSnapshot as this would cause divergence.")
            (false, forgotten)
          case None =>
            debug(s"No such snapshot $snapshotId to delete.")
            (false, forgotten)
      }
    }
    removeSnapshots(forgottenSnapshot)
    deleted
  }


  private def loadSnapshotSizes(): Seq[(OffsetAndEpoch, Long)] = {
    snapshots synchronized {
      snapshots.keys.toSeq.flatMap {
        snapshotId => readSnapshot(snapshotId).asScala.map { reader => (snapshotId, reader.sizeInBytes())}
      }
    }
  }

  override def maybeClean(): Boolean = {
    val snapshotSizes = loadSnapshotSizes()
    val logSize: Long = log.size
    var didClean = false
    var snapshotTotalSize: Long = snapshotSizes.map(_._2).sum

    // If we've violated retention, delete as many snapshots as necessary (leaving the newest)
    if (logSize + snapshotTotalSize > retentionSize) {
      snapshotSizes.take(snapshotSizes.size - 1).foreach { case (snapshotId, size) =>
        if (logSize + snapshotTotalSize > retentionSize) {
          snapshotTotalSize -= size
          deleteSnapshot(snapshotId)
          didClean = true
        }
      }
    }

    // If retention is still violated, clean up old segments as long as they precede our latest snapshot
    if (logSize + snapshotTotalSize > retentionSize) {
      latestSnapshotId().ifPresent { snapshotId =>
        var diff = log.size - retentionSize

        def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]): Boolean = {
          // Only delete this segment if the _next_ segment is less than the latest snapshot
          if (nextSegmentOpt.exists(_.baseOffset <= snapshotId.offset)) {
            if (diff - segment.size >= 0) {
              diff -= segment.size
              true
            } else {
              false
            }
          } else {
            false
          }
        }

        if (log.deleteOldSegments(shouldDelete, RetentionSizeBreach) > 0) {
          didClean = true
        }
      }
    }

    didClean
  }

  /**
   * Forget the snapshots earlier than a given snapshot id and return the associated
   * snapshot readers.
   *
   * This method assumes that the lock for `snapshots` is already held.
   */
  @nowarn("cat=deprecation") // Needed for TreeMap.until
  private def forgetSnapshotsBefore(
    logStartSnapshotId: OffsetAndEpoch
  ): mutable.TreeMap[OffsetAndEpoch, Option[FileRawSnapshotReader]] = {
    val expiredSnapshots = snapshots.until(logStartSnapshotId).clone()
    snapshots --= expiredSnapshots.keys

    expiredSnapshots
  }

  /**
   * Rename the given snapshots on the log directory. Asynchronously, close and delete the
   * given snapshots after some delay.
   */
  private def removeSnapshots(
    expiredSnapshots: mutable.TreeMap[OffsetAndEpoch, Option[FileRawSnapshotReader]]
  ): Unit = {
    expiredSnapshots.foreach { case (snapshotId, _) =>
      Snapshots.markForDelete(log.dir.toPath, snapshotId)
    }

    if (expiredSnapshots.nonEmpty) {
      scheduler.schedule(
        "delete-snapshot-files",
        KafkaMetadataLog.deleteSnapshotFiles(log.dir.toPath, expiredSnapshots, this),
        fileDeleteDelayMs
      )
    }
  }

  override def close(): Unit = {
    log.close()
    snapshots synchronized {
      snapshots.values.flatten.foreach(_.close())
      snapshots.clear()
    }
  }
}

object MetadataLogConfig {
  def apply(abstractConfig: AbstractConfig, maxBatchSizeInBytes: Int, maxFetchSizeInBytes: Int): MetadataLogConfig = {
    new MetadataLogConfig(
      abstractConfig.getInt(KafkaConfig.MetadataLogSegmentBytesProp),
      abstractConfig.getLong(KafkaConfig.MetadataLogSegmentMillisProp),
      abstractConfig.getLong(KafkaConfig.MetadataMaxRetentionBytesProp),
      abstractConfig.getLong(KafkaConfig.MetadataMaxRetentionMillisProp),
      maxBatchSizeInBytes,
      maxFetchSizeInBytes
    )
  }
}

case class MetadataLogConfig(logSegmentBytes: Int,
                             logSegmentMillis: Long,
                             retentionMaxBytes: Long,
                             retentionMillis: Long,
                             maxBatchSizeInBytes: Int,
                             maxFetchSizeInBytes: Int)

object KafkaMetadataLog {
  def apply(
    topicPartition: TopicPartition,
    topicId: Uuid,
    dataDir: File,
    time: Time,
    scheduler: Scheduler,
    config: MetadataLogConfig
  ): KafkaMetadataLog = {
    val props = new Properties()
    props.put(LogConfig.MaxMessageBytesProp, config.maxBatchSizeInBytes.toString)
    props.put(LogConfig.MessageFormatVersionProp, ApiVersion.latestVersion.toString)
    props.put(LogConfig.SegmentBytesProp, Int.box(config.logSegmentBytes))
    props.put(LogConfig.SegmentMsProp, Long.box(config.logSegmentMillis))

    LogConfig.validateValues(props)
    val defaultLogConfig = LogConfig(props)

    val log = Log(
      dir = dataDir,
      config = defaultLogConfig,
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = scheduler,
      brokerTopicStats = new BrokerTopicStats,
      time = time,
      maxProducerIdExpirationMs = Int.MaxValue,
      producerIdExpirationCheckIntervalMs = Int.MaxValue,
      logDirFailureChannel = new LogDirFailureChannel(5),
      lastShutdownClean = false,
      topicId = Some(topicId),
      keepPartitionMetadataFile = true
    )

    val metadataLog = new KafkaMetadataLog(
      log,
      time,
      scheduler,
      recoverSnapshots(log),
      topicPartition,
      config.maxFetchSizeInBytes,
      defaultLogConfig.fileDeleteDelayMs,
      config.retentionMaxBytes,
      config.retentionMillis
    )

    // When recovering, truncate fully if the latest snapshot is after the log end offset. This can happen to a follower
    // when the follower crashes after downloading a snapshot from the leader but before it could truncate the log fully.
    metadataLog.truncateToLatestSnapshot()

    metadataLog
  }

  private def recoverSnapshots(
    log: Log
  ): mutable.TreeMap[OffsetAndEpoch, Option[FileRawSnapshotReader]] = {
    val snapshots = mutable.TreeMap.empty[OffsetAndEpoch, Option[FileRawSnapshotReader]]
    // Scan the log directory; deleting partial snapshots and older snapshot, only remembering immutable snapshots start
    // from logStartOffset
    Files
      .walk(log.dir.toPath, 1)
      .map[Optional[SnapshotPath]] { path =>
        if (path != log.dir.toPath) {
          Snapshots.parse(path)
        } else {
          Optional.empty()
        }
      }
      .forEach { path =>
        path.ifPresent { snapshotPath =>
          if (snapshotPath.partial ||
            snapshotPath.deleted ||
            snapshotPath.snapshotId.offset < log.logStartOffset) {
            // Delete partial snapshot, deleted snapshot and older snapshot
            Files.deleteIfExists(snapshotPath.path)
          } else {
            snapshots.put(snapshotPath.snapshotId, None)
          }
        }
      }
    snapshots
  }

  private def deleteSnapshotFiles(
    logDir: Path,
    expiredSnapshots: mutable.TreeMap[OffsetAndEpoch, Option[FileRawSnapshotReader]],
    logging: Logging
  ): () => Unit = () => {
    expiredSnapshots.foreach { case (snapshotId, snapshotReader) =>
      snapshotReader.foreach { reader =>
        CoreUtils.swallow(reader.close(), logging)
      }
      Snapshots.deleteIfExists(logDir, snapshotId)
    }
  }
}
