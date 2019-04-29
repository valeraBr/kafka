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
package kafka.log.remote

import java.util
import scala.collection.Set
import kafka.server.LogReadResult
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.{Configurable, TopicPartition}

class RemoteLogManager extends Configurable {
  var remoteStorageManager: RemoteStorageManager = _

  /**
   * Configure this class with the given key-value pairs
   */
  override def configure(configs: util.Map[String, _]): Unit = {
    remoteStorageManager = Class.forName(configs.get("remote.log.storage.manager.class").toString)
      .getDeclaredConstructor().newInstance().asInstanceOf[RemoteStorageManager]
    remoteStorageManager.configure(configs)
  }

  /**
   * Ask RLM to monitor the given TopicPartitions, and copy inactive Log
   * Segments to remote storage. RLM updates local index files once a
   * Log Segment in a TopicPartition is copied to Remote Storage
   */
  def addPartitions(topicPartitions: Set[TopicPartition]): Boolean = {
    false
  }

  /**
   * Stops copy of LogSegment if TopicPartition ownership is moved from a broker.
   */
  def removePartitions(topicPartitions: Set[TopicPartition]): Boolean = {
    false
  }

  /**
   * Read topic partition data from remote
   *
   * @param fetchMaxByes
   * @param hardMaxBytesLimit
   * @param readPartitionInfo
   * @return
   */
  def read(fetchMaxByes: Int, hardMaxBytesLimit: Boolean, readPartitionInfo: Seq[(TopicPartition, PartitionData)]): LogReadResult = {
    null
  }

  /**
   * Stops all the threads and closes the instance.
   */
  def shutdown(): Unit = {
  }

}

case class RemoteLogManagerConfig(remoteLogStorageEnable: Boolean,
                                  remoteLogStorageManager: String,
                                  remoteLogRetentionBytes: Long,
                                  remoteLogRetentionMillis: Long)
