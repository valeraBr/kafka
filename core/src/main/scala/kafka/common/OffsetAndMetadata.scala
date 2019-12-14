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

package kafka.common

import java.util.Optional

case class OffsetAndRange(var offset: Long, var lowerKey: Optional[Long], var upperKey: Optional[Long]) {

  override def toString: String = {
    s"OffsetAndMetadata(offset=$offset" +
      s", lowerKey=$lowerKey" +
      s", upperKey=$upperKey)"
  }
}

case class OffsetAndMetadata(offsetRanges: java.util.List[OffsetAndRange],
                             leaderEpoch: Optional[Integer],
                             metadata: String,
                             commitTimestamp: Long,
                             expireTimestamp: Option[Long]) {

  // cached min offsetAndRange
  var minOffsetAndRange = OffsetAndRange(Long.MaxValue, Optional.empty(), Optional.empty())

  override def toString: String = {
    s"OffsetAndMetadata(offset=$offsetRanges" +
      s", leaderEpoch=$leaderEpoch" +
      s", metadata=$metadata" +
      s", commitTimestamp=$commitTimestamp" +
      s", expireTimestamp=$expireTimestamp)"
  }

  def offset: Long = {
    import scala.collection.JavaConverters._

    for (offsetAndRange <- offsetRanges.asScala) {
      if (minOffsetAndRange.offset > offsetAndRange.offset)
        minOffsetAndRange = offsetAndRange
    }
    minOffsetAndRange.offset
  }

  def lowerKey: Long = if (minOffsetAndRange.lowerKey.isPresent) minOffsetAndRange.lowerKey.get else -1L

  def upperKey: Long = if (minOffsetAndRange.upperKey.isPresent) minOffsetAndRange.upperKey.get else -1L
}

object OffsetAndMetadata {
  val NoMetadata: String = ""

  def apply(offset: Long, metadata: String, commitTimestamp: Long): OffsetAndMetadata = {
    OffsetAndMetadata(java.util.Collections.singletonList(OffsetAndRange(offset, Optional.empty(), Optional.empty())), Optional.empty(), metadata, commitTimestamp, None)
  }

  def apply(offset: Long, metadata: String, commitTimestamp: Long, expireTimestamp: Long): OffsetAndMetadata = {
    OffsetAndMetadata(java.util.Collections.singletonList(OffsetAndRange(offset, Optional.empty(), Optional.empty())), Optional.empty(), metadata, commitTimestamp, Some(expireTimestamp))
  }

  def apply(offset: Long, leaderEpoch: Optional[Integer], metadata: String, commitTimestamp: Long): OffsetAndMetadata = {
    OffsetAndMetadata(java.util.Collections.singletonList(OffsetAndRange(offset, Optional.empty(), Optional.empty())), leaderEpoch, metadata, commitTimestamp, None)
  }
}
