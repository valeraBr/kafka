/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.security.auth

import kafka.common.{BaseEnum, KafkaException}
import org.apache.kafka.common.protocol.Errors

/**
 * ResourceTypes.
 */


sealed trait ResourceType extends BaseEnum { def error: Errors }

case object Cluster extends ResourceType {
  val name = "Cluster"
  val error = Errors.CLUSTER_AUTHORIZATION_FAILED
}

case object Broker extends ResourceType {
  val name = "Broker"
  val error = Errors.BROKER_AUTHORIZATION_FAILED
}

case object Topic extends ResourceType {
  val name = "Topic"
  val error = Errors.TOPIC_AUTHORIZATION_FAILED
}

case object Group extends ResourceType {
  val name = "Group"
  val error = Errors.GROUP_AUTHORIZATION_FAILED
}

case object ProducerTransactionalId extends ResourceType {
  val name = "ProducerTransactionalId"
  val error = Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED
}

case object ProducerIdResource extends ResourceType {
  val name = "ProducerIdResource"
  val error = Errors.PRODUCER_ID_AUTHORIZATION_FAILED
}

object ResourceType {

  def fromString(resourceType: String): ResourceType = {
    val rType = values.find(rType => rType.name.equalsIgnoreCase(resourceType))
    rType.getOrElse(throw new KafkaException(resourceType + " not a valid resourceType name. The valid names are " + values.mkString(",")))
  }

  def values: Seq[ResourceType] = List(Cluster, Topic, Group, ProducerTransactionalId, ProducerIdResource, Broker)
}
