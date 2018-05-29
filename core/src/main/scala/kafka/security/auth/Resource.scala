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
package kafka.security.auth

object Resource {
  val Separator = ":"
  val ClusterResourceName = "kafka-cluster"
  val ClusterResource = new Resource(Cluster, Resource.ClusterResourceName)
  val ProducerIdResourceName = "producer-id"
  val WildCardResource = "*"

  def fromString(str: String): Resource = {
    str.split(Separator, 3) match {
      case Array(resourceType, resourceNameType, name, _*) => new Resource(ResourceType.fromString(resourceType), name, ResourceNameType.fromString(resourceNameType))
      case _ => throw new IllegalArgumentException("expected a string in format ResourceType:ResourceNameType:ResourceName but got " + str)
    }
  }
}

/**
 *
 * @param resourceType type of resource.
 * @param name name of the resource, for topic this will be topic name , for group it will be group name. For cluster type
 *             it will be a constant string kafka-cluster.
 * @param resourceNameType type of resource name: literal, wildcard-suffixed, etc.
 */
case class Resource(resourceType: ResourceType, name: String, resourceNameType: ResourceNameType) {

  def this(resourceType: ResourceType, name: String) {
    this(resourceType, name, Literal)
  }

  override def toString: String = {
    resourceType.name + Resource.Separator + resourceNameType.name + Resource.Separator + name
  }

  // TODO why does this class not hae equals() and hashCode()?
}

