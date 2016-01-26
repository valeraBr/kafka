/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import java.io.File
import org.apache.kafka.common.protocol.SecurityProtocol
import kafka.server.KafkaConfig
import org.junit.Test
import kafka.utils.TestUtils
import scala.collection.JavaConverters._

class SaslMultiMechanismConsumerTest extends BaseConsumerTest with SaslTestHarness {
  override protected val zkSaslEnabled = true
  override protected val kafkaClientSaslMechanism = "PLAIN"
  override protected val kafkaServerSaslMechanisms = List("GSSAPI", "PLAIN")
  override protected def allKafkaClientSaslMechanisms = List("PLAIN", "GSSAPI")
  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "true")
  override protected def securityProtocol = SecurityProtocol.SASL_SSL
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))
  override protected val saslProperties = Some(kafkaSaslProperties(kafkaClientSaslMechanism, kafkaServerSaslMechanisms))

  @Test
  def testMultipleBrokerMechanisms() {

    val plainProducer = producers(0)
    val plainConsumer = consumers(0)

    val gssapiSaslProperties = kafkaSaslProperties("GSSAPI", kafkaServerSaslMechanisms)
    val gssapiProducer = TestUtils.createNewProducer(brokerList,
                                               securityProtocol = this.securityProtocol,
                                               trustStoreFile = this.trustStoreFile,
                                               saslProperties = Some(gssapiSaslProperties))
    producers += gssapiProducer
    val gssapiConsumer = TestUtils.createNewConsumer(brokerList,
                                               securityProtocol = this.securityProtocol,
                                               trustStoreFile = this.trustStoreFile,
                                               saslProperties = Some(gssapiSaslProperties))
    consumers += gssapiConsumer
    val numRecords = 1000
    var startingOffset = 0

    // Test SASL/PLAIN producer and consumer
    sendRecords(plainProducer, numRecords, tp)
    plainConsumer.assign(List(tp).asJava)
    plainConsumer.seek(tp, 0)
    consumeAndVerifyRecords(consumer = plainConsumer, numRecords = numRecords, startingOffset = startingOffset)
    val plainCommitCallback = new CountConsumerCommitCallback()
    plainConsumer.commitAsync(plainCommitCallback)
    awaitCommitCallback(plainConsumer, plainCommitCallback)
    startingOffset += numRecords

    // Test SASL/GSSAPI producer and consumer
    sendRecords(gssapiProducer, numRecords, tp)
    gssapiConsumer.assign(List(tp).asJava)
    gssapiConsumer.seek(tp, startingOffset)
    consumeAndVerifyRecords(consumer = gssapiConsumer, numRecords = numRecords, startingOffset = startingOffset)
    val gssapiCommitCallback = new CountConsumerCommitCallback()
    gssapiConsumer.commitAsync(gssapiCommitCallback)
    awaitCommitCallback(gssapiConsumer, gssapiCommitCallback)
    startingOffset += numRecords

    // Test SASL/PLAIN producer and SASL/GSSAPI consumer
    sendRecords(plainProducer, numRecords, tp)
    gssapiConsumer.assign(List(tp).asJava)
    gssapiConsumer.seek(tp, startingOffset)
    consumeAndVerifyRecords(consumer = gssapiConsumer, numRecords = numRecords, startingOffset = startingOffset)
    startingOffset += numRecords

    // Test SASL/GSSAPI producer and SASL/PLAIN consumer
    sendRecords(gssapiProducer, numRecords, tp)
    plainConsumer.assign(List(tp).asJava)
    plainConsumer.seek(tp, startingOffset)
    consumeAndVerifyRecords(consumer = plainConsumer, numRecords = numRecords, startingOffset = startingOffset)

  }
}
