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
package kafka.server

import java.net.Socket
import java.util.Collections
import kafka.api.{KafkaSasl, SaslSetup}
import kafka.server.SaslApiVersionsRequestTest.{kafkaClientSaslMechanism, kafkaServerSaslMechanisms}
import kafka.test.annotation.{ClusterTemplate, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.test.{ClusterConfig, ClusterGenerator, ClusterInstance}
import kafka.utils.JaasTestUtils
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.message.SaslHandshakeRequestData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.{ApiVersionsRequest, ApiVersionsResponse, SaslHandshakeRequest, SaslHandshakeResponse}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.server.config.KafkaSecurityConfigs
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{AfterEach, BeforeEach}

import scala.jdk.CollectionConverters._

object SaslApiVersionsRequestTest {
  val kafkaClientSaslMechanism = "PLAIN"
  val kafkaServerSaslMechanisms: Seq[String] = List("PLAIN")
  val controlPlaneListenerName = "CONTROL_PLANE"
  val securityProtocol = SecurityProtocol.SASL_PLAINTEXT

  def saslApiVersionsRequestClusterConfig(clusterGenerator: ClusterGenerator): Unit = {
    clusterGenerator.accept(ClusterConfig.defaultBuilder
      .securityProtocol(securityProtocol)
      .`type`(Type.ZK)
      .putSaslServerProperty(KafkaSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, kafkaClientSaslMechanism)
      .putSaslServerProperty(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, kafkaServerSaslMechanisms.mkString(","))
      .putSaslClientProperty(SaslConfigs.SASL_MECHANISM, kafkaClientSaslMechanism)
      // Configure control plane listener to make sure we have separate listeners for testing.
      .putServerProperty(KafkaConfig.ControlPlaneListenerNameProp, controlPlaneListenerName)
      .putServerProperty(KafkaConfig.ListenerSecurityProtocolMapProp, s"$controlPlaneListenerName:$securityProtocol,$securityProtocol:$securityProtocol")
      .putServerProperty("listeners", s"$securityProtocol://localhost:0,$controlPlaneListenerName://localhost:0")
      .putServerProperty(KafkaConfig.AdvertisedListenersProp, s"$securityProtocol://localhost:0,$controlPlaneListenerName://localhost:0")
      .build())
  }
}

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
class SaslApiVersionsRequestTest(cluster: ClusterInstance) extends AbstractApiVersionsRequestTest(cluster) {
  private var sasl: SaslSetup = _

  @BeforeEach
  def setupSasl(): Unit = {
    sasl = new SaslSetup() {}
    sasl.startSasl(sasl.jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), KafkaSasl, JaasTestUtils.KafkaServerContextName))
  }

  @ClusterTemplate("saslApiVersionsRequestClusterConfig")
  def testApiVersionsRequestBeforeSaslHandshakeRequest(): Unit = {
    val socket = IntegrationTestUtils.connect(cluster.brokerSocketServers().asScala.head, cluster.clientListener())
    try {
      val apiVersionsResponse = IntegrationTestUtils.sendAndReceive[ApiVersionsResponse](
        new ApiVersionsRequest.Builder().build(0), socket)
      validateApiVersionsResponse(apiVersionsResponse)
      sendSaslHandshakeRequestValidateResponse(socket)
    } finally {
      socket.close()
    }
  }

  @ClusterTemplate("saslApiVersionsRequestClusterConfig")
  def testApiVersionsRequestAfterSaslHandshakeRequest(): Unit = {
    val socket = IntegrationTestUtils.connect(cluster.brokerSocketServers().asScala.head, cluster.clientListener())
    try {
      sendSaslHandshakeRequestValidateResponse(socket)
      val response = IntegrationTestUtils.sendAndReceive[ApiVersionsResponse](
        new ApiVersionsRequest.Builder().build(0), socket)
      assertEquals(Errors.ILLEGAL_SASL_STATE.code, response.data.errorCode)
    } finally {
      socket.close()
    }
  }

  @ClusterTemplate("saslApiVersionsRequestClusterConfig")
  def testApiVersionsRequestWithUnsupportedVersion(): Unit = {
    val socket = IntegrationTestUtils.connect(cluster.brokerSocketServers().asScala.head, cluster.clientListener())
    try {
      val apiVersionsRequest = new ApiVersionsRequest.Builder().build(0)
      val apiVersionsResponse = sendUnsupportedApiVersionRequest(apiVersionsRequest)
      assertEquals(Errors.UNSUPPORTED_VERSION.code, apiVersionsResponse.data.errorCode)
      val apiVersionsResponse2 = IntegrationTestUtils.sendAndReceive[ApiVersionsResponse](
        new ApiVersionsRequest.Builder().build(0), socket)
      validateApiVersionsResponse(apiVersionsResponse2)
      sendSaslHandshakeRequestValidateResponse(socket)
    } finally {
      socket.close()
    }
  }

  @AfterEach
  def closeSasl(): Unit = {
    sasl.closeSasl()
  }

  private def sendSaslHandshakeRequestValidateResponse(socket: Socket): Unit = {
    val request = new SaslHandshakeRequest(new SaslHandshakeRequestData().setMechanism("PLAIN"),
      ApiKeys.SASL_HANDSHAKE.latestVersion)
    val response = IntegrationTestUtils.sendAndReceive[SaslHandshakeResponse](request, socket)
    assertEquals(Errors.NONE, response.error)
    assertEquals(Collections.singletonList("PLAIN"), response.enabledMechanisms)
  }
}
