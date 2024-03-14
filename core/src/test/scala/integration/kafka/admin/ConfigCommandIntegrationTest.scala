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
package kafka.admin

import kafka.admin.ConfigCommand.ConfigCommandOptions
import kafka.cluster.{Broker, EndPoint}
import kafka.server.{KafkaBroker, KafkaConfig}
import kafka.test.annotation.{ClusterTest, ClusterTestDefaults, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.test.junit.ZkClusterInvocationContext.ZkClusterInstance
import kafka.test.ClusterInstance
import kafka.utils.{Exit, TestUtils}
import kafka.zk.{AdminZkClient, BrokerInfo}
import org.apache.kafka.clients.admin.{AlterConfigOp, AlterConfigsOptions, AlterConfigsResult, Config}
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.config.{ConfigException, ConfigResource}
import org.apache.kafka.common.errors.{InvalidConfigurationException, UnsupportedVersionException}
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.config.ConfigEntityName
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.Tag
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito

import java.util.{Collection, Collections, Map => JMap}
import java.util.concurrent.ExecutionException
import java.util.stream.Collectors
import scala.annotation.nowarn
import scala.collection.Seq
import scala.jdk.CollectionConverters._

@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(clusterType = Type.ALL, brokers = 1)
@Tag("integration")
class ConfigCommandIntegrationTest(cluster: ClusterInstance) {

  @ClusterTest(clusterType = Type.ZK)
  def shouldExitWithNonZeroStatusOnUpdatingUnallowedConfigViaZk(zkCluster: ClusterInstance): Unit = {
    assertNonZeroStatusExit(Array(
      "--zookeeper", zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "security.inter.broker.protocol=PLAINTEXT"))
  }

  @ClusterTest(clusterType = Type.ZK)
  def shouldExitWithNonZeroStatusOnZkCommandAlterUserQuota(zkCluster: ClusterInstance): Unit = {
    assertNonZeroStatusExit(Array(
      "--zookeeper", zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect,
      "--entity-type", "users",
      "--entity-name", "admin",
      "--alter", "--add-config", "consumer_byte_rate=20000"))
  }

  private def assertNonZeroStatusExit(args: Array[String]): Unit = {
    var exitStatus: Option[Int] = None
    Exit.setExitProcedure { (status, _) =>
      exitStatus = Some(status)
      throw new RuntimeException
    }

    try {
      ConfigCommand.main(args)
    } catch {
      case _: RuntimeException =>
    } finally {
      Exit.resetExitProcedure()
    }

    assertEquals(Some(1), exitStatus)
  }

  @ClusterTest(clusterType = Type.ZK)
  def testDynamicBrokerConfigUpdateUsingZooKeeper(zkCluster: ClusterInstance): Unit = {
    val zkClient = zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying().zkClient
    val zkConnect = zkCluster.asInstanceOf[ZkClusterInstance].getUnderlying.zkConnect
    val brokerId = cluster.anyBrokerSocketServer().config.brokerId.toString
    val adminZkClient = new AdminZkClient(zkClient)
    val alterOpts = Array("--zookeeper", zkConnect, "--entity-type", "brokers", "--alter")

    def entityOpt(brokerId: Option[String]): Array[String] = {
      brokerId.map(id => Array("--entity-name", id)).getOrElse(Array("--entity-default"))
    }

    def alterConfigWithZk(configs: Map[String, String], brokerId: Option[String],
                          encoderConfigs: Map[String, String] = Map.empty): Unit = {
      val configStr = (configs ++ encoderConfigs).map { case (k, v) => s"$k=$v" }.mkString(",")
      val addOpts = new ConfigCommandOptions(alterOpts ++ entityOpt(brokerId) ++ Array("--add-config", configStr))
      ConfigCommand.alterConfigWithZk(zkClient, addOpts, adminZkClient)
    }

    def verifyConfig(configs: Map[String, String], brokerId: Option[String]): Unit = {
      val entityConfigs = zkClient.getEntityConfigs("brokers", brokerId.getOrElse(ConfigEntityName.DEFAULT))
      assertEquals(configs, entityConfigs.asScala)
    }

    def alterAndVerifyConfig(configs: Map[String, String], brokerId: Option[String]): Unit = {
      alterConfigWithZk(configs, brokerId)
      verifyConfig(configs, brokerId)
    }

    def deleteAndVerifyConfig(configNames: Set[String], brokerId: Option[String]): Unit = {
      val deleteOpts = new ConfigCommandOptions(alterOpts ++ entityOpt(brokerId) ++
        Array("--delete-config", configNames.mkString(",")))
      ConfigCommand.alterConfigWithZk(zkClient, deleteOpts, adminZkClient)
      verifyConfig(Map.empty, brokerId)
    }

    // Broker configuration operations using ZooKeeper are only supported if the affected broker(s) are not running.
    cluster.shutdownBroker(brokerId.toInt)

    // Add config
    alterAndVerifyConfig(Map("message.max.size" -> "110000"), Some(brokerId))
    alterAndVerifyConfig(Map("message.max.size" -> "120000"), None)

    // Change config
    alterAndVerifyConfig(Map("message.max.size" -> "130000"), Some(brokerId))
    alterAndVerifyConfig(Map("message.max.size" -> "140000"), None)

    // Delete config
    deleteAndVerifyConfig(Set("message.max.size"), Some(brokerId))
    deleteAndVerifyConfig(Set("message.max.size"), None)

    // Listener configs: should work only with listener name
    alterAndVerifyConfig(Map("listener.name.external.ssl.keystore.location" -> "/tmp/test.jks"), Some(brokerId))
    assertThrows(classOf[ConfigException], () => alterConfigWithZk(Map("ssl.keystore.location" -> "/tmp/test.jks"), Some(brokerId)))

    // Per-broker config configured at default cluster-level should fail
    assertThrows(classOf[ConfigException], () => alterConfigWithZk(Map("listener.name.external.ssl.keystore.location" -> "/tmp/test.jks"), None))
    deleteAndVerifyConfig(Set("listener.name.external.ssl.keystore.location"), Some(brokerId))

    // Password config update without encoder secret should fail
    assertThrows(classOf[IllegalArgumentException], () => alterConfigWithZk(Map("listener.name.external.ssl.keystore.password" -> "secret"), Some(brokerId)))

    // Password config update with encoder secret should succeed and encoded password must be stored in ZK
    val configs = Map("listener.name.external.ssl.keystore.password" -> "secret", "log.cleaner.threads" -> "2")
    val encoderConfigs = Map(KafkaConfig.PasswordEncoderSecretProp -> "encoder-secret")
    alterConfigWithZk(configs, Some(brokerId), encoderConfigs)
    val brokerConfigs = zkClient.getEntityConfigs("brokers", brokerId)
    assertFalse(brokerConfigs.contains(KafkaConfig.PasswordEncoderSecretProp), "Encoder secret stored in ZooKeeper")
    assertEquals("2", brokerConfigs.getProperty("log.cleaner.threads")) // not encoded
    val encodedPassword = brokerConfigs.getProperty("listener.name.external.ssl.keystore.password")
    val passwordEncoder = ConfigCommand.createPasswordEncoder(encoderConfigs)
    assertEquals("secret", passwordEncoder.decode(encodedPassword).value)
    assertEquals(configs.size, brokerConfigs.size)

    // Password config update with overrides for encoder parameters
    val configs2 = Map("listener.name.internal.ssl.keystore.password" -> "secret2")
    val encoderConfigs2 = Map(KafkaConfig.PasswordEncoderSecretProp -> "encoder-secret",
      KafkaConfig.PasswordEncoderCipherAlgorithmProp -> "DES/CBC/PKCS5Padding",
      KafkaConfig.PasswordEncoderIterationsProp -> "1024",
      KafkaConfig.PasswordEncoderKeyFactoryAlgorithmProp -> "PBKDF2WithHmacSHA1",
      KafkaConfig.PasswordEncoderKeyLengthProp -> "64")
    alterConfigWithZk(configs2, Some(brokerId), encoderConfigs2)
    val brokerConfigs2 = zkClient.getEntityConfigs("brokers", brokerId)
    val encodedPassword2 = brokerConfigs2.getProperty("listener.name.internal.ssl.keystore.password")
    assertEquals("secret2", ConfigCommand.createPasswordEncoder(encoderConfigs).decode(encodedPassword2).value)
    assertEquals("secret2", ConfigCommand.createPasswordEncoder(encoderConfigs2).decode(encodedPassword2).value)


    // Password config update at default cluster-level should fail
    assertThrows(classOf[ConfigException], () => alterConfigWithZk(configs, None, encoderConfigs))

    // Dynamic config updates using ZK should fail if broker is running.
    registerBrokerInZk(zkClient, brokerId.toInt)
    assertThrows(classOf[IllegalArgumentException], () => alterConfigWithZk(Map("message.max.size" -> "210000"), Some(brokerId)))
    assertThrows(classOf[IllegalArgumentException], () => alterConfigWithZk(Map("message.max.size" -> "220000"), None))

    // Dynamic config updates using ZK should for a different broker that is not running should succeed
    alterAndVerifyConfig(Map("message.max.size" -> "230000"), Some("2"))
  }

  private def registerBrokerInZk(zkClient: kafka.zk.KafkaZkClient, id: Int): Unit = {
    zkClient.createTopLevelPaths()
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val endpoint = new EndPoint("localhost", 9092, ListenerName.forSecurityProtocol(securityProtocol), securityProtocol)
    val brokerInfo = BrokerInfo(Broker(id, Seq(endpoint), rack = None), MetadataVersion.latestTesting, jmxPort = 9192)
    zkClient.registerBroker(brokerInfo)
  }

  @ClusterTest
  def testUpdateInvalidBrokersConfig(): Unit = {
    checkInvalidBrokerConfig(None)
    checkInvalidBrokerConfig(Some(cluster.anyBrokerSocketServer().config.brokerId.toString))
  }

  private def checkInvalidBrokerConfig(entityNameOrDefault: Option[String]): Unit = {
    for (incremental <- Array(true, false)) {
      val entityNameParams = entityNameOrDefault.map(name => Array("--entity-name", name)).getOrElse(Array("--entity-default"))
      ConfigCommand.alterConfig(cluster.createAdminClient(), new ConfigCommandOptions(
        Array("--bootstrap-server", cluster.bootstrapServers(),
          "--alter",
          "--add-config", "invalid=2",
          "--entity-type", "brokers")
          ++ entityNameParams
      ), incremental)

      val describeResult = TestUtils.grabConsoleOutput(
        ConfigCommand.describeConfig(cluster.createAdminClient(), new ConfigCommandOptions(
          Array("--bootstrap-server", cluster.bootstrapServers(),
            "--describe",
            "--entity-type", "brokers")
            ++ entityNameParams
        )))
      // We will treat unknown config as sensitive
      assertTrue(describeResult.contains("sensitive=true"))
      // Sensitive config will not return
      assertTrue(describeResult.contains("invalid=null"))
    }
  }

  @ClusterTest
  def testUpdateInvalidTopicConfig(): Unit = {
    TestUtils.createTopicWithAdminRaw(
      admin = cluster.createAdminClient(),
      topic = "test-config-topic",
    )
    assertInstanceOf(
      classOf[InvalidConfigurationException],
      assertThrows(
        classOf[ExecutionException],
        () => ConfigCommand.alterConfig(cluster.createAdminClient(), new ConfigCommandOptions(
          Array("--bootstrap-server", cluster.bootstrapServers(),
            "--alter",
            "--add-config", "invalid=2",
            "--entity-type", "topics",
            "--entity-name", "test-config-topic")
        ))).getCause
    )
  }

  @ClusterTest
  def testUpdateAndDeleteBrokersConfig(): Unit = {
    checkBrokerConfig(None)
    checkBrokerConfig(Some(cluster.anyBrokerSocketServer().config.brokerId.toString))
  }

  private def checkBrokerConfig(entityNameOrDefault: Option[String]): Unit = {
    val entityNameParams = entityNameOrDefault.map(name => Array("--entity-name", name)).getOrElse(Array("--entity-default"))
    // add -> check -> delete -> check
    for (incremental <- Array(true, false)) {
      ConfigCommand.alterConfig(cluster.createAdminClient(), new ConfigCommandOptions(
        Array("--bootstrap-server", cluster.bootstrapServers(),
          "--alter",
          "--add-config", "log.cleaner.threads=2",
          "--entity-type", "brokers")
          ++ entityNameParams
      ), incremental)
      TestUtils.waitUntilTrue(
        () => cluster.brokerSocketServers().asScala.forall(broker => broker.config.getInt("log.cleaner.threads") == 2),
        "Timeout waiting for topic config propagating to broker")

      val describeResult = TestUtils.grabConsoleOutput(
        ConfigCommand.describeConfig(cluster.createAdminClient(), new ConfigCommandOptions(
          Array("--bootstrap-server", cluster.bootstrapServers(),
            "--describe",
            "--entity-type", "brokers")
            ++ entityNameParams
        )))
      assertTrue(describeResult.contains("log.cleaner.threads=2"))
      assertTrue(describeResult.contains("sensitive=false"))

      ConfigCommand.alterConfig(cluster.createAdminClient(), new ConfigCommandOptions(
        Array("--bootstrap-server", cluster.bootstrapServers(),
          "--alter",
          "--delete-config", "log.cleaner.threads",
          "--entity-type", "brokers")
          ++ entityNameParams
      ), incremental)
      TestUtils.waitUntilTrue(
        () => cluster.brokers().collect(Collectors.toList[KafkaBroker]).asScala.forall(broker => broker.config.getInt("log.cleaner.threads") != 2),
        "Timeout waiting for topic config propagating to broker")

      assertFalse(TestUtils.grabConsoleOutput(
        ConfigCommand.describeConfig(cluster.createAdminClient(), new ConfigCommandOptions(
          Array("--bootstrap-server", cluster.bootstrapServers(),
            "--describe",
            "--entity-type", "brokers")
            ++ entityNameParams
        ))).contains("log.cleaner.threads"))
    }
  }

  @ClusterTest
  def testUpdateConfigAndDeleteTopicConfig(): Unit = {
    TestUtils.createTopicWithAdminRaw(
      admin = cluster.createAdminClient(),
      topic = "test-config-topic",
    )
    // add -> check -> delete -> check
    ConfigCommand.alterConfig(cluster.createAdminClient(), new ConfigCommandOptions(
      Array("--bootstrap-server", cluster.bootstrapServers(),
        "--alter",
        "--add-config", "segment.bytes=10240000",
        "--entity-type", "topics",
        "--entity-name", "test-config-topic")
    ))
    TestUtils.waitUntilTrue(
      () => cluster.brokers().collect(Collectors.toList[KafkaBroker]).asScala.forall(broker => broker.logManager.logsByTopic("test-config-topic").head.config.getInt("segment.bytes") == 10240000),
      "Timeout waiting for topic config propagating to broker")

    val describeResult = TestUtils.grabConsoleOutput(
      ConfigCommand.describeConfig(cluster.createAdminClient(), new ConfigCommandOptions(
        Array("--bootstrap-server", cluster.bootstrapServers(),
          "--describe",
          "--entity-type", "topics",
          "--entity-name", "test-config-topic")
      )))
    assertTrue(describeResult.contains("segment.bytes=10240000"))
    assertTrue(describeResult.contains("sensitive=false"))

    ConfigCommand.alterConfig(cluster.createAdminClient(), new ConfigCommandOptions(
      Array("--bootstrap-server", cluster.bootstrapServers(),
        "--alter",
        "--delete-config", "segment.bytes",
        "--entity-type", "topics",
        "--entity-name", "test-config-topic")
    ))
    TestUtils.waitUntilTrue(
      () => cluster.brokers().collect(Collectors.toList[KafkaBroker]).asScala.forall(broker => broker.logManager.logsByTopic("test-config-topic").head.config.getInt("segment.bytes") != 10240000),
      "Timeout waiting for topic config propagating to broker")

    assertFalse(TestUtils.grabConsoleOutput(
      ConfigCommand.describeConfig(cluster.createAdminClient(), new ConfigCommandOptions(
        Array("--bootstrap-server", cluster.bootstrapServers(),
          "--describe",
          "--entity-type", "topics",
          "--entity-name", "test-config-topic")
      ))).contains("segment.bytes"))
  }

  @ClusterTest
  def testCannotUpdateTopicConfigUsingDeprecatedAlterConfigs(): Unit = {
    TestUtils.createTopicWithAdminRaw(
      admin = cluster.createAdminClient(),
      topic = "test-config-topic",
    )
    assertThrows(classOf[IllegalArgumentException], () => ConfigCommand.alterConfig(cluster.createAdminClient(), new ConfigCommandOptions(
      Array("--bootstrap-server", cluster.bootstrapServers(),
        "--alter",
        "--add-config", "segment.bytes=10240000",
        "--entity-type", "topics",
        "--entity-name", "test-config-topic")
    ), useIncrementalAlterConfigs = false))
  }

  @ClusterTest
  def testUpdateBrokerConfigNotAffectedByInvalidConfig(): Unit = {
    // Test case from KAFKA-13788
    ConfigCommand.alterConfig(cluster.createAdminClient(), new ConfigCommandOptions(
      Array("--bootstrap-server", cluster.bootstrapServers(),
        "--alter",
        "--add-config", "log.cleaner.threadzz=2",
        "--entity-type", "brokers",
        "--entity-default")
    ))

    ConfigCommand.alterConfig(cluster.createAdminClient(), new ConfigCommandOptions(
      Array("--bootstrap-server", cluster.bootstrapServers(),
        "--alter",
        "--add-config", "log.cleaner.threads=2",
        "--entity-type", "brokers",
        "--entity-default")
    ))
    TestUtils.waitUntilTrue(
      () => cluster.brokerSocketServers().asScala.forall(broker => broker.config.getInt("log.cleaner.threads") == 2),
      "Timeout waiting for topic config propagating to broker")
  }

  @nowarn("cat=deprecation")
  @ClusterTest(clusterType=Type.ZK, metadataVersion = MetadataVersion.IBP_2_2_IV0)
  def testFallbackToDeprecatedAlterConfigs(): Unit = {
    val spyAdmin = Mockito.spy(cluster.createAdminClient())

    val mockResult = {
      val future = new KafkaFutureImpl[Void]()
      future.completeExceptionally(new UnsupportedVersionException("simulated error"))

      val constructor = classOf[AlterConfigsResult].getDeclaredConstructor(classOf[JMap[ConfigResource, KafkaFuture[Void]]])
      constructor.setAccessible(true)
      val result = constructor.newInstance(Collections.singletonMap(new ConfigResource(ConfigResource.Type.BROKER, ""), future))
      constructor.setAccessible(false)
      result
    }
    Mockito.doReturn(mockResult).when(spyAdmin)
      .incrementalAlterConfigs(any(classOf[JMap[ConfigResource, Collection[AlterConfigOp]]]), any(classOf[AlterConfigsOptions]))

    ConfigCommand.alterConfig(spyAdmin, new ConfigCommandOptions(
      Array("--bootstrap-server", cluster.bootstrapServers(),
        "--alter",
        "--add-config", "log.cleaner.threads=2",
        "--entity-type", "brokers",
        "--entity-default")
    ))

    Mockito.verify(spyAdmin).alterConfigs(any(classOf[JMap[ConfigResource, Config]]), any(classOf[AlterConfigsOptions]))

    TestUtils.waitUntilTrue(
      () => cluster.brokerSocketServers().asScala.forall(broker => broker.config.getInt("log.cleaner.threads") == 2),
      "Timeout waiting for topic config propagating to broker")
  }
}
