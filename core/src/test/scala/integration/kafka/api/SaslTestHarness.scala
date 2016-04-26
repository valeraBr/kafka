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

import kafka.zk.ZooKeeperTestHarness
import kafka.server.KafkaConfig
import org.junit.{After, Before}
import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.kafka.common.config.SaslConfigs

trait SaslTestHarness extends ZooKeeperTestHarness with SaslSetup {
  protected val zkSaslEnabled: Boolean
  protected val kafkaClientSaslMechanism = "GSSAPI"
  protected val kafkaServerSaslMechanisms = List(kafkaClientSaslMechanism)

  // Override this list to enable client login modules for multiple mechanisms for testing
  // of multi-mechanism brokers with clients using different mechanisms in a single JVM
  protected def allKafkaClientSaslMechanisms = List(kafkaClientSaslMechanism)

  @Before
  override def setUp() {
    if (zkSaslEnabled)
      startSasl(Both, kafkaServerSaslMechanisms, allKafkaClientSaslMechanisms)
    else
      startSasl(KafkaSasl, kafkaServerSaslMechanisms, allKafkaClientSaslMechanisms)
    super.setUp
  }

  @After
  override def tearDown() {
    super.tearDown
    closeSasl()
  }

  def kafkaSaslProperties(kafkaClientSaslMechanism: String, kafkaServerSaslMechanisms: List[String]) = {
    val props = new Properties
    props.put(SaslConfigs.SASL_MECHANISM, kafkaClientSaslMechanism)
    props.put(KafkaConfig.SaslMechanismInterBrokerProtocolProp, kafkaClientSaslMechanism)
    props.put(SaslConfigs.SASL_ENABLED_MECHANISMS, kafkaServerSaslMechanisms.asJava)
    props
  }

}
