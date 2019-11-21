# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.mark import parametrize
from ducktape.mark.resource import cluster

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka import config_property
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.end_to_end import EndToEndTest
from kafkatest.utils import is_int
from kafkatest.version import LATEST_0_9, LATEST_0_10, LATEST_0_10_0, LATEST_0_10_1, LATEST_0_10_2, LATEST_0_11_0, LATEST_1_0, LATEST_1_1, LATEST_2_0, LATEST_2_1, LATEST_2_2, LATEST_2_3, V_0_9_0_0, V_0_11_0_0, DEV_BRANCH, KafkaVersion

class TestDowngrade(EndToEndTest):

    TOPIC_CONFIG = {
        "partitions": 3,
        "replication-factor": 3,
        "configs": {"min.insync.replicas": 2}
    }

    def __init__(self, test_context):
        super(TestDowngrade, self).__init__(test_context=test_context, topic_config=self.TOPIC_CONFIG)

    def upgrade_from(self, kafka_version):
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            node.version = DEV_BRANCH
            node.config[config_property.INTER_BROKER_PROTOCOL_VERSION] = str(kafka_version)
            node.config[config_property.MESSAGE_FORMAT_VERSION] = str(kafka_version)
            self.kafka.start_node(node)

    def downgrade_to(self, kafka_version):
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            node.version = kafka_version
            del node.config[config_property.INTER_BROKER_PROTOCOL_VERSION]
            del node.config[config_property.MESSAGE_FORMAT_VERSION]
            self.kafka.start_node(node)

    def setup_services(self, kafka_version, compression_types, security_protocol):
        self.create_zookeeper()
        self.zk.start()

        self.create_kafka(num_nodes=3,
                          security_protocol=security_protocol,
                          interbroker_security_protocol=security_protocol,
                          version=kafka_version)
        self.kafka.start()

        self.create_producer(log_level="DEBUG",
                             compression_types=compression_types,
                             version=kafka_version)
        self.producer.start()

        self.create_consumer(log_level="DEBUG",
                             version=kafka_version)
        self.consumer.start()

    @cluster(num_nodes=7)
    @parametrize(version=str(LATEST_2_3), compression_types=["none"])
    @parametrize(version=str(LATEST_2_3), compression_types=["zstd"])
    @parametrize(version=str(LATEST_2_2), compression_types=["none"])
    @parametrize(version=str(LATEST_2_2), compression_types=["zstd"])
    @parametrize(version=str(LATEST_2_1), compression_types=["none"])
    @parametrize(version=str(LATEST_2_1), compression_types=["lz4"])
    @parametrize(version=str(LATEST_2_0), compression_types=["none"])
    @parametrize(version=str(LATEST_2_0), compression_types=["snappy"])
    @parametrize(version=str(LATEST_1_1), compression_types=["none"])
    @parametrize(version=str(LATEST_1_1), compression_types=["lz4"])
    @parametrize(version=str(LATEST_1_0), compression_types=["none"])
    @parametrize(version=str(LATEST_1_0), compression_types=["snappy"])
    @parametrize(version=str(LATEST_0_11_0), compression_types=["none"])
    @parametrize(version=str(LATEST_0_11_0), compression_types=["lz4"])
    @parametrize(version=str(LATEST_0_10_2), compression_types=["none"])
    @parametrize(version=str(LATEST_0_10_2), compression_types=["lz4"])
    @parametrize(version=str(LATEST_0_10_1), compression_types=["none"])
    @parametrize(version=str(LATEST_0_10_1), compression_types=["gzip"])
    @parametrize(version=str(LATEST_0_10_0), compression_types=["none"])
    @parametrize(version=str(LATEST_0_10_0), compression_types=["lz4"])
    @parametrize(version=str(LATEST_0_9), compression_types=["none"], security_protocol="SASL_SSL")
    @parametrize(version=str(LATEST_0_9), compression_types=["snappy"])
    @parametrize(version=str(LATEST_0_9), compression_types=["lz4"])
    def test_upgrade_and_downgrade(self, version, compression_types, security_protocol="PLAINTEXT"):
        """Test upgrade and downgrade of Kafka cluster from old versions to the current version

        `version` is the Kafka version to upgrade from and dowrade back to

        Downgrades are supported to any version which is at or above the current 
        `inter.broker.protocol.version` (IBP). For example, if a user upgrades from 1.1 to 2.3, 
        but they leave the IBP set to 1.1, then downgrading to any version at 1.1 or higher is 
        supported.

        This test case verifies that producers and consumers continue working during
        the course of an upgrade and downgrade.

        - Start 3 node broker cluster on version 'kafka_version'
        - Start producer and consumer in the background
        - Roll the cluster to upgrade to the current version with IBP set to 'kafka_version'
        - Roll the cluster to downgrade back to 'kafka_version'
        - Finally, validate that every message acked by the producer was consumed by the consumer
        """
        kafka_version = KafkaVersion(version)

        self.setup_services(kafka_version, compression_types, security_protocol)
        self.await_startup()

        self.logger.info("First pass bounce - rolling upgrade")
        self.upgrade_from(kafka_version)
        self.run_validation()

        self.logger.info("Second pass bounce - rolling downgrade")
        self.downgrade_to(kafka_version)
        self.run_validation()
