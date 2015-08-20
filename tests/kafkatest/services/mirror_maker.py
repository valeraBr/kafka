
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

from ducktape.services.service import Service
from ducktape.utils.util import wait_until

import os
import subprocess

"""
0.8.2.1 MirrorMaker options

Option                                  Description
------                                  -----------
--abort.on.send.failure <Stop the       Configure the mirror maker to exit on
  entire mirror maker when a send         a failed send. (default: true)
  failure occurs>
--blacklist <Java regex (String)>       Blacklist of topics to mirror.
--consumer.config <config file>         Embedded consumer config for consuming
                                          from the source cluster.
--consumer.rebalance.listener <A        The consumer rebalance listener to use
  custom rebalance listener of type       for mirror maker consumer.
  ConsumerRebalanceListener>
--help                                  Print this message.
--message.handler <A custom message     Message handler which will process
  handler of type                         every record in-between consumer and
  MirrorMakerMessageHandler>              producer.
--message.handler.args <Arguments       Arguments used by custom rebalance
  passed to message handler               listener for mirror maker consumer
  constructor.>
--num.streams <Integer: Number of       Number of consumption streams.
  threads>                                (default: 1)
--offset.commit.interval.ms <Integer:   Offset commit interval in ms (default:
  offset commit interval in               60000)
  millisecond>
--producer.config <config file>         Embedded producer config.
--rebalance.listener.args <Arguments    Arguments used by custom rebalance
  passed to custom rebalance listener     listener for mirror maker consumer
  constructor as a string.>
--whitelist <Java regex (String)>       Whitelist of topics to mirror.
"""


class MirrorMaker(Service):

    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/mirror_maker"
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "mirror_maker.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    PRODUCER_CONFIG = os.path.join(PERSISTENT_ROOT, "producer.properties")
    CONSUMER_CONFIG = os.path.join(PERSISTENT_ROOT, "consumer.properties")
    KAFKA_HOME = "/opt/kafka/"

    logs = {
        "mirror_maker_log": {
            "path": LOG_FILE,
            "collect_default": True}
        }

    def __init__(self, context, source, target, whitelist=None, blacklist=None, num_streams=1, consumer_timeout_ms=None):
        """
        MirrorMaker mirrors messages from one or more source clusters to a single destination cluster.

        Args:
            context:                    standard context
            source:                     source Kafka cluster
            target:                     target Kafka cluster to which data will be mirrored
            whitelist:                  whitelist regex for topics to mirror
            blacklist:                  blacklist regex for topics not to mirror
            num_streams:                number of consumer threads to create
            consumer_timeout_ms:        consumer stops if t > consumer_timeout_ms elapses between consecutive messages
        """
        super(MirrorMaker, self).__init__(context, num_nodes=1)

        self.consumer_timeout_ms = consumer_timeout_ms
        self.num_streams = num_streams
        self.whitelist = whitelist
        self.blacklist = blacklist
        self.source = source
        self.target = target

    @property
    def start_cmd(self):
        cmd = "export LOG_DIR=%s;" % MirrorMaker.LOG_DIR
        cmd += " export KAFKA_LOG4J_OPTS=\"-Dlog4j.configuration=file:%s\";" % MirrorMaker.LOG4J_CONFIG
        cmd += " %s/bin/kafka-run-class.sh kafka.tools.MirrorMaker" % MirrorMaker.KAFKA_HOME
        cmd += " --consumer.config %s" % MirrorMaker.CONSUMER_CONFIG
        cmd += " --producer.config %s" % MirrorMaker.PRODUCER_CONFIG
        cmd += " --num.streams %d" % self.num_streams

        if self.whitelist is not None:
            cmd += " --whitelist=\"%s\"" % self.whitelist
        if self.blacklist is not None:
            cmd += " --blacklist=\"%s\"" % self.blacklist
        cmd += " 1>> %s 2>> %s &" % (MirrorMaker.LOG_FILE, MirrorMaker.LOG_FILE)
        return cmd

    @property
    def node(self):
        """Convenience method since this Service only ever has one node"""
        if self.nodes:
            return self.nodes[0]
        else:
            return None

    def pids(self, node):
        try:
            cmd = "ps ax | grep -i MirrorMaker | grep java | grep -v grep | awk '{print $1}'"
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (subprocess.CalledProcessError, ValueError) as e:
            return []

    def alive(self):
        return len(self.pids(self.node)) > 0

    def start_node(self, node):
        node.account.ssh("mkdir -p %s" % MirrorMaker.PERSISTENT_ROOT, allow_fail=False)
        node.account.ssh("mkdir -p %s" % MirrorMaker.LOG_DIR, allow_fail=False)

        # Create, upload one consumer config file for source cluster
        consumer_props = self.render('consumer.properties', zookeeper_connect=self.source.zk.connect_setting())
        node.account.create_file(MirrorMaker.CONSUMER_CONFIG, consumer_props)

        # Create, upload producer properties file for target cluster
        producer_props = self.render('producer.properties',  broker_list=self.target.bootstrap_servers(),
                                     producer_type="async")
        node.account.create_file(MirrorMaker.PRODUCER_CONFIG, producer_props)

        # Create and upload log properties
        log_config = self.render('tools_log4j.properties', log_file=MirrorMaker.LOG_FILE)
        node.account.create_file(MirrorMaker.LOG4J_CONFIG, log_config)

        # Run mirror maker
        cmd = self.start_cmd
        self.logger.debug("Mirror maker command: %s", cmd)
        node.account.ssh(cmd, allow_fail=False)
        wait_until(lambda: self.alive(), timeout_sec=10, backoff_sec=.5,
                   err_msg="Mirror maker took to long to start.")
        self.logger.debug("Mirror maker is alive")

    def stop_node(self, node):
        node.account.kill_process("java", allow_fail=True)
        wait_until(lambda: not self.alive(), timeout_sec=10, backoff_sec=.5,
                   err_msg="Mirror maker took to long to stop.")

    def clean_node(self, node):
        node.account.ssh("rm -rf %s" % MirrorMaker.PERSISTENT_ROOT, allow_fail=False)

