/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.trogdor.basic;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.utils.Shell;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.trogdor.common.Node;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.Topology;

import java.io.IOException;

/**
 * Defines a cluster topology
 */
public class BasicPlatform implements Platform {
    private final Node curNode;
    private final BasicTopology topology;
    private final CommandRunner commandRunner;

    public interface CommandRunner {
        String run(Node curNode, String[] command) throws IOException;
    }

    public static class ShellCommandRunner implements CommandRunner {
        @Override
        public String run(Node curNode, String[] command) throws IOException {
            return Shell.execCommand(command);
        }
    }

    public BasicPlatform(String curNodeName, BasicTopology topology,
                         CommandRunner commandRunner) {
        this.curNode = topology.node(curNodeName);
        if (this.curNode == null) {
            throw new RuntimeException(String.format("No node named %s found " +
                    "in the cluster!  Cluster nodes are: %s", curNodeName,
                Utils.join(topology.nodes().keySet(), ",")));
        }
        this.topology = topology;
        this.commandRunner = commandRunner;
    }

    public BasicPlatform(String curNodeName, JsonNode configRoot) {
        JsonNode nodes = configRoot.get("nodes");
        if (nodes == null) {
            throw new RuntimeException("Expected to find a 'nodes' field " +
                "in the root JSON configuration object");
        }
        this.topology = new BasicTopology(nodes);
        this.curNode = topology.node(curNodeName);
        if (this.curNode == null) {
            throw new RuntimeException(String.format("No node named %s found " +
                "in the cluster!  Cluster nodes are: %s", curNodeName,
                Utils.join(topology.nodes().keySet(), ",")));
        }
        this.commandRunner = new ShellCommandRunner();
    }

    @Override
    public String name() {
        return "BasicPlatform";
    }

    @Override
    public Node curNode() {
        return curNode;
    }

    @Override
    public Topology topology() {
        return topology;
    }

    @Override
    public String runCommand(String[] command) throws IOException {
        return commandRunner.run(curNode, command);
    }
}
