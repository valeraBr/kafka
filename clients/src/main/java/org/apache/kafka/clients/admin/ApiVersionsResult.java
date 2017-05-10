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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.NodeVersions;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Results of the apiVersions call.
 */
@InterfaceStability.Unstable
public class ApiVersionsResult {
    private final Map<Node, KafkaFuture<NodeVersions>> futures;

    ApiVersionsResult(Map<Node, KafkaFuture<NodeVersions>> futures) {
        this.futures = futures;
    }

    public Map<Node, KafkaFuture<NodeVersions>> results() {
        return futures;
    }

    public KafkaFuture<Map<Node, NodeVersions>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).
            thenApply(new KafkaFuture.Function<Void, Map<Node, NodeVersions>>() {
                @Override
                public Map<Node, NodeVersions> apply(Void v) {
                    Map<Node, NodeVersions> versions = new HashMap<>(futures.size());
                    for (Map.Entry<Node, KafkaFuture<NodeVersions>> entry : futures.entrySet()) {
                        try {
                            versions.put(entry.getKey(), entry.getValue().get());
                        } catch (InterruptedException | ExecutionException e) {
                            // This should be unreachable, because allOf ensured that all the futures
                            // completed successfully.
                            throw new RuntimeException(e);
                        }
                    }
                    return versions;
                }
            });
    }
}
