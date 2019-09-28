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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.MessageTestUtil;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertTrue;

public class StopReplicaRequestTest {

    @Test
    public void testStopReplicaRequestNormalization() {
        Set<TopicPartition> tps = TestUtils.generateRandomTopicPartitions(10, 10);
        StopReplicaRequest.Builder builder = new StopReplicaRequest.Builder((short) 5, 0, 0, 0, false, tps);
        assertTrue(builder.build((short) 1).sizeInBytes() <  builder.build((short) 0).sizeInBytes());
    }

}
