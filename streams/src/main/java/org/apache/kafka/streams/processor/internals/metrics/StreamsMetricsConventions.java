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
package org.apache.kafka.streams.processor.internals.metrics;

import java.util.LinkedHashMap;
import java.util.Map;

public final class StreamsMetricsConventions {
    private StreamsMetricsConventions() {
    }

    public static String threadLevelSensorName(final String threadName, final String sensorName) {
        return "thread." + threadName + "." + sensorName;
    }

    static Map<String, String> threadLevelTags(final String threadName, final Map<String, String> tags) {
        if (tags.containsKey("client-id")) {
            return tags;
        } else {
            final LinkedHashMap<String, String> newTags = new LinkedHashMap<>(tags);
            newTags.put("client-id", threadName);
            return newTags;
        }
    }

    public static String cacheLevelSensorName(final String threadName,
                                              final String taskName,
                                              final String cacheName,
                                              final String sensorName) {
        return threadName + "." + taskName + "." + cacheName + "." + sensorName;
    }
}
