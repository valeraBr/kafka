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

package org.apache.kafka.controller;

public final class MockControllerMetrics implements ControllerMetrics {
    private volatile boolean active;
    private int topicCount;

    public MockControllerMetrics() {
        this.active = false;
        this.topicCount = 0;
    }

    @Override
    public void setActive(boolean active) {
        this.active = active;
    }

    @Override
    public boolean active() {
        return this.active;
    }

    @Override
    public void updateEventQueueTime(long durationMs) {
        // nothing to do
    }

    @Override
    public void updateEventQueueProcessingTime(long durationMs) {
        // nothing to do
    }

    @Override
    public int topicCount() {
        return this.topicCount;
    }

    @Override
    public void incTopicCount() {
        this.topicCount++;
    }

    @Override
    public void decTopicCount() {
        this.topicCount--;    
    }
}
