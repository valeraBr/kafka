/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor;

public class TaskId {

    public final int taskGroupId;
    public final int partition;

    public TaskId(int taskGroupId, int partition) {
        this.taskGroupId = taskGroupId;
        this.partition = partition;
    }

    public String toString() {
        return taskGroupId + "_" + partition;
    }

    public static TaskId parse(String string) {
        int index = string.indexOf('_');
        if (index <= 0 || index + 1 >= string.length()) throw new TaskIdFormatException();

        try {
            int taskGroupId = Integer.parseInt(string.substring(0, index));
            int partition = Integer.parseInt(string.substring(index + 1));

            return new TaskId(taskGroupId, partition);
        } catch (Exception e) {
            throw new TaskIdFormatException();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TaskId) {
            TaskId other = (TaskId) o;
            return other.taskGroupId == this.taskGroupId && other.partition == this.partition;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        long n = ((long) taskGroupId << 32) | (long) partition;
        return (int) (n % 0xFFFFFFFFL);
    }

    public static class TaskIdFormatException extends RuntimeException {
    }
}
