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

package org.apache.kafka.streams.processor.internals.assignment;

import org.apache.kafka.streams.processor.TaskId;

import java.util.HashSet;
import java.util.Set;

public class ClientState {

    final static double COST_ACTIVE = 0.1;
    final static double COST_STANDBY  = 0.2;
    final static double COST_LOAD = 0.5;

    public final Set<TaskId> activeTasks;
    public final Set<TaskId> standbyTasks;
    public final Set<TaskId> assignedTasks;
    public final Set<TaskId> prevActiveTasks;
    public final Set<TaskId> prevAssignedTasks;

    public double capacity;
    public double cost;

    public ClientState() {
        this(0d);
    }

    public ClientState(double capacity) {
        this(new HashSet<TaskId>(), new HashSet<TaskId>(), new HashSet<TaskId>(), new HashSet<TaskId>(), new HashSet<TaskId>(), capacity);
    }

    private ClientState(Set<TaskId> activeTasks, Set<TaskId> standbyTasks, Set<TaskId> assignedTasks, Set<TaskId> prevActiveTasks, Set<TaskId> prevAssignedTasks, double capacity) {
        this.activeTasks = activeTasks;
        this.standbyTasks = standbyTasks;
        this.assignedTasks = assignedTasks;
        this.prevActiveTasks = prevActiveTasks;
        this.prevAssignedTasks = prevAssignedTasks;
        this.capacity = capacity;
        this.cost = 0d;
    }

    public ClientState copy() {
        return new ClientState(new HashSet<>(activeTasks), new HashSet<>(standbyTasks), new HashSet<>(assignedTasks),
                new HashSet<>(prevActiveTasks), new HashSet<>(prevAssignedTasks), capacity);
    }

    public void assign(TaskId taskId, boolean active) {
        if (active)
            activeTasks.add(taskId);
        else
            standbyTasks.add(taskId);

        assignedTasks.add(taskId);

        double cost = COST_LOAD;
        cost = prevAssignedTasks.remove(taskId) ? COST_STANDBY : cost;
        cost = prevActiveTasks.remove(taskId) ? COST_ACTIVE : cost;

        this.cost += cost;
    }

    @Override
    public String toString() {
        return "[activeTasks: (" + activeTasks +
                ") assignedTasks: (" + assignedTasks +
                ") prevActiveTasks: (" + prevActiveTasks +
                ") prevAssignedTasks: (" + prevAssignedTasks +
                ") capacity: " + capacity +
                " cost: " + cost +
                "]";
    }
}
