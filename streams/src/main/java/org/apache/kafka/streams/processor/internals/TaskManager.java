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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskIdFormatException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;

public class TaskManager {
    // initialize the task list
    // activeTasks needs to be concurrent as it can be accessed
    // by QueryableState
    private final Logger log;
    private final UUID processId;
    private final AssignedStreamsTasks active;
    private final AssignedStandbyTasks standby;
    private final ChangelogReader changelogReader;
    private final String logPrefix;
    private final Consumer<byte[], byte[]> restoreConsumer;
    private final StreamThread.TaskCreator taskCreator;
    private final StreamThread.AbstractTaskCreator<StandbyTask> standbyTaskCreator;

    private final Admin adminClient;
    private DeleteRecordsResult deleteRecordsResult;
    private boolean rebalanceInProgress = false;  // if we are in the middle of a rebalance, it is not safe to commit

    // the restore consumer is only ever assigned changelogs from restoring tasks or standbys (but not both)
    private boolean restoreConsumerAssignedStandbys = false;

    // following information is updated during rebalance phase by the partition assignor
    private Map<TopicPartition, TaskId> partitionsToTaskId = new HashMap<>();
    private Map<TopicPartition, Task> partitionToTask = new HashMap<>();
    private Map<TaskId, Set<TopicPartition>> assignedActiveTasks = new HashMap<>();
    private Map<TaskId, Set<TopicPartition>> assignedStandbyTasks = new HashMap<>();
    private Map<TaskId, Set<TopicPartition>> addedActiveTasks = new HashMap<>();
    private Map<TaskId, Set<TopicPartition>> addedStandbyTasks = new HashMap<>();
    private Map<TaskId, Set<TopicPartition>> revokedActiveTasks = new HashMap<>();
    private Map<TaskId, Set<TopicPartition>> revokedStandbyTasks = new HashMap<>();
    private Map<TaskId, Task> tasks = new TreeMap<>();

    private Consumer<byte[], byte[]> consumer;

    TaskManager(final ChangelogReader changelogReader,
                final UUID processId,
                final String logPrefix,
                final Consumer<byte[], byte[]> restoreConsumer,
                final StreamsMetadataState streamsMetadataState,
                final StreamThread.TaskCreator taskCreator,
                final StreamThread.StandbyTaskCreator standbyTaskCreator,
                final Admin adminClient,
                final AssignedStreamsTasks active,
                final AssignedStandbyTasks standby) {
        this(changelogReader, processId, logPrefix, restoreConsumer, taskCreator, standbyTaskCreator, adminClient, active, standby);
    }

    TaskManager(final ChangelogReader changelogReader,
                final UUID processId,
                final String logPrefix,
                final Consumer<byte[], byte[]> restoreConsumer,
                final StreamThread.TaskCreator taskCreator,
                final StreamThread.StandbyTaskCreator standbyTaskCreator,
                final Admin adminClient,
                final AssignedStreamsTasks active,
                final AssignedStandbyTasks standby) {
        this.changelogReader = changelogReader;
        this.processId = processId;
        this.logPrefix = logPrefix;
        this.restoreConsumer = restoreConsumer;
        this.taskCreator = taskCreator;
        this.standbyTaskCreator = standbyTaskCreator;
        this.active = active;
        this.standby = standby;

        final LogContext logContext = new LogContext(logPrefix);

        this.log = logContext.logger(getClass());

        this.adminClient = adminClient;
    }

    public Admin adminClient() {
        return adminClient;
    }


    public void setAssignmentMetadata(final Map<TaskId, Set<TopicPartition>> activeTasks,
                                      final Map<TaskId, Set<TopicPartition>> standbyTasks) {
        addedActiveTasks.clear();
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : activeTasks.entrySet()) {
            if (!assignedActiveTasks.containsKey(entry.getKey())) {
                addedActiveTasks.put(entry.getKey(), entry.getValue());
            }
        }

        addedStandbyTasks.clear();
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : standbyTasks.entrySet()) {
            if (!assignedStandbyTasks.containsKey(entry.getKey())) {
                addedStandbyTasks.put(entry.getKey(), entry.getValue());
            }
        }

        revokedActiveTasks.clear();
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : assignedActiveTasks.entrySet()) {
            if (!activeTasks.containsKey(entry.getKey())) {
                revokedActiveTasks.put(entry.getKey(), entry.getValue());
            }
        }

        revokedStandbyTasks.clear();
        for (final Map.Entry<TaskId, Set<TopicPartition>> entry : assignedStandbyTasks.entrySet()) {
            if (!standbyTasks.containsKey(entry.getKey())) {
                revokedStandbyTasks.put(entry.getKey(), entry.getValue());
            }
        }

        log.debug("Assigning metadata with: " +
                      "\tpreviousAssignedActiveTasks: {},\n" +
                      "\tpreviousAssignedStandbyTasks: {}\n" +
                      "The updated task states are: \n" +
                      "\tassignedActiveTasks {},\n" +
                      "\tassignedStandbyTasks {},\n" +
                      "\taddedActiveTasks {},\n" +
                      "\taddedStandbyTasks {},\n" +
                      "\trevokedActiveTasks {},\n" +
                      "\trevokedStandbyTasks {}",
                  assignedActiveTasks, assignedStandbyTasks,
                  activeTasks, standbyTasks,
                  addedActiveTasks, addedStandbyTasks,
                  revokedActiveTasks, revokedStandbyTasks);

        assignedActiveTasks = activeTasks;
        assignedStandbyTasks = standbyTasks;
    }


    void createTasks(final Collection<TopicPartition> assignment) {
        if (consumer == null) {
            throw new IllegalStateException(logPrefix + "consumer has not been initialized while adding stream tasks. This should not happen.");
        }

        if (!assignment.isEmpty() && !assignedActiveTasks().isEmpty()) {
            resumeSuspended(assignment);
        }
        if (!addedActiveTasks.isEmpty()) {
            addNewActiveTasks(addedActiveTasks);
        }
        if (!addedStandbyTasks.isEmpty()) {
            addNewStandbyTasks(addedStandbyTasks);
        }

        // need to clear restore consumer if it was reading standbys but we have active tasks that may need restoring
        if (!addedActiveTasks.isEmpty() && restoreConsumerAssignedStandbys) {
            restoreConsumer.unsubscribe();
            restoreConsumerAssignedStandbys = false;
        }

        // Pause all the new partitions until the underlying state store is ready for all the active tasks.
        log.debug("Pausing all active task partitions until the underlying state stores are ready");
        pausePartitions();
    }

    private void resumeSuspended(final Collection<TopicPartition> assignment) {
        final Set<TaskId> suspendedTasks = partitionsToTaskSet(assignment);
        suspendedTasks.removeAll(addedActiveTasks.keySet());

        log.debug("Suspended tasks to be resumed: {}", suspendedTasks);

        for (final TaskId taskId : suspendedTasks) {
            final Set<TopicPartition> partitions = tasks.get(taskId).partitions();
            try {
                if (!active.maybeResumeSuspendedTask(taskId, partitions)) {
                    // recreate if resuming the suspended task failed because the associated partitions changed
                    addedActiveTasks.put(taskId, partitions);
                }
            } catch (final StreamsException e) {
                log.error("Failed to resume a suspended active task {} due to the following error:", taskId, e);
                throw e;
            }
        }
    }

    private void addNewActiveTasks(final Map<TaskId, Set<TopicPartition>> newActiveTasks) {
        log.debug("New active tasks to be created: {}", newActiveTasks);

        for (final StreamTask task : taskCreator.createTasks(consumer, newActiveTasks)) {
            active.addNewTask(task);
            final Task previous = tasks.put(task.id(), task);
            if (previous != null) {
                throw new IllegalStateException("Found prior version of a newly created task:" + task + ": " + previous);
            }
        }
    }

    private void addNewStandbyTasks(final Map<TaskId, Set<TopicPartition>> newStandbyTasks) {
        log.debug("New standby tasks to be created: {}", newStandbyTasks);

        for (final StandbyTask task : standbyTaskCreator.createTasks(consumer, newStandbyTasks)) {
            standby.addNewTask(task);
            final Task previous = tasks.put(task.id(), task);
            if (previous != null) {
                throw new IllegalStateException("Found prior version of a newly created task:" + task + ": " + previous);
            }
        }
    }

    /**
     * Returns ids of tasks whose states are kept on the local storage. This includes active, standby, and previously
     * assigned but not yet cleaned up tasks
     */
    public Set<TaskId> cachedTasksIds() {
        // A client could contain some inactive tasks whose states are still kept on the local storage in the following scenarios:
        // 1) the client is actively maintaining standby tasks by maintaining their states from the change log.
        // 2) the client has just got some tasks migrated out of itself to other clients while these task states
        //    have not been cleaned up yet (this can happen in a rolling bounce upgrade, for example).

        final HashSet<TaskId> tasks = new HashSet<>();

        final File[] stateDirs = taskCreator.stateDirectory().listTaskDirectories();
        if (stateDirs != null) {
            for (final File dir : stateDirs) {
                try {
                    final TaskId id = TaskId.parse(dir.getName());
                    // if the checkpoint file exists, the state is valid.
                    if (new File(dir, StateManagerUtil.CHECKPOINT_FILE_NAME).exists()) {
                        tasks.add(id);
                    }
                } catch (final TaskIdFormatException e) {
                    // there may be some unknown files that sits in the same directory,
                    // we should ignore these files instead trying to delete them as well
                }
            }
        }

        return tasks;
    }

    /**
     * Closes standby tasks that were not reassigned at the end of a rebalance.
     *
     * @return list of changelog topic partitions from revoked tasks
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    List<TopicPartition> closeRevokedStandbyTasks() {
        final List<TopicPartition> revokedChangelogs = standby.closeRevokedStandbyTasks(revokedStandbyTasks);
        final Iterator<Task> iterator = tasks.values().iterator();
        while (iterator.hasNext()) {
            final Task next = iterator.next();
            if (next instanceof StandbyTask) {
                // TODO, right now the delegate is actually closing the task
//                next.close(true);
                next.transitionTo(Task.State.CLOSED);
                // TODO: apparently, there's no test coverage here
                iterator.remove();
            }
        }

        // If the restore consumer is assigned any standby partitions they must be removed
        removeChangelogsFromRestoreConsumer(revokedChangelogs, true);

        return revokedChangelogs;
    }

    /**
     * Closes suspended active tasks that were not reassigned at the end of a rebalance.
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    void closeRevokedSuspendedTasks() {
        // changelogs should have already been removed during suspend
        final RuntimeException exception = active.closeNotAssignedSuspendedTasks(revokedActiveTasks.keySet());
        final Iterator<Task> iterator = tasks.values().iterator();
        while (iterator.hasNext()) {
            final Task next = iterator.next();
            if (next instanceof StreamTask) {
                // TODO, right now the delegate is actually closing the task
//                next.close(true);
                next.transitionTo(Task.State.CLOSED);
                // TODO: apparently, there's no test coverage here
                iterator.remove();
            }
        }

        // At this point all revoked tasks should have been closed, we can just throw the exception
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Similar to shutdownTasksAndState, however does not close the task managers, in the hope that
     * soon the tasks will be assigned again.
     * @return list of suspended tasks
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    Set<TaskId> suspendActiveTasksAndState(final Collection<TopicPartition> revokedPartitions) {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);
        final List<TopicPartition> revokedChangelogs = new ArrayList<>();

        final Set<TaskId> revokedTasks = partitionsToTaskSet(revokedPartitions);

        final RuntimeException firstExceptionFromCall = active.suspendOrCloseTasks(revokedTasks, revokedChangelogs);
        firstException.compareAndSet(null, firstExceptionFromCall);
        for (final Task next : tasks.values()) {
            if (revokedTasks.contains(next.id())) {
                next.transitionTo(Task.State.REVOKED);
            }
        }

        changelogReader.remove(revokedChangelogs);
        removeChangelogsFromRestoreConsumer(revokedChangelogs, false);

        final Exception exception = firstException.get();
        if (exception != null) {
            throw new StreamsException(logPrefix + "failed to suspend stream tasks", exception);
        }
        return active.suspendedTaskIds();
    }

    /**
     * Closes active tasks as zombies, as these partitions have been lost and are no longer owned.
     * NOTE this method assumes that when it is called, EVERY task/partition has been lost and must
     * be closed as a zombie.
     * @return list of lost tasks
     */
    Set<TaskId> closeLostTasks() {
        final Set<TaskId> lostTasks = activeTaskIds();
        log.debug("Closing lost active tasks as zombies: {}", lostTasks);

        final RuntimeException exception = active.closeAllTasksAsZombies();
        final Iterator<Task> iterator = tasks.values().iterator();
        while (iterator.hasNext()) {
            final Task next = iterator.next();
            next.transitionTo(Task.State.CLOSED);
            iterator.remove();
        }

        log.debug("Clearing the store changelog reader: {}", changelogReader);
        changelogReader.clear();

        if (!restoreConsumerAssignedStandbys) {
            log.debug("Clearing the restore consumer's assignment: {}", restoreConsumer.assignment());
            restoreConsumer.unsubscribe();
        }

        if (exception != null) {
            throw exception;
        }

        return lostTasks;
    }

    void shutdown(final boolean clean) {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        try {
            active.shutdown(clean);
        } catch (final RuntimeException fatalException) {
            firstException.compareAndSet(null, fatalException);
        }
        standby.shutdown(clean);
        final Iterator<Task> iterator = tasks.values().iterator();
        while (iterator.hasNext()) {
            final Task next = iterator.next();
            next.transitionTo(Task.State.REVOKED);
            next.transitionTo(Task.State.CLOSED);
            iterator.remove();
        }

        // remove the changelog partitions from restore consumer
        try {
            restoreConsumer.unsubscribe();
        } catch (final RuntimeException fatalException) {
            firstException.compareAndSet(null, fatalException);
        }
        taskCreator.close();

        final RuntimeException fatalException = firstException.get();
        if (fatalException != null) {
            throw fatalException;
        }
    }

    public Set<TaskId> previousRunningTaskIds() {
        return tasks.values()
                    .stream()
                    .filter(t -> t instanceof StreamTask && t.state() == Task.State.REVOKED)
                    .map(Task::id)
                    .collect(Collectors.toSet());
    }

    public Set<TaskId> activeTaskIds() {
        return tasks.values()
                    .stream()
                    .filter(t -> t instanceof StreamTask)
                    .map(Task::id)
                    .collect(Collectors.toSet());
    }

    Set<TaskId> standbyTaskIds() {
        return tasks.values()
                    .stream()
                    .filter(t -> t instanceof StandbyTask)
                    .map(Task::id)
                    .collect(Collectors.toSet());
    }

    Set<TaskId> revokedActiveTaskIds() {
        return tasks.values()
                    .stream()
                    .filter(t -> t instanceof StreamTask && t.state() == Task.State.REVOKED)
                    .map(Task::id)
                    .collect(Collectors.toSet());
    }

    Set<TaskId> revokedStandbyTaskIds() {
        return tasks.values()
                    .stream()
                    .filter(t -> t instanceof StandbyTask && t.state() == Task.State.REVOKED)
                    .map(Task::id)
                    .collect(Collectors.toSet());
    }

    Set<TaskId> previousActiveTaskIds() {
        return tasks.values()
                    .stream()
                    .filter(t -> t instanceof StreamTask && t.state() == Task.State.REVOKED)
                    .map(Task::id)
                    .collect(Collectors.toSet());
    }

    Set<TaskId> previousStandbyTaskIds() {
        return tasks.values()
                    .stream()
                    .filter(t -> t instanceof StandbyTask && t.state() == Task.State.REVOKED)
                    .map(Task::id)
                    .collect(Collectors.toSet());
    }

    // the following functions are for testing only
    Map<TaskId, Set<TopicPartition>> assignedActiveTasks() {
        return tasks.values().stream().filter(t -> t instanceof StreamTask).collect(Collectors.toMap(Task::id, Task::partitions));
    }

    Map<TaskId, Set<TopicPartition>> assignedStandbyTasks() {
        return tasks.values().stream().filter(t -> t instanceof StandbyTask).collect(Collectors.toMap(Task::id, Task::partitions));
    }

    StreamTask activeTask(final TopicPartition partition) {
        for (final Task task : tasks.values()) {
            if (task instanceof StreamTask && task.partitions().contains(partition)) {
                return (StreamTask) task;
            }
        }
        return null;
    }

    StandbyTask standbyTask(final TopicPartition partition) {
        for (final Task task : tasks.values()) {
            if (task instanceof StandbyTask && task.partitions().contains(partition)) {
                return (StandbyTask) task;
            }
        }
        return null;
    }

    Map<TaskId, StreamTask> activeTasks() {
        return tasks.values().stream().filter(t -> t instanceof StreamTask).map(t -> (StreamTask) t).collect(Collectors.toMap(Task::id, t -> t));
    }

    Map<TaskId, StandbyTask> standbyTasks() {
        return tasks.values().stream().filter(t -> t instanceof StandbyTask).map(t -> (StandbyTask) t).collect(Collectors.toMap(Task::id, t -> t));
    }

    void setConsumer(final Consumer<byte[], byte[]> consumer) {
        this.consumer = consumer;
    }

    public UUID processId() {
        return processId;
    }

    InternalTopologyBuilder builder() {
        return taskCreator.builder();
    }

    void pausePartitions() {
        log.trace("Pausing partitions: {}", consumer.assignment());
        consumer.pause(consumer.assignment());
    }

    /**
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     */
    boolean updateNewAndRestoringTasks() {
        active.initializeNewTasks();
        standby.initializeNewTasks();

        if (active.hasRestoringTasks()) {
            changelogReader.restore();
            active.updateRestored(changelogReader.completedChangelogs());
        } else {
            active.clearRestoringPartitions();
        }

        if (active.allTasksRunning()) {
            final Set<TopicPartition> assignment = consumer.assignment();
            log.trace("Resuming partitions {}", assignment);
            consumer.resume(assignment);
            assignStandbyPartitions();
            return standby.allTasksRunning();
        }

        return false;
    }

    boolean hasActiveRunningTasks() {
        for (final Task task : tasks.values()) {
            if (task instanceof StreamTask && task.state() == Task.State.RUNNING) {
                return true;
            }
        }
        return false;
    }

    boolean hasStandbyRunningTasks() {
        return standby.hasRunningTasks();
    }

    private void assignStandbyPartitions() {
        final Collection<StandbyTask> running = standby.running();
        final Map<TopicPartition, Long> checkpointedOffsets = new HashMap<>();
        for (final StandbyTask standbyTask : running) {
            checkpointedOffsets.putAll(standbyTask.checkpointedOffsets());
        }

        log.debug("Assigning and seeking restoreConsumer to {}", checkpointedOffsets);
        restoreConsumerAssignedStandbys = true;
        restoreConsumer.assign(checkpointedOffsets.keySet());
        for (final Map.Entry<TopicPartition, Long> entry : checkpointedOffsets.entrySet()) {
            final TopicPartition partition = entry.getKey();
            final long offset = entry.getValue();
            if (offset >= 0) {
                restoreConsumer.seek(partition, offset);
            } else {
                restoreConsumer.seekToBeginning(singleton(partition));
            }
        }
    }

    public void setRebalanceInProgress(final boolean rebalanceInProgress) {
        this.rebalanceInProgress = rebalanceInProgress;
    }

    public void setPartitionsToTaskId(final Map<TopicPartition, TaskId> partitionsToTaskId) {
        this.partitionsToTaskId = partitionsToTaskId;
    }

    public void updateSubscriptionsFromAssignment(final List<TopicPartition> partitions) {
        if (builder().sourceTopicPattern() != null) {
            final Set<String> assignedTopics = new HashSet<>();
            for (final TopicPartition topicPartition : partitions) {
                assignedTopics.add(topicPartition.topic());
            }

            final Collection<String> existingTopics = builder().subscriptionUpdates().getUpdates();
            if (!existingTopics.containsAll(assignedTopics)) {
                assignedTopics.addAll(existingTopics);
                builder().updateSubscribedTopics(assignedTopics, logPrefix);
            }
        }
    }

    public void updateSubscriptionsFromMetadata(final Set<String> topics) {
        if (builder().sourceTopicPattern() != null) {
            final Collection<String> existingTopics = builder().subscriptionUpdates().getUpdates();
            if (!existingTopics.equals(topics)) {
                builder().updateSubscribedTopics(topics, logPrefix);
            }
        }
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     * @return number of committed offsets, or -1 if we are in the middle of a rebalance and cannot commit
     */
    int commitAll() {
        return rebalanceInProgress ? -1 : active.commit() + standby.commit();
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    int process(final long now) {
        return active.process(now);
    }

    /**
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    int punctuate() {
        return active.punctuate();
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    int maybeCommitActiveTasksPerUserRequested() {
        return rebalanceInProgress ? -1 : active.maybeCommitPerUserRequested();
    }

    void maybePurgeCommitedRecords() {
        // we do not check any possible exceptions since none of them are fatal
        // that should cause the application to fail, and we will try delete with
        // newer offsets anyways.
        if (deleteRecordsResult == null || deleteRecordsResult.all().isDone()) {

            if (deleteRecordsResult != null && deleteRecordsResult.all().isCompletedExceptionally()) {
                log.debug("Previous delete-records request has failed: {}. Try sending the new request now",
                          deleteRecordsResult.lowWatermarks());
            }

            final Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
            for (final Map.Entry<TopicPartition, Long> entry : active.recordsToDelete().entrySet()) {
                recordsToDelete.put(entry.getKey(), RecordsToDelete.beforeOffset(entry.getValue()));
            }
            if (!recordsToDelete.isEmpty()) {
                deleteRecordsResult = adminClient.deleteRecords(recordsToDelete);
                log.trace("Sent delete-records request: {}", recordsToDelete);
            }
        }
    }

    /**
     * Produces a string representation containing useful information about the TaskManager.
     * This is useful in debugging scenarios.
     *
     * @return A string representation of the TaskManager instance.
     */
    @Override
    public String toString() {
        return toString("");
    }

    public String toString(final String indent) {
        final StringBuilder builder = new StringBuilder();
        builder.append("TaskManager\n");
        builder.append(indent).append("\tMetadataState:\n");
        builder.append(indent).append("\tActive tasks:\n");
        builder.append(active.toString(indent + "\t\t"));
        builder.append(indent).append("\tStandby tasks:\n");
        builder.append(standby.toString(indent + "\t\t"));
        return builder.toString();
    }

    // this should be safe to call whether the restore consumer is assigned standby or active restoring partitions
    // as the removal will be a no-op
    private void removeChangelogsFromRestoreConsumer(final Collection<TopicPartition> changelogs, final boolean areStandbyPartitions) {
        if (!changelogs.isEmpty() && areStandbyPartitions == restoreConsumerAssignedStandbys) {
            final Set<TopicPartition> updatedAssignment = new HashSet<>(restoreConsumer.assignment());
            updatedAssignment.removeAll(changelogs);
            restoreConsumer.assign(updatedAssignment);
        }
    }

    private Set<TaskId> partitionsToTaskSet(final Collection<TopicPartition> partitions) {
        final Set<TaskId> taskIds = new HashSet<>();
        for (final TopicPartition tp : partitions) {
            final TaskId id = partitionsToTaskId.get(tp);
            if (id != null) {
                taskIds.add(id);
            } else {
                log.error("Failed to lookup taskId for partition {}", tp);
                throw new StreamsException("Found partition in assignment with no corresponding task");
            }
        }
        return taskIds;
    }
}
