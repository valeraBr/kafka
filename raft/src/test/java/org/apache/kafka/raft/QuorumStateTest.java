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
package org.apache.kafka.raft;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.internals.BatchAccumulator;
import org.apache.kafka.raft.internals.VoterSetTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// TODO: Add tests for kraft.version 1
// TODO: add test for votedUuid
public class QuorumStateTest {
    private final int localId = 0;
    private final Uuid localUuid = Uuid.randomUuid();
    private final int logEndEpoch = 0;
    private final MockQuorumStateStore store = new MockQuorumStateStore();
    private final MockTime time = new MockTime();
    private final int electionTimeoutMs = 5000;
    private final int fetchTimeoutMs = 10000;
    private final MockableRandom random = new MockableRandom(1L);
    private final short kraftVersion = 0;

    private final BatchAccumulator<?> accumulator = Mockito.mock(BatchAccumulator.class);

    private QuorumState buildQuorumState(Set<Integer> voters) {
        return buildQuorumState(OptionalInt.of(localId), voters);
    }

    private QuorumState buildQuorumState(
        OptionalInt localId,
        Set<Integer> voters
    ) {
        return new QuorumState(
            localId,
            localUuid,
            () -> VoterSetTest.voterSet(VoterSetTest.voterMap(voters, false)),
            () -> kraftVersion,
            electionTimeoutMs,
            fetchTimeoutMs,
            store,
            time,
            new LogContext(),
            random
        );
    }

    @Test
    public void testInitializePrimordialEpoch() {
        Set<Integer> voters = Utils.mkSet(localId);
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters);
        assertTrue(state.isUnattached());
        assertEquals(0, state.epoch());
        state.transitionToCandidate();
        CandidateState candidateState = state.candidateStateOrThrow();
        assertTrue(candidateState.isVoteGranted());
        assertEquals(1, candidateState.epoch());
    }

    @Test
    public void testInitializeAsUnattached() {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.writeElectionState(ElectionState.withUnknownLeader(epoch, voters), kraftVersion);

        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        QuorumState state = buildQuorumState(voters);
        state.initialize(new OffsetAndEpoch(0L, 0));

        assertTrue(state.isUnattached());
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(epoch, unattachedState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            unattachedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testInitializeAsFollower() {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.writeElectionState(ElectionState.withElectedLeader(epoch, node1, voters), kraftVersion);

        QuorumState state = buildQuorumState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isFollower());
        assertEquals(epoch, state.epoch());

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(epoch, followerState.epoch());
        assertEquals(node1, followerState.leaderId());
        assertEquals(fetchTimeoutMs, followerState.remainingFetchTimeMs(time.milliseconds()));
    }

    @Test
    public void testInitializeAsVoted() {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.writeElectionState(ElectionState.withVotedCandidate(epoch, node1, Optional.empty(), voters), kraftVersion);

        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        QuorumState state = buildQuorumState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isVoted());
        assertEquals(epoch, state.epoch());

        VotedState votedState = state.votedStateOrThrow();
        assertEquals(epoch, votedState.epoch());
        assertEquals(node1, votedState.votedId());
        assertEquals(electionTimeoutMs + jitterMs,
            votedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testInitializeAsResignedCandidate() {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        ElectionState election = ElectionState.withVotedCandidate(epoch, localId, Optional.empty(), voters);
        store.writeElectionState(election, kraftVersion);

        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        QuorumState state = buildQuorumState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isCandidate());
        assertEquals(epoch, state.epoch());

        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(epoch, candidateState.epoch());
        // Even though the persisted value doesn't include the VotedUuid the deserialized value will
        // include the localUuid when the local node is the candidate
        assertEquals(
            ElectionState.withVotedCandidate(epoch, localId, Optional.of(localUuid), voters),
            candidateState.election()
        );
        assertEquals(Utils.mkSet(node1, node2), candidateState.unrecordedVoters());
        assertEquals(Utils.mkSet(localId), candidateState.grantingVoters());
        assertEquals(Collections.emptySet(), candidateState.rejectingVoters());
        assertEquals(
            electionTimeoutMs + jitterMs,
            candidateState.remainingElectionTimeMs(time.milliseconds())
        );
    }

    @Test
    public void testInitializeAsResignedLeader() {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        ElectionState election = ElectionState.withElectedLeader(epoch, localId, voters);
        store.writeElectionState(election, kraftVersion);

        // If we were previously a leader, we will start as resigned in order to ensure
        // a new leader gets elected. This ensures that records are always uniquely
        // defined by epoch and offset even accounting for the loss of unflushed data.

        // The election timeout should be reset after we become a candidate again
        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        QuorumState state = buildQuorumState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertFalse(state.isLeader());
        assertEquals(epoch, state.epoch());

        ResignedState resignedState = state.resignedStateOrThrow();
        assertEquals(epoch, resignedState.epoch());
        assertEquals(election, resignedState.election());
        assertEquals(Utils.mkSet(node1, node2), resignedState.unackedVoters());
        assertEquals(electionTimeoutMs + jitterMs,
            resignedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testCandidateToCandidate() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        assertEquals(1, state.epoch());

        CandidateState candidate1 = state.candidateStateOrThrow();
        candidate1.recordRejectedVote(node2);

        // Check backoff behavior before transitioning
        int backoffMs = 500;
        candidate1.startBackingOff(time.milliseconds(), backoffMs);
        assertTrue(candidate1.isBackingOff());
        assertFalse(candidate1.isBackoffComplete(time.milliseconds()));

        time.sleep(backoffMs - 1);
        assertTrue(candidate1.isBackingOff());
        assertFalse(candidate1.isBackoffComplete(time.milliseconds()));

        time.sleep(1);
        assertTrue(candidate1.isBackingOff());
        assertTrue(candidate1.isBackoffComplete(time.milliseconds()));

        // The election timeout should be reset after we become a candidate again
        int jitterMs = 2500;
        random.mockNextInt(jitterMs);

        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        CandidateState candidate2 = state.candidateStateOrThrow();
        assertEquals(2, state.epoch());
        assertEquals(Collections.singleton(localId), candidate2.grantingVoters());
        assertEquals(Collections.emptySet(), candidate2.rejectingVoters());
        assertEquals(electionTimeoutMs + jitterMs,
            candidate2.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testCandidateToResigned() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        assertEquals(1, state.epoch());

        assertThrows(IllegalStateException.class, () ->
            state.transitionToResigned(Collections.singletonList(localId)));
        assertTrue(state.isCandidate());
    }

    @Test
    public void testCandidateToLeader()  {
        Set<Integer> voters = Utils.mkSet(localId);
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        assertEquals(1, state.epoch());

        state.transitionToLeader(0L, accumulator);
        LeaderState<?> leaderState = state.leaderStateOrThrow();
        assertTrue(state.isLeader());
        assertEquals(1, leaderState.epoch());
        assertEquals(Optional.empty(), leaderState.highWatermark());
    }

    @Test
    public void testCandidateToLeaderWithoutGrantedVote() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        assertFalse(state.candidateStateOrThrow().isVoteGranted());
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        assertTrue(state.candidateStateOrThrow().isVoteGranted());
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
    }

    @Test
    public void testCandidateToFollower() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();

        state.transitionToFollower(5, otherNodeId);
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.of(otherNodeId), state.leaderId());
        assertEquals(
            Optional.of(ElectionState.withElectedLeader(5, otherNodeId, voters)),
            store.readElectionState()
        );
    }

    @Test
    public void testCandidateToUnattached() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();

        state.transitionToUnattached(5);
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        assertEquals(
            Optional.of(ElectionState.withUnknownLeader(5, voters)),
            store.readElectionState()
        );
    }

    @Test
    public void testCandidateToVoted() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();

        state.transitionToVoted(5, otherNodeId, Optional.empty());
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());

        VotedState followerState = state.votedStateOrThrow();
        assertEquals(otherNodeId, followerState.votedId());
        assertEquals(
            Optional.of(ElectionState.withVotedCandidate(5, otherNodeId, Optional.empty(), voters)),
            store.readElectionState()
        );
    }

    @Test
    public void testCandidateToAnyStateLowerEpoch() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToCandidate();
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(4, otherNodeId, Optional.empty()));
        assertThrows(IllegalStateException.class, () -> state.transitionToFollower(4, otherNodeId));
        assertEquals(6, state.epoch());
        assertEquals(
            Optional.of(ElectionState.withVotedCandidate(6, localId, Optional.empty(), voters)),
            store.readElectionState()
        );
    }

    @Test
    public void testLeaderToLeader() {
        Set<Integer> voters = Utils.mkSet(localId);
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());

        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());
    }

    @Test
    public void testLeaderToResigned() {
        Set<Integer> voters = Utils.mkSet(localId);
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());

        state.transitionToResigned(Collections.singletonList(localId));
        assertTrue(state.isResigned());
        ResignedState resignedState = state.resignedStateOrThrow();
        assertEquals(ElectionState.withElectedLeader(1, localId, voters),
            resignedState.election());
        assertEquals(1, resignedState.epoch());
        assertEquals(Collections.emptySet(), resignedState.unackedVoters());
    }

    @Test
    public void testLeaderToCandidate() {
        Set<Integer> voters = Utils.mkSet(localId);
        assertEquals(Optional.empty(), store.readElectionState());

        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.transitionToLeader(0L, accumulator);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());

        assertThrows(IllegalStateException.class, state::transitionToCandidate);
        assertTrue(state.isLeader());
        assertEquals(1, state.epoch());
    }

    @Test
    public void testLeaderToFollower() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        QuorumState state = initializeEmptyState(voters);

        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L, accumulator);
        state.transitionToFollower(5, otherNodeId);

        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.of(otherNodeId), state.leaderId());
        assertEquals(
            Optional.of(ElectionState.withElectedLeader(5, otherNodeId, voters)),
            store.readElectionState()
        );
    }

    @Test
    public void testLeaderToUnattached() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L, accumulator);
        state.transitionToUnattached(5);
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        assertEquals(
            Optional.of(ElectionState.withUnknownLeader(5, voters)),
            store.readElectionState()
        );
    }

    @Test
    public void testLeaderToVoted() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L, accumulator);
        state.transitionToVoted(5, otherNodeId, Optional.empty());

        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        VotedState votedState = state.votedStateOrThrow();
        assertEquals(otherNodeId, votedState.votedId());
        assertEquals(
            Optional.of(ElectionState.withVotedCandidate(5, otherNodeId, Optional.empty(), voters)),
            store.readElectionState()
        );
    }

    @Test
    public void testLeaderToAnyStateLowerEpoch() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L, accumulator);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(4, otherNodeId, Optional.empty()));
        assertThrows(IllegalStateException.class, () -> state.transitionToFollower(4, otherNodeId));
        assertEquals(6, state.epoch());
        assertEquals(
            Optional.of(ElectionState.withElectedLeader(6, localId, voters)),
            store.readElectionState()
        );
    }

    @Test
    public void testCannotFollowOrVoteForSelf() {
        Set<Integer> voters = Utils.mkSet(localId);
        assertEquals(Optional.empty(), store.readElectionState());
        QuorumState state = initializeEmptyState(voters);

        assertThrows(IllegalStateException.class, () -> state.transitionToFollower(0, localId));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(0, localId, Optional.empty()));
    }

    @Test
    public void testUnattachedToLeaderOrResigned() {
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, leaderId);
        store.writeElectionState(ElectionState.withVotedCandidate(epoch, leaderId, Optional.empty(), voters), kraftVersion);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isUnattached());
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));
        assertThrows(IllegalStateException.class, () -> state.transitionToResigned(Collections.emptyList()));
    }

    @Test
    public void testUnattachedToVotedSameEpoch() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToVoted(5, otherNodeId, Optional.empty());

        VotedState votedState = state.votedStateOrThrow();
        assertEquals(5, votedState.epoch());
        assertEquals(otherNodeId, votedState.votedId());
        assertEquals(
            Optional.of(ElectionState.withVotedCandidate(5, otherNodeId, Optional.empty(), voters)),
            store.readElectionState()
        );

        // Verify election timeout is reset when we vote for a candidate
        assertEquals(electionTimeoutMs + jitterMs,
            votedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testUnattachedToVotedHigherEpoch() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToVoted(8, otherNodeId, Optional.empty());

        VotedState votedState = state.votedStateOrThrow();
        assertEquals(8, votedState.epoch());
        assertEquals(otherNodeId, votedState.votedId());
        assertEquals(
            Optional.of(ElectionState.withVotedCandidate(8, otherNodeId, Optional.empty(), voters)),
            store.readElectionState()
        );
    }

    @Test
    public void testUnattachedToCandidate() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToCandidate();

        assertTrue(state.isCandidate());
        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(6, candidateState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            candidateState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testUnattachedToUnattached() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);

        long remainingElectionTimeMs = state.unattachedStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        time.sleep(1000);

        state.transitionToUnattached(6);
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(6, unattachedState.epoch());

        // Verify that the election timer does not get reset
        assertEquals(remainingElectionTimeMs - 1000,
            unattachedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testUnattachedToFollowerSameEpoch() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);

        state.transitionToFollower(5, otherNodeId);
        assertTrue(state.isFollower());
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(5, followerState.epoch());
        assertEquals(otherNodeId, followerState.leaderId());
        assertEquals(fetchTimeoutMs, followerState.remainingFetchTimeMs(time.milliseconds()));
    }

    @Test
    public void testUnattachedToFollowerHigherEpoch() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);

        state.transitionToFollower(8, otherNodeId);
        assertTrue(state.isFollower());
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertEquals(otherNodeId, followerState.leaderId());
        assertEquals(fetchTimeoutMs, followerState.remainingFetchTimeMs(time.milliseconds()));
    }

    @Test
    public void testUnattachedToAnyStateLowerEpoch() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(4, otherNodeId, Optional.empty()));
        assertThrows(IllegalStateException.class, () -> state.transitionToFollower(4, otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(
            Optional.of(ElectionState.withUnknownLeader(5, voters)),
            store.readElectionState()
        );
    }

    @Test
    public void testVotedToInvalidLeaderOrResigned() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, node1, Optional.empty());
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0, accumulator));
        assertThrows(IllegalStateException.class, () -> state.transitionToResigned(Collections.emptyList()));
    }

    @Test
    public void testVotedToCandidate() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, node1, Optional.empty());

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(6, candidateState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            candidateState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testVotedToVotedSameEpoch() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToUnattached(5);
        state.transitionToVoted(8, node1, Optional.empty());
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(8, node1, Optional.empty()));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(8, node2, Optional.empty()));
    }

    @Test
    public void testVotedToFollowerSameEpoch() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, node1, Optional.empty());
        state.transitionToFollower(5, node2);

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(5, followerState.epoch());
        assertEquals(node2, followerState.leaderId());
        assertEquals(
            Optional.of(ElectionState.withElectedLeader(5, node2, voters)),
            store.readElectionState()
        );
    }

    @Test
    public void testVotedToFollowerHigherEpoch() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, node1, Optional.empty());
        state.transitionToFollower(8, node2);

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertEquals(node2, followerState.leaderId());
        assertEquals(
            Optional.of(ElectionState.withElectedLeader(8, node2, voters)),
            store.readElectionState()
        );
    }

    @Test
    public void testVotedToUnattachedSameEpoch() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, node1, Optional.empty());
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(5));
    }

    @Test
    public void testVotedToUnattachedHigherEpoch() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, otherNodeId, Optional.empty());

        long remainingElectionTimeMs = state.votedStateOrThrow().remainingElectionTimeMs(time.milliseconds());
        time.sleep(1000);

        state.transitionToUnattached(6);
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(6, unattachedState.epoch());

        // Verify that the election timer does not get reset
        assertEquals(remainingElectionTimeMs - 1000,
            unattachedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testVotedToAnyStateLowerEpoch() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToVoted(5, otherNodeId, Optional.empty());
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(4, otherNodeId, Optional.empty()));
        assertThrows(IllegalStateException.class, () -> state.transitionToFollower(4, otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(
            Optional.of(ElectionState.withVotedCandidate(5, otherNodeId, Optional.empty(), voters)),
            store.readElectionState()
        );
    }

    @Test
    public void testFollowerToFollowerSameEpoch() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(8, node2);
        assertThrows(IllegalStateException.class, () -> state.transitionToFollower(8, node1));
        assertThrows(IllegalStateException.class, () -> state.transitionToFollower(8, node2));

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertEquals(node2, followerState.leaderId());
        assertEquals(
            Optional.of(ElectionState.withElectedLeader(8, node2, voters)),
            store.readElectionState()
        );
    }

    @Test
    public void testFollowerToFollowerHigherEpoch() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(8, node2);
        state.transitionToFollower(9, node1);

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(9, followerState.epoch());
        assertEquals(node1, followerState.leaderId());
        assertEquals(
            Optional.of(ElectionState.withElectedLeader(9, node1, voters)),
            store.readElectionState()
        );
    }

    @Test
    public void testFollowerToLeaderOrResigned() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(8, node2);
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0, accumulator));
        assertThrows(IllegalStateException.class, () -> state.transitionToResigned(Collections.emptyList()));
    }

    @Test
    public void testFollowerToCandidate() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(8, node2);

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToCandidate();
        assertTrue(state.isCandidate());
        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(9, candidateState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            candidateState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testFollowerToUnattachedSameEpoch() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(8, node2);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(8));
    }

    @Test
    public void testFollowerToUnattachedHigherEpoch() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(8, node2);

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToUnattached(9);
        assertTrue(state.isUnattached());
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(9, unattachedState.epoch());
        assertEquals(electionTimeoutMs + jitterMs,
            unattachedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testFollowerToVotedSameEpoch() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(8, node2);

        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(8, node1, Optional.empty()));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(8, localId, Optional.empty()));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(8, node2, Optional.empty()));
    }

    @Test
    public void testFollowerToVotedHigherEpoch() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(8, node2);

        int jitterMs = 2500;
        random.mockNextInt(electionTimeoutMs, jitterMs);
        state.transitionToVoted(9, node1, Optional.empty());
        assertTrue(state.isVoted());
        VotedState votedState = state.votedStateOrThrow();
        assertEquals(9, votedState.epoch());
        assertEquals(node1, votedState.votedId());
        assertEquals(electionTimeoutMs + jitterMs,
            votedState.remainingElectionTimeMs(time.milliseconds()));
    }

    @Test
    public void testFollowerToAnyStateLowerEpoch() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.transitionToFollower(5, otherNodeId);
        assertThrows(IllegalStateException.class, () -> state.transitionToUnattached(4));
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(4, otherNodeId, Optional.empty()));
        assertThrows(IllegalStateException.class, () -> state.transitionToFollower(4, otherNodeId));
        assertEquals(5, state.epoch());
        assertEquals(
            Optional.of(ElectionState.withElectedLeader(5, otherNodeId, voters)),
            store.readElectionState()
        );
    }

    @Test
    public void testCanBecomeFollowerOfNonVoter() {
        int otherNodeId = 1;
        int nonVoterId = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));

        // Transition to voted
        state.transitionToVoted(4, nonVoterId, Optional.empty());
        assertTrue(state.isVoted());
        VotedState votedState = state.votedStateOrThrow();
        assertEquals(4, votedState.epoch());
        assertEquals(nonVoterId, votedState.votedId());

        // Transition to follower
        state.transitionToFollower(4, nonVoterId);
        assertEquals(new LeaderAndEpoch(OptionalInt.of(nonVoterId), 4), state.leaderAndEpoch());
    }

    @Test
    public void testObserverCannotBecomeCandidateOrLeader() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());
        assertThrows(IllegalStateException.class, state::transitionToCandidate);
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));
    }

    @Test
    public void testObserverWithIdCanVote() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(otherNodeId);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());
        state.transitionToVoted(5, otherNodeId, Optional.empty());
        assertTrue(state.isVoted());
        VotedState votedState = state.votedStateOrThrow();
        assertEquals(5, votedState.epoch());
        assertEquals(otherNodeId, votedState.votedId());
    }

    @Test
    public void testObserverFollowerToUnattached() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());

        state.transitionToFollower(2, node1);
        state.transitionToUnattached(3);
        assertTrue(state.isUnattached());
        UnattachedState unattachedState = state.unattachedStateOrThrow();
        assertEquals(3, unattachedState.epoch());

        // Observers can remain in the unattached state indefinitely until a leader is found
        assertEquals(Long.MAX_VALUE, unattachedState.electionTimeoutMs());
    }

    @Test
    public void testObserverUnattachedToFollower() {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(node1, node2);
        QuorumState state = initializeEmptyState(voters);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());

        state.transitionToUnattached(2);
        state.transitionToFollower(3, node1);
        assertTrue(state.isFollower());
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(3, followerState.epoch());
        assertEquals(node1, followerState.leaderId());
        assertEquals(fetchTimeoutMs, followerState.remainingFetchTimeMs(time.milliseconds()));
    }

    @Test
    public void testInitializeWithCorruptedStore() {
        QuorumStateStore stateStore = Mockito.mock(QuorumStateStore.class);
        Mockito.doThrow(UncheckedIOException.class).when(stateStore).readElectionState();

        QuorumState state = buildQuorumState(Utils.mkSet(localId));

        int epoch = 2;
        state.initialize(new OffsetAndEpoch(0L, epoch));
        assertEquals(epoch, state.epoch());
        assertTrue(state.isUnattached());
        assertFalse(state.hasLeader());
    }

    @Test
    public void testHasRemoteLeader() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        QuorumState state = initializeEmptyState(voters);
        assertFalse(state.hasRemoteLeader());

        state.transitionToCandidate();
        assertFalse(state.hasRemoteLeader());

        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.transitionToLeader(0L, accumulator);
        assertFalse(state.hasRemoteLeader());

        state.transitionToUnattached(state.epoch() + 1);
        assertFalse(state.hasRemoteLeader());

        state.transitionToVoted(state.epoch() + 1, otherNodeId, Optional.empty());
        assertFalse(state.hasRemoteLeader());

        state.transitionToFollower(state.epoch() + 1, otherNodeId);
        assertTrue(state.hasRemoteLeader());
    }

    @Test
    public void testHighWatermarkRetained() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        QuorumState state = initializeEmptyState(voters);
        state.transitionToFollower(5, otherNodeId);

        FollowerState followerState = state.followerStateOrThrow();
        followerState.updateHighWatermark(OptionalLong.of(10L));

        Optional<LogOffsetMetadata> highWatermark = Optional.of(new LogOffsetMetadata(10L));
        assertEquals(highWatermark, state.highWatermark());

        state.transitionToUnattached(6);
        assertEquals(highWatermark, state.highWatermark());

        state.transitionToVoted(7, otherNodeId, Optional.empty());
        assertEquals(highWatermark, state.highWatermark());

        state.transitionToCandidate();
        assertEquals(highWatermark, state.highWatermark());

        CandidateState candidateState = state.candidateStateOrThrow();
        candidateState.recordGrantedVote(otherNodeId);
        assertTrue(candidateState.isVoteGranted());

        state.transitionToLeader(10L, accumulator);
        assertEquals(Optional.empty(), state.highWatermark());
    }

    @Test
    public void testInitializeWithEmptyLocalId() {
        QuorumState state = buildQuorumState(OptionalInt.empty(), Utils.mkSet(0, 1));
        state.initialize(new OffsetAndEpoch(0L, 0));

        assertTrue(state.isObserver());
        assertFalse(state.isVoter());

        assertThrows(IllegalStateException.class, state::transitionToCandidate);
        assertThrows(IllegalStateException.class, () -> state.transitionToVoted(1, 1, Optional.empty()));
        assertThrows(IllegalStateException.class, () -> state.transitionToLeader(0L, accumulator));

        state.transitionToFollower(1, 1);
        assertTrue(state.isFollower());

        state.transitionToUnattached(2);
        assertTrue(state.isUnattached());
    }

    @Test
    public void testObserverInitializationFailsIfElectionStateHasVotedCandidate() {
        Set<Integer> voters = Utils.mkSet(0, 1);
        int epoch = 5;
        int votedId = 1;

        store.writeElectionState(
            ElectionState.withVotedCandidate(epoch, votedId, Optional.empty(), voters),
            kraftVersion
        );

        QuorumState state1 = buildQuorumState(OptionalInt.of(2), voters);
        assertThrows(IllegalStateException.class, () -> state1.initialize(new OffsetAndEpoch(0, 0)));

        QuorumState state2 = buildQuorumState(OptionalInt.empty(), voters);
        assertThrows(IllegalStateException.class, () -> state2.initialize(new OffsetAndEpoch(0, 0)));
    }

    private QuorumState initializeEmptyState(Set<Integer> voters) {
        QuorumState state = buildQuorumState(voters);
        store.writeElectionState(ElectionState.withUnknownLeader(0, voters), kraftVersion);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        return state;
    }
}
