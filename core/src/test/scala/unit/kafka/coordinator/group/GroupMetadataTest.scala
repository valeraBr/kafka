/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.coordinator.group

import java.util

import kafka.common.OffsetAndMetadata
import kafka.utils.timer.MockTimer
import org.apache.kafka.clients.consumer.internals.{ConsumerProtocol, PartitionAssignor}
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.{Before, Test}

/**
 * Test group state transitions and other GroupMetadata functionality
 */
class GroupMetadataTest {
  private val protocolType = "consumer"
  private val groupId = "groupId"
  private val groupInstanceId = Some("groupInstanceId")
  private val clientId = "clientId"
  private val clientHost = "clientHost"
  private val rebalanceTimeoutMs = 60000
  private val sessionTimeoutMs = 10000

  private var group: GroupMetadata = null
  private var timer: MockTimer = null

  @Before
  def setUp() {
    timer = new MockTimer
    group = new GroupMetadata("groupId", Empty, timer.time)
  }

  @Test
  def testCanRebalanceWhenStable() {
    assertTrue(group.canRebalance)
  }

  @Test
  def testCanRebalanceWhenCompletingRebalance() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(CompletingRebalance)
    assertTrue(group.canRebalance)
  }

  @Test
  def testCannotRebalanceWhenPreparingRebalance() {
    group.transitionTo(PreparingRebalance)
    assertFalse(group.canRebalance)
  }

  @Test
  def testCannotRebalanceWhenDead() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Empty)
    group.transitionTo(Dead)
    assertFalse(group.canRebalance)
  }

  @Test
  def testStableToPreparingRebalanceTransition() {
    group.transitionTo(PreparingRebalance)
    assertState(group, PreparingRebalance)
  }

  @Test
  def testStableToDeadTransition() {
    group.transitionTo(Dead)
    assertState(group, Dead)
  }

  @Test
  def testAwaitingRebalanceToPreparingRebalanceTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(CompletingRebalance)
    group.transitionTo(PreparingRebalance)
    assertState(group, PreparingRebalance)
  }

  @Test
  def testPreparingRebalanceToDeadTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    assertState(group, Dead)
  }

  @Test
  def testPreparingRebalanceToEmptyTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Empty)
    assertState(group, Empty)
  }

  @Test
  def testEmptyToDeadTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Empty)
    group.transitionTo(Dead)
    assertState(group, Dead)
  }

  @Test
  def testAwaitingRebalanceToStableTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(CompletingRebalance)
    group.transitionTo(Stable)
    assertState(group, Stable)
  }

  @Test(expected = classOf[IllegalStateException])
  def testEmptyToStableIllegalTransition() {
    group.transitionTo(Stable)
  }

  @Test
  def testStableToStableIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(CompletingRebalance)
    group.transitionTo(Stable)
    try {
      group.transitionTo(Stable)
      fail("should have failed due to illegal transition")
    } catch {
      case e: IllegalStateException => // ok
    }
  }

  @Test(expected = classOf[IllegalStateException])
  def testEmptyToAwaitingRebalanceIllegalTransition() {
    group.transitionTo(CompletingRebalance)
  }

  @Test(expected = classOf[IllegalStateException])
  def testPreparingRebalanceToPreparingRebalanceIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(PreparingRebalance)
  }

  @Test(expected = classOf[IllegalStateException])
  def testPreparingRebalanceToStableIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Stable)
  }

  @Test(expected = classOf[IllegalStateException])
  def testAwaitingRebalanceToAwaitingRebalanceIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(CompletingRebalance)
    group.transitionTo(CompletingRebalance)
  }

  def testDeadToDeadIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    group.transitionTo(Dead)
    assertState(group, Dead)
  }

  @Test(expected = classOf[IllegalStateException])
  def testDeadToStableIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    group.transitionTo(Stable)
  }

  @Test(expected = classOf[IllegalStateException])
  def testDeadToPreparingRebalanceIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    group.transitionTo(PreparingRebalance)
  }

  @Test(expected = classOf[IllegalStateException])
  def testDeadToAwaitingRebalanceIllegalTransition() {
    group.transitionTo(PreparingRebalance)
    group.transitionTo(Dead)
    group.transitionTo(CompletingRebalance)
  }

  @Test
  def testSelectProtocol() {
    val memberId = "memberId"
    val member = new MemberMetadata(memberId, groupId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))

    group.add(member)
    assertEquals("range", group.selectProtocol)

    val otherMemberId = "otherMemberId"
    val otherMember = new MemberMetadata(otherMemberId, groupId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, List(("roundrobin", Array.empty[Byte]), ("range", Array.empty[Byte])))

    group.add(otherMember)
    // now could be either range or robin since there is no majority preference
    assertTrue(Set("range", "roundrobin")(group.selectProtocol))

    val lastMemberId = "lastMemberId"
    val lastMember = new MemberMetadata(lastMemberId, groupId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, List(("roundrobin", Array.empty[Byte]), ("range", Array.empty[Byte])))

    group.add(lastMember)
    // now we should prefer 'roundrobin'
    assertEquals("roundrobin", group.selectProtocol)
  }

  @Test(expected = classOf[IllegalStateException])
  def testSelectProtocolRaisesIfNoMembers() {
    group.selectProtocol
    fail()
  }

  @Test
  def testSelectProtocolChoosesCompatibleProtocol() {
    val memberId = "memberId"
    val member = new MemberMetadata(memberId, groupId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))

    val otherMemberId = "otherMemberId"
    val otherMember = new MemberMetadata(otherMemberId, groupId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, List(("roundrobin", Array.empty[Byte]), ("blah", Array.empty[Byte])))

    group.add(member)
    group.add(otherMember)
    assertEquals("roundrobin", group.selectProtocol)
  }

  @Test
  def testSupportsProtocols() {
    // by default, the group supports everything
    assertTrue(group.supportsProtocols(protocolType, Set("roundrobin", "range")))

    val memberId = "memberId"
    val member = new MemberMetadata(memberId, groupId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, List(("range", Array.empty[Byte]), ("roundrobin", Array.empty[Byte])))

    group.add(member)
    group.transitionTo(PreparingRebalance)
    assertTrue(group.supportsProtocols(protocolType, Set("roundrobin", "foo")))
    assertTrue(group.supportsProtocols(protocolType, Set("range", "foo")))
    assertFalse(group.supportsProtocols(protocolType, Set("foo", "bar")))

    val otherMemberId = "otherMemberId"
    val otherMember = new MemberMetadata(otherMemberId, groupId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, List(("roundrobin", Array.empty[Byte]), ("blah", Array.empty[Byte])))

    group.add(otherMember)

    assertTrue(group.supportsProtocols(protocolType, Set("roundrobin", "foo")))
    assertFalse(group.supportsProtocols("invalid_type", Set("roundrobin", "foo")))
    assertFalse(group.supportsProtocols(protocolType, Set("range", "foo")))
  }

  @Test
  def testInitNextGeneration() {
    val memberId = "memberId"
    val member = new MemberMetadata(memberId, groupId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, List(("roundrobin", Array.empty[Byte])))

    group.transitionTo(PreparingRebalance)
    group.add(member, _ => ())

    assertEquals(0, group.generationId)
    assertNull(group.protocolOrNull)

    group.initNextGeneration()

    assertEquals(1, group.generationId)
    assertEquals("roundrobin", group.protocolOrNull)
  }

  @Test
  def testInitNextGenerationEmptyGroup() {
    assertEquals(Empty, group.currentState)
    assertEquals(0, group.generationId)
    assertNull(group.protocolOrNull)

    group.transitionTo(PreparingRebalance)
    group.initNextGeneration()

    assertEquals(1, group.generationId)
    assertNull(group.protocolOrNull)
  }

  @Test
  def testOffsetCommit(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val offset = offsetAndMetadata(37)
    val commitRecordOffset = 3

    group.prepareOffsetCommit(Map(partition -> offset))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.onOffsetCommitAppend(partition, CommitRecordMetadataAndOffset(Some(commitRecordOffset), offset))
    assertTrue(group.hasOffsets)
    assertEquals(Some(offset), group.offset(partition))
  }

  @Test
  def testStableGroupOnlyChooseUnsubscribedExpiredOffsets(): Unit = {
    val tp1 = new TopicPartition("unsubscribed_and_expired", 0)
    val tp2 = new TopicPartition("unsubscribed_and_unexpired", 0)
    val tp3 = new TopicPartition("subscribed_and_expired", 0)
    val tp4 = new TopicPartition("subscribed_and_unexpired", 0)
    val retentionMs = 10000
    val firstCommitTimestamp = timer.time.milliseconds
    var offsetAndMetadata = OffsetAndMetadata(37, "", firstCommitTimestamp)
    val commitRecordOffset = Some(3L)

    val group = new GroupMetadata("groupId", Stable, timer.time)
    val member = new MemberMetadata("memberId", groupId, groupInstanceId, clientId, clientHost,
      rebalanceTimeoutMs, sessionTimeoutMs,
      protocolType, List(("range", Array.empty[Byte]), ("range", Array.empty[Byte])))
    member.assignment = ConsumerProtocol.serializeAssignment(
      new PartitionAssignor.Assignment(util.Arrays.asList(tp3, tp4), null)).array
    group.add(member)
    group.currentStateTimestamp = None // Use the given timestamp instead

    group.prepareOffsetCommit(Map(tp1 -> offsetAndMetadata, tp3 -> offsetAndMetadata))
    group.onOffsetCommitAppend(tp1, CommitRecordMetadataAndOffset(commitRecordOffset, offsetAndMetadata))
    group.onOffsetCommitAppend(tp3, CommitRecordMetadataAndOffset(commitRecordOffset, offsetAndMetadata))

    timer.advanceClock(retentionMs + 1000)
    offsetAndMetadata = OffsetAndMetadata(37, "", timer.time.milliseconds)
    group.prepareOffsetCommit(Map(tp2 -> offsetAndMetadata, tp4 -> offsetAndMetadata))
    group.onOffsetCommitAppend(tp2, CommitRecordMetadataAndOffset(commitRecordOffset, offsetAndMetadata))
    group.onOffsetCommitAppend(tp4, CommitRecordMetadataAndOffset(commitRecordOffset, offsetAndMetadata))
    timer.advanceClock(1)

    val expectedOffsets = Map(tp1 -> OffsetAndMetadata(37, "", firstCommitTimestamp))
    val expiredOffsets = group.removeExpiredOffsets(timer.time.milliseconds, retentionMs)
    assertEquals(expectedOffsets, expiredOffsets)
  }

  @Test
  def testOffsetCommitFailure(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val offset = offsetAndMetadata(37)

    group.prepareOffsetCommit(Map(partition -> offset))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.failPendingOffsetWrite(partition, offset)
    assertFalse(group.hasOffsets)
    assertEquals(None, group.offset(partition))
  }

  @Test
  def testOffsetCommitFailureWithAnotherPending(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val firstOffset = offsetAndMetadata(37)
    val secondOffset = offsetAndMetadata(57)

    group.prepareOffsetCommit(Map(partition -> firstOffset))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.prepareOffsetCommit(Map(partition -> secondOffset))
    assertTrue(group.hasOffsets)

    group.failPendingOffsetWrite(partition, firstOffset)
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.onOffsetCommitAppend(partition, CommitRecordMetadataAndOffset(Some(3L), secondOffset))
    assertTrue(group.hasOffsets)
    assertEquals(Some(secondOffset), group.offset(partition))
  }

  @Test
  def testOffsetCommitWithAnotherPending(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val firstOffset = offsetAndMetadata(37)
    val secondOffset = offsetAndMetadata(57)

    group.prepareOffsetCommit(Map(partition -> firstOffset))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.prepareOffsetCommit(Map(partition -> secondOffset))
    assertTrue(group.hasOffsets)

    group.onOffsetCommitAppend(partition, CommitRecordMetadataAndOffset(Some(4L), firstOffset))
    assertTrue(group.hasOffsets)
    assertEquals(Some(firstOffset), group.offset(partition))

    group.onOffsetCommitAppend(partition, CommitRecordMetadataAndOffset(Some(5L), secondOffset))
    assertTrue(group.hasOffsets)
    assertEquals(Some(secondOffset), group.offset(partition))
  }

  @Test
  def testConsumerBeatsTransactionalOffsetCommit(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val producerId = 13232L
    val txnOffsetCommit = offsetAndMetadata(37)
    val consumerOffsetCommit = offsetAndMetadata(57)

    group.prepareTxnOffsetCommit(producerId, Map(partition -> txnOffsetCommit))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.prepareOffsetCommit(Map(partition -> consumerOffsetCommit))
    assertTrue(group.hasOffsets)

    group.onTxnOffsetCommitAppend(producerId, partition, CommitRecordMetadataAndOffset(Some(3L), txnOffsetCommit))
    group.onOffsetCommitAppend(partition, CommitRecordMetadataAndOffset(Some(4L), consumerOffsetCommit))
    assertTrue(group.hasOffsets)
    assertEquals(Some(consumerOffsetCommit), group.offset(partition))

    group.completePendingTxnOffsetCommit(producerId, isCommit = true)
    assertTrue(group.hasOffsets)
    // This is the crucial assertion which validates that we materialize offsets in offset order, not transactional order.
    assertEquals(Some(consumerOffsetCommit), group.offset(partition))
  }

  @Test
  def testTransactionBeatsConsumerOffsetCommit(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val producerId = 13232L
    val txnOffsetCommit = offsetAndMetadata(37)
    val consumerOffsetCommit = offsetAndMetadata(57)

    group.prepareTxnOffsetCommit(producerId, Map(partition -> txnOffsetCommit))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.prepareOffsetCommit(Map(partition -> consumerOffsetCommit))
    assertTrue(group.hasOffsets)

    group.onOffsetCommitAppend(partition, CommitRecordMetadataAndOffset(Some(3L), consumerOffsetCommit))
    group.onTxnOffsetCommitAppend(producerId, partition, CommitRecordMetadataAndOffset(Some(4L), txnOffsetCommit))
    assertTrue(group.hasOffsets)
    // The transactional offset commit hasn't been committed yet, so we should materialize the consumer offset commit.
    assertEquals(Some(consumerOffsetCommit), group.offset(partition))

    group.completePendingTxnOffsetCommit(producerId, isCommit = true)
    assertTrue(group.hasOffsets)
    // The transactional offset commit has been materialized and the transactional commit record is later in the log,
    // so it should be materialized.
    assertEquals(Some(txnOffsetCommit), group.offset(partition))
  }

  @Test
  def testTransactionalCommitIsAbortedAndConsumerCommitWins(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val producerId = 13232L
    val txnOffsetCommit = offsetAndMetadata(37)
    val consumerOffsetCommit = offsetAndMetadata(57)

    group.prepareTxnOffsetCommit(producerId, Map(partition -> txnOffsetCommit))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))

    group.prepareOffsetCommit(Map(partition -> consumerOffsetCommit))
    assertTrue(group.hasOffsets)

    group.onOffsetCommitAppend(partition, CommitRecordMetadataAndOffset(Some(3L), consumerOffsetCommit))
    group.onTxnOffsetCommitAppend(producerId, partition, CommitRecordMetadataAndOffset(Some(4L), txnOffsetCommit))
    assertTrue(group.hasOffsets)
    // The transactional offset commit hasn't been committed yet, so we should materialize the consumer offset commit.
    assertEquals(Some(consumerOffsetCommit), group.offset(partition))

    group.completePendingTxnOffsetCommit(producerId, isCommit = false)
    assertTrue(group.hasOffsets)
    // The transactional offset commit should be discarded and the consumer offset commit should continue to be
    // materialized.
    assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId))
    assertEquals(Some(consumerOffsetCommit), group.offset(partition))
  }

  @Test
  def testFailedTxnOffsetCommitLeavesNoPendingState(): Unit = {
    val partition = new TopicPartition("foo", 0)
    val producerId = 13232L
    val txnOffsetCommit = offsetAndMetadata(37)

    group.prepareTxnOffsetCommit(producerId, Map(partition -> txnOffsetCommit))
    assertTrue(group.hasPendingOffsetCommitsFromProducer(producerId))
    assertTrue(group.hasOffsets)
    assertEquals(None, group.offset(partition))
    group.failPendingTxnOffsetCommit(producerId, partition)
    assertFalse(group.hasOffsets)
    assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId))

    // The commit marker should now have no effect.
    group.completePendingTxnOffsetCommit(producerId, isCommit = true)
    assertFalse(group.hasOffsets)
    assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId))
  }

  private def assertState(group: GroupMetadata, targetState: GroupState) {
    val states: Set[GroupState] = Set(Stable, PreparingRebalance, CompletingRebalance, Dead)
    val otherStates = states - targetState
    otherStates.foreach { otherState =>
      assertFalse(group.is(otherState))
    }
    assertTrue(group.is(targetState))
  }

  private def offsetAndMetadata(offset: Long): OffsetAndMetadata = {
    OffsetAndMetadata(offset, "", timer.time.milliseconds)
  }

}
