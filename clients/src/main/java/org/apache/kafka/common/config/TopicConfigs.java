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

package org.apache.kafka.common.config;

/**
 * Keys that can be used to configure a topic.  These keys are useful when creating or reconfiguring a
 * topic using the AdminClient.
 *
 * This is a public API, so we should not remove or alter keys without a discussion and a deprecation period.
 * Eventually this should replace LogConfig.scala.
 *
 * Note that some of these are subtly different from the configuration keys that control
 * the broker defaults.  For example, to set the default broker cleanup policy, you set log.cleanup.policy,
 * but to set the per-topic cleanup policy, you set cleanup.policy.
 */
public class TopicConfigs {
    public static final String SEGMENT_BYTES = "segment.bytes";
    public static final String SEGMENT_BYTES_DOC = "This configuration controls the segment file size for " +
        "the log. Retention and cleaning is always done a file at a time so a larger segment size means " +
        "fewer files but less granular control over retention.";

    public static final String SEGMENT_MS = "segment.ms";
    public static final String SEGMENT_MS_DOC = "This configuration controls the period of time after " +
        "which Kafka will force the log to roll even if the segment file isn't full to ensure that retention " +
        "can delete or compact old data.";

    public static final String SEGMENT_JITTER_MS = "segment.jitter.ms";
    public static final String SEGMENT_JITTER_MS_DOC = "The maximum random jitter subtracted from the scheduled " +
        "segment roll time to avoid thundering herds of segment rolling";

    public static final String SEGMENT_INDEX_BYTES = "segment.index.bytes";
    public static final String SEGMENT_INDEX_BYTES_DOC = "This configuration controls the size of the index that " +
        "maps offsets to file positions. We preallocate this index file and shrink it only after log " +
        "rolls. You generally should not need to change this setting.";

    public static final String FLUSH_MESSAGES_INTERVAL = "flush.messages";
    public static final String FLUSH_MESSAGES_INTERVAL_DOC = "This setting allows specifying an interval at " +
        "which we will force an fsync of data written to the log. For example if this was set to 1 " +
        "we would fsync after every message; if it were 5 we would fsync after every five messages. " +
        "In general we recommend you not set this and use replication for durability and allow the " +
        "operating system's background flush capabilities as it is more efficient. This setting can " +
        "be overridden on a per-topic basis (see <a href=\"#topic-config\">the per-topic configuration section</a>).";

    public static final String FLUSH_MS = "flush.ms";
    public static final String FLUSH_MS_DOC = "This setting allows specifying a time interval at which we will " +
        "force an fsync of data written to the log. For example if this was set to 1000 " +
        "we would fsync after 1000 ms had passed. In general we recommend you not set " +
        "this and use replication for durability and allow the operating system's background " +
        "flush capabilities as it is more efficient.";

    public static final String RETENTION_BYTES = "retention.bytes";
    public static final String RETENTION_BYTES_DOC = "This configuration controls the maximum size a log can grow " +
        "to before we will discard old log segments to free up space if we are using the " +
        "\"delete\" retention policy. By default there is no size limit only a time limit.";

    public static final String RETENTION_MS = "retention.ms";
    public static final String RETENTION_MS_DOC = "This configuration controls the maximum time we will retain a " +
        "log before we will discard old log segments to free up space if we are using the " +
        "\"delete\" retention policy. This represents an SLA on how soon consumers must read " +
        "their data.";

    public static final String MAX_MESSAGE_BYTES = "max.message.bytes";
    public static final String MAX_MESSAGE_BYTES_DOC = "This is largest message size Kafka will allow to be " +
        "appended. Note that if you increase this size you must also increase your consumer's fetch size so " +
        "they can fetch messages this large.";

    public static final String INDEX_INTERVAL_BYTES = "index.interval.bytes";
    public static final String INDEX_INTERVAL_BYTES_DOCS = "This setting controls how frequently " +
        "Kafka adds an index entry to it's offset index. The default setting ensures that we index a " +
        "message roughly every 4096 bytes. More indexing allows reads to jump closer to the exact " +
        "position in the log but makes the index larger. You probably don't need to change this.";

    public static final String FILE_DELETE_DELAY_MS = "file.delete.delay.ms";
    public static final String FILE_DELETE_DELAY_MS_DOC = "The time to wait before deleting a file from the " +
        "filesystem";

    public static final String DELETE_RETENTION_MS = "delete.retention.ms";
    public static final String DELETE_RETENTION_MS_DOC = "The amount of time to retain delete tombstone markers " +
        "for <a href=\"#compaction\">log compacted</a> topics. This setting also gives a bound " +
        "on the time in which a consumer must complete a read if they begin from offset 0 " +
        "to ensure that they get a valid snapshot of the final stage (otherwise delete " +
        "tombstones may be collected before they complete their scan).";

    public static final String MIN_COMPACTION_LAG_MS = "min.compaction.lag.ms";
    public static final String MIN_COMPACTION_LAG_MS_DOC = "The minimum time a message will remain " +
        "uncompacted in the log. Only applicable for logs that are being compacted.";

    public static final String MIN_CLEANABLE_DIRTY_RATIO = "min.cleanable.dirty.ratio";
    public static final String MIN_CLEANABLE_DIRTY_RATIO_DOC = "This configuration controls how frequently " +
        "the log compactor will attempt to clean the log (assuming <a href=\"#compaction\">log " +
        "compaction</a> is enabled). By default we will avoid cleaning a log where more than " +
        "50% of the log has been compacted. This ratio bounds the maximum space wasted in " +
        "the log by duplicates (at 50% at most 50% of the log could be duplicates). A " +
        "higher ratio will mean fewer, more efficient cleanings but will mean more wasted " +
        "space in the log.";

    public static final String CLEANUP_POLICY = "cleanup.policy";
    public static final String CLEANUP_POLICY_COMPACT = "compact";
    public static final String CLEANUP_POLICY_DELETE = "delete";
    public static final String CLEANUP_POLICY_DOC = "A string that is either \"" + CLEANUP_POLICY_DELETE +
        "\" or \"" + CLEANUP_POLICY_COMPACT + "\". This string designates the retention policy to use on " +
        "old log segments. The default policy (\"delete\") will discard old segments when their retention " +
        "time or size limit has been reached. The \"compact\" setting will enable <a href=\"#compaction\">log " +
        "compaction</a> on the topic.";

    public static final String UNCLEAN_LEADER_ELECTION_ENABLE = "unclean.leader.election.enable";
    public static final String UNCLEAN_LEADER_ELECTION_ENABLE_DOC = "Indicates whether to enable replicas " +
        "not in the ISR set to be elected as leader as a last resort, even though doing so may result in data " +
        "loss.";

    public static final String MIN_IN_SYNC_REPLICAS = "min.insync.replicas";
    public static final String MIN_IN_SYNC_REPLICAS_DOC = "When a producer sets acks to \"all\" (or \"-1\"), " +
        "this configuration specifies the minimum number of replicas that must acknowledge " +
        "a write for the write to be considered successful. If this minimum cannot be met, " +
        "then the producer will raise an exception (either NotEnoughReplicas or " +
        "NotEnoughReplicasAfterAppend).<br>When used together, min.insync.replicas and acks " +
        "allow you to enforce greater durability guarantees. A typical scenario would be to " +
        "create a topic with a replication factor of 3, set min.insync.replicas to 2, and " +
        "produce with acks of \"all\". This will ensure that the producer raises an exception " +
        "if a majority of replicas do not receive a write.";

    public static final String COMPRESSION_TYPE = "compression.type";
    public static final String COMPRESSION_TYPE_DOC = "Specify the final compression type for a given topic. " +
        "This configuration accepts the standard compression codecs ('gzip', 'snappy', lz4). It additionally " +
        "accepts 'uncompressed' which is equivalent to no compression; and 'producer' which means retain the " +
        "original compression codec set by the producer.";

    public static final String PREALLOCATE = "preallocate";
    public static final String PREALLOCATE_DOC = "True if we should preallocate the file on disk when " +
        "creating a new log segment.";

    public static final String MESSAGE_FORMAT_VERSION = "message.format.version";
    public static final String MESSAGE_FORMAT_VERSION_DOC = "Specify the message format version the broker " +
        "will use to append messages to the logs. The value should be a valid ApiVersion. Some examples are: " +
        "0.8.2, 0.9.0.0, 0.10.0, check ApiVersion for more details. By setting a particular message format " +
        "version, the user is certifying that all the existing messages on disk are smaller or equal than the " +
        "specified version. Setting this value incorrectly will cause consumers with older versions to break as " +
        "they will receive messages with a format that they don't understand.";

    public static final String MESSAGE_TIMESTAMP_TYPE = "message.timestamp.type";
    public static final String MESSAGE_TIMESTAMP_TYPE_DOC = "Define whether the timestamp in the message is " +
        "message create time or log append time. The value should be either `CreateTime` or `LogAppendTime`";

    public static final String MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS = "message.timestamp.difference.max.ms";
    public static final String MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_DOC = "The maximum difference allowed between " +
        "the timestamp when a broker receives a message and the timestamp specified in the message. If " +
        "message.timestamp.type=CreateTime, a message will be rejected if the difference in timestamp " +
        "exceeds this threshold. This configuration is ignored if message.timestamp.type=LogAppendTime.";

    public static final String LEADER_REPLICATION_THROTTLED_REPLICAS = "leader.replication.throttled.replicas";
    public static final String LEADER_REPLICATION_THROTTLED_REPLICAS_DOC = "A list of replicas for which log " +
        "replication should be throttled on the leader side. The list should describe a set of " +
        "replicas in the form [PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively " +
        "the wildcard '*' can be used to throttle all replicas for this topic.";

    public static final String FOLLOWER_REPLICATION_THROTTLED_REPLICAS = "follower.replication.throttled.replicas";
    public static final String FOLLOWER_REPLICATION_THROTTLED_REPLICAS_DOC = "A list of replicas for which log " +
        "replication should be throttled on the follower side. The list should describe a set of " +
        "replicas in the form [PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively " +
        "the wildcard '*' can be used to throttle all replicas for this topic.";
}
