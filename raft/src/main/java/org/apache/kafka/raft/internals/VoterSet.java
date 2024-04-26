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
package org.apache.kafka.raft.internals;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.feature.SupportedVersionRange;

/**
 * A type for representing the set of voters for a topic partition.
 *
 * It encapsulates static information like a voter's endpoint and their supported kraft.version.
 *
 * It providees functionality for converting to and from {@code VotersRecord} and for converting
 * from the static configuration.
 */
final public class VoterSet {
    private final Map<Integer, VoterNode> voters;

    VoterSet(Map<Integer, VoterNode> voters) {
        if (voters.isEmpty()) {
            throw new IllegalArgumentException("Voters cannot be empty");
        }

        this.voters = voters;
    }

    /**
     * Returns the socket address for a given voter at a given listener.
     *
     * @param voter the id of the voter
     * @param listener the name of the listener
     * @return the socket address if it exist, otherwise {@code Optional.empty()}
     */
    public Optional<InetSocketAddress> voterAddress(int voter, String listener) {
        return Optional.ofNullable(voters.get(voter))
            .flatMap(voterNode -> voterNode.address(listener));
    }

    /**
     * Returns all of the voter ids.
     */
    public Set<Integer> voterIds() {
        return voters.keySet();
    }

    /**
     * Adds a voter to the voter set.
     *
     * This object is immutable. A new voter set is returned if the voter was added.
     *
     * A new voter can be added to a voter set if its id doesn't already exist in the voter set.
     *
     * @param voter the new voter to add
     * @return a new voter set if the voter was added, otherwise {@code Optional.empty()}
     */
    public Optional<VoterSet> addVoter(VoterNode voter) {
        if (voters.containsKey(voter.id())) {
            return Optional.empty();
        }

        HashMap<Integer, VoterNode> newVoters = new HashMap<>(voters);
        newVoters.put(voter.id(), voter);

        return Optional.of(new VoterSet(newVoters));
    }

    /**
     * Removew a voter from the voter set.
     *
     * This object is immutable. A new voter set is returned if the voter was removed.
     *
     * A voter can be removed from the voter set if its id and directory id match.
     *
     * @param voterId the voter id
     * @param voterDirectoryId the voter directory id
     * @return a new voter set if the voter was remove, otherwise {@code Optional.empty()}
     */
    public Optional<VoterSet> removeVoter(int voterId, Optional<Uuid> voterDirectoryId) {
        VoterNode oldVoter = voters.get(voterId);
        if (oldVoter != null && Objects.equals(oldVoter.directoryId(), voterDirectoryId)) {
            HashMap<Integer, VoterNode> newVoters = new HashMap<>(voters);
            newVoters.remove(voterId);

            return Optional.of(new VoterSet(newVoters));
        }

        return Optional.empty();
    }

    /**
     * Converts a voter set to a voters record for a given version.
     *
     * @param version the version of the voters record
     */
    public VotersRecord toVotersRecord(short version) {
        return new VotersRecord()
            .setVersion(version)
            .setVoters(
                voters
                    .values()
                    .stream()
                    .map(voter -> {
                        Iterator<VotersRecord.Endpoint> endpoints = voter
                            .listeners()
                            .entrySet()
                            .stream()
                            .map(entry ->
                                new VotersRecord.Endpoint()
                                    .setName(entry.getKey())
                                    .setHost(entry.getValue().getHostString())
                                    .setPort(entry.getValue().getPort())
                            )
                            .iterator();

                        VotersRecord.KRaftVersionFeature kraftVersionFeature = new VotersRecord.KRaftVersionFeature()
                            .setMinSupportedVersion(voter.supportedKRaftVersion().min())
                            .setMaxSupportedVersion(voter.supportedKRaftVersion().max());

                        return new VotersRecord.Voter()
                            .setVoterId(voter.id())
                            .setVoterDirectoryId(voter.directoryId().orElse(Uuid.ZERO_UUID))
                            .setEndpoints(new VotersRecord.EndpointCollection(endpoints))
                            .setKRaftVersionFeature(kraftVersionFeature);
                    })
                    .collect(Collectors.toList())
            );
    }

    /**
     * Determines if two sets of voters have an overlapping majority.
     *
     * A overlapping majority means that for all majorities in {@code this} set of voters and for
     * all majority in {@code that} voeter set they have at least one voter in common.
     *
     * This can be used to validate a change in the set of voters will get committed by both sets
     * of voters.
     *
     * @param that the other voter set to compare
     * @return true if they have an overlapping majority, false otherwise
     */
    public boolean hasOverlappingMajority(VoterSet that) {
        if (Utils.diff(HashSet::new, voters.keySet(), that.voters.keySet()).size() > 2) return false;
        if (Utils.diff(HashSet::new, that.voters.keySet(), voters.keySet()).size() > 2) return false;

        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VoterSet that = (VoterSet) o;

        return voters.equals(that.voters);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(voters);
    }

    @Override
    public String toString() {
        return String.format("VoterSet(voters=%s)", voters);
    }

    final static class VoterNode {
        private final int id;
        private final Optional<Uuid> directoryId;
        private final Map<String, InetSocketAddress> listeners;
        private final SupportedVersionRange supportedKRaftVersion;

        VoterNode(
            int id,
            Optional<Uuid> directoryId,
            Map<String, InetSocketAddress> listeners,
            SupportedVersionRange supportedKRaftVersion
        ) {
            this.id = id;
            this.directoryId = directoryId;
            this.listeners = listeners;
            this.supportedKRaftVersion = supportedKRaftVersion;
        }

        int id() {
            return id;
        }

        Optional<Uuid> directoryId() {
            return directoryId;
        }

        Map<String, InetSocketAddress> listeners() {
            return listeners;
        }

        SupportedVersionRange supportedKRaftVersion() {
            return supportedKRaftVersion;
        }


        Optional<InetSocketAddress> address(String listener) {
            return Optional.ofNullable(listeners.get(listener));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            VoterNode that = (VoterNode) o;

            if (id != that.id) return false;
            if (!Objects.equals(directoryId, that.directoryId)) return false;
            if (!Objects.equals(supportedKRaftVersion, that.supportedKRaftVersion)) return false;
            if (!Objects.equals(listeners, that.listeners)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, directoryId, listeners, supportedKRaftVersion);
        }

        @Override
        public String toString() {
            return String.format(
                "VoterNode(id=%d, directoryId=%s, listeners=%s, supportedKRaftVersion=%s)",
                id,
                directoryId,
                listeners,
                supportedKRaftVersion
            );
        }
    }

    /**
     * Converts a {@code VotersRecord} to a {@code VoterSet}.
     *
     * @param voters the set of voters control record
     * @return the voter set
     */
    public static VoterSet fromVotersRecord(VotersRecord voters) {
        HashMap<Integer, VoterNode> voterNodes = new HashMap<>(voters.voters().size());
        for (VotersRecord.Voter voter: voters.voters()) {
            final Optional<Uuid> directoryId;
            if (!voter.voterDirectoryId().equals(Uuid.ZERO_UUID)) {
                directoryId = Optional.of(voter.voterDirectoryId());
            } else {
                directoryId = Optional.empty();
            }

            Map<String, InetSocketAddress> listeners = new HashMap<>(voter.endpoints().size());
            for (VotersRecord.Endpoint endpoint : voter.endpoints()) {
                listeners.put(endpoint.name(), InetSocketAddress.createUnresolved(endpoint.host(), endpoint.port()));
            }

            voterNodes.put(
                voter.voterId(),
                new VoterNode(
                    voter.voterId(),
                    directoryId,
                    listeners,
                    new SupportedVersionRange(
                        voter.kRaftVersionFeature().minSupportedVersion(),
                        voter.kRaftVersionFeature().maxSupportedVersion()
                    )
                )
            );
        }

        return new VoterSet(voterNodes);
    }

    /**
     * Creates a voter set from a map of socket addresses.
     *
     * @param listener the listener name for all of the endpoints
     * @param voters the socket addresses by voter id
     * @return the voter set
     */
    public static VoterSet fromAddressSpecs(String listener, Map<Integer, InetSocketAddress> voters) {
        Map<Integer, VoterNode> voterNodes = voters
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> new VoterNode(
                        entry.getKey(),
                        Optional.empty(),
                        Collections.singletonMap(listener, entry.getValue()),
                        new SupportedVersionRange((short) 0, (short) 0)
                    )
                )
            );
        return new VoterSet(voterNodes);
    }
}
