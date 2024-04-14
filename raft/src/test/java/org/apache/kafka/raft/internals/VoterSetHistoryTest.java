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

import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

final public class VoterSetHistoryTest {
    @Test
    void testStaicVoterSet() {
        VoterSet staticVoterSet = new VoterSet(VoterSetTest.voterMap(Arrays.asList(1, 2, 3)));
        VoterSetHistory votersHistory = new VoterSetHistory(Optional.of(staticVoterSet));

        validateStaticVoterSet(staticVoterSet, votersHistory);

        // Should be a no-op
        votersHistory.truncateTo(100);
        validateStaticVoterSet(staticVoterSet, votersHistory);

        // Should be a no-op
        votersHistory.trimPrefixTo(100);
        validateStaticVoterSet(staticVoterSet, votersHistory);
    }

    private void validateStaticVoterSet(VoterSet expected, VoterSetHistory votersHistory) {
        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(0));
        assertEquals(Optional.empty(), votersHistory.valueAtOrBefore(100));
        assertEquals(expected, votersHistory.lastValue());
    }
}
