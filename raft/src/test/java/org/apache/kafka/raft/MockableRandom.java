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

import java.util.OptionalInt;
import java.util.Random;
import java.util.function.IntFunction;

/**
 * A Random instance that makes it easy to modify the behavior of certain methods for test purposes.
 */
class MockableRandom extends Random {

    private IntFunction<OptionalInt> nextIntFunction = __ -> OptionalInt.empty();

    public MockableRandom(long seed) {
        super(seed);
    }

    /**
     * If the function returns an empty option, `nextInt` behaves as usual. Otherwise, the integer
     * returned from `function` is returned by `nextInt`.
     */
    private void mockNextInt(IntFunction<OptionalInt> function) {
        this.nextIntFunction = function;
    }

    public void mockNextInt(int expectedBound, int returnValue) {
        this.nextIntFunction = b -> {
            if (b == expectedBound)
                return OptionalInt.of(returnValue);
            else
                return OptionalInt.empty();
        };
    }

    public void mockNextInt(int returnValue) {
        this.nextIntFunction = __ -> OptionalInt.of(returnValue);
    }

    @Override
    public int nextInt(int bound) {
        return nextIntFunction.apply(bound).orElse(super.nextInt(bound));
    }
}
