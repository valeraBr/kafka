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

package org.apache.kafka.streams.kstream;


import java.util.Collection;
import java.util.Collections;

public class SlidingWindows extends Windows<SlidingWindow> {

    private static final long DEFAULT_SIZE_MS = 1000L;

    private long size;

    private SlidingWindows(String name) {
        super(name);

        this.size = DEFAULT_SIZE_MS;
    }

    /**
     * Returns a half-interval sliding window definition with the default window size
     */
    public static SlidingWindows of(String name) {
        return new SlidingWindows(name);
    }

    /**
     * Returns a half-interval sliding window definition with the window size in milliseconds
     */
    public SlidingWindows with(long size) {
        this.size = size;

        return this;
    }

    @Override
    public Collection<Window> windowsFor(long timestamp) {
        // TODO
        return Collections.<Window>emptyList();
    }

    @Override
    public boolean equalTo(Windows other) {
        if (!other.getClass().equals(SlidingWindows.class))
            return false;

        SlidingWindows otherWindows = (SlidingWindows) other;

        return this.size == otherWindows.size;
    }
}
