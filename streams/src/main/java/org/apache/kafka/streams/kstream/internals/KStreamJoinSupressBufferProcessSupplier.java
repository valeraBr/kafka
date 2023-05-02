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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBuffer;

import java.time.Duration;

public class KStreamJoinSupressBufferProcessSupplier<K, V> implements ProcessorSupplier<K, V, K, V> {
  private final TimeOrderedKeyValueBuffer<K, V> buffer;
  private final Duration gracePeriod;

  public KStreamJoinSupressBufferProcessSupplier(final TimeOrderedKeyValueBuffer<K, V> buffer, final Duration gracePeriod) {
    this.buffer = buffer;
    this.gracePeriod = gracePeriod;
  }

  /**
   * Return a newly constructed {@link Processor} instance.
   * The supplier should always generate a new instance each time {@link  ProcessorSupplier#get()} gets called.
   * <p>x
   * Creating a single {@link Processor} object and returning the same object reference in {@link ProcessorSupplier#get()}
   * is a violation of the supplier pattern and leads to runtime exceptions.
   *
   * @return a new {@link Processor} instance
   */
  @Override
  public Processor<K, V, K, V> get() {
    return new KStreamJoinBufferProcessor<>(buffer, gracePeriod);
  }
}
