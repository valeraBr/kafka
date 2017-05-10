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
package org.apache.kafka.streams.kstream;


import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TimestampExtractor;

public interface ValueTransformerRichFunction<VR> {

    /**
     * Initialize this transformer.
     * This is called once per instance when the topology gets initialized.
     * <p>
     * The provided {@link ProcessorContext context} can be used to access topology and record meta data, to
     * {@link ProcessorContext#schedule(long) schedule itself} for periodical calls (cf. {@link #punctuate(long)}), and
     * to access attached {@link StateStore}s.
     * <p>
     * Note that {@link ProcessorContext} is updated in the background with the current record's meta data.
     * Thus, it only contains valid record meta data when accessed within {@link ValueTransformer transform(Object)}
     * or {@link ValueTransformerWithKey transform(Object Object)}.
     * <p>
     * Note that using {@link ProcessorContext#forward(Object, Object)},
     * {@link ProcessorContext#forward(Object, Object, int)}, or
     * {@link ProcessorContext#forward(Object, Object, String)} is not allowed within any method of
     * {@code ValueTransformer} and will result in an {@link StreamsException exception}.
     *
     * @param context the context
     */
    void init(final ProcessorContext context);

     /**
      * Perform any periodic operations if this processor {@link ProcessorContext#schedule(long) schedule itself} with
      * the context during {@link #init(ProcessorContext) initialization}.
      * <p>
      * It is not possible to return any new output records within {@code punctuate}.
      * Using {@link ProcessorContext#forward(Object, Object)}, {@link ProcessorContext#forward(Object, Object, int)},
      * or {@link ProcessorContext#forward(Object, Object, String)} will result in an
      * {@link StreamsException exception}.
      * Furthermore, {@code punctuate} must return {@code null}.
      * <p>
      * Note, that {@code punctuate} is called base on <it>stream time</it> (i.e., time progress with regard to
      * timestamps return by the used {@link TimestampExtractor})
      * and not based on wall-clock time.
      *
      * @param timestamp the stream time when {@code punctuate} is being called
      * @return must return {@code null}&mdash;otherwise, an {@link StreamsException exception} will be thrown
      */
    VR punctuate(final long timestamp);

    /**
     * Close this processor and clean up any resources.
     * <p>
     * It is not possible to return any new output records within {@code close()}.
     * Using {@link ProcessorContext#forward(Object, Object)}, {@link ProcessorContext#forward(Object, Object, int)},
     * or {@link ProcessorContext#forward(Object, Object, String)} will result in an {@link StreamsException exception}.
     */
    void close();
}
