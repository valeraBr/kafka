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
package org.apache.kafka.streams.processor;

/**
 * An abstract implementation of {@link Processor} that manages the {@link ProcessorContext} instance and provides default no-op
 * implementation of {@link #close()}.
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public abstract class AbstractProcessor<KIn, VIn> implements Processor<KIn, VIn> {

    private ProcessorContext context;

    protected AbstractProcessor() {
    }

    @Override
    public void init(final ProcessorContext context) {
        this.context = context;
    }

    /**
     * Get the processor's context set during {@link #init(ProcessorContext) initialization}.
     *
     * @return the processor context; null only when called prior to {@link #init(ProcessorContext) initialization}.
     */
    protected final ProcessorContext context() {
        return context;
    }
}
