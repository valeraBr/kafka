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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class ForwardingDisabledProcessorContextTest {
    @Mock(MockType.NICE)
    private ProcessorContext<Object, Object> delegate;
    private ForwardingDisabledProcessorContext context;

    @Before
    public void setUp() {
        context = new ForwardingDisabledProcessorContext(delegate);
    }

    @Test(expected = StreamsException.class)
    public void shouldThrowOnForward() {
        context.forward(null, null);
    }

    @Test(expected = StreamsException.class)
    public void shouldThrowOnForwardWithTo() {
        context.forward(null, null, To.all());
    }

    @SuppressWarnings("deprecation") // need to test deprecated code until removed
    @Test(expected = StreamsException.class)
    public void shouldThrowOnForwardWithChildIndex() {
        context.forward(null, null, 1);
    }

    @SuppressWarnings("deprecation") // need to test deprecated code until removed
    @Test(expected = StreamsException.class)
    public void shouldThrowOnForwardWithChildName() {
        context.forward(null, null, "child1");
    }
}