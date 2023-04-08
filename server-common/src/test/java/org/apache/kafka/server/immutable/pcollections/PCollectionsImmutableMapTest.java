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

package org.apache.kafka.server.immutable.pcollections;

import org.apache.kafka.server.immutable.DelegationChecker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pcollections.HashPMap;
import org.pcollections.HashTreePMap;

import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.function.Function.identity;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

@SuppressWarnings({"unchecked", "deprecation"})
public class PCollectionsImmutableMapTest {
    private static final HashPMap<Object, Object> SINGLETON_MAP = HashTreePMap.singleton(new Object(), new Object());

    private static final class PCollectionsHashMapWrapperDelegationChecker<R> extends DelegationChecker<HashPMap<Object, Object>, PCollectionsImmutableMap<Object, Object>, R> {
        public PCollectionsHashMapWrapperDelegationChecker() {
            super(mock(HashPMap.class), PCollectionsImmutableMap::new);
        }

        public HashPMap<Object, Object> unwrap(PCollectionsImmutableMap<Object, Object> wrapper) {
            return wrapper.underlying();
        }
    }

    @Test
    public void testUnderlying() {
        assertSame(SINGLETON_MAP, new PCollectionsImmutableMap<>(SINGLETON_MAP).underlying());
    }

    @Test
    public void testDelegationOfAfterAdding() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.plus(eq(this), eq(this)), SINGLETON_MAP)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.updated(this, this), identity())
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfAfterRemoving() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.minus(eq(this)), SINGLETON_MAP)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.removed(this), identity())
            .expectWrapperToWrapMockFunctionReturnValue()
            .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void testDelegationOfSize(int mockFunctionReturnValue) {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(HashPMap::size, mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableMap::size, identity())
            .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfIsEmpty(boolean mockFunctionReturnValue) {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(HashPMap::isEmpty, mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableMap::isEmpty, identity())
            .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfContainsKey(boolean mockFunctionReturnValue) {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.containsKey(eq(this)), mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.containsKey(this), identity())
            .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfContainsValue(boolean mockFunctionReturnValue) {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.containsValue(eq(this)), mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.containsValue(this), identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfGet() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.get(eq(this)), new Object())
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.get(this), identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfPut() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.put(eq(this), eq(this)), this)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.put(this, this), identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfRemoveByKey() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.remove(eq(this)), this)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.remove(this), identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfPutAll() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForVoidMethodInvocation(mock -> mock.putAll(eq(Collections.emptyMap())))
            .defineWrapperVoidMethodInvocation(wrapper -> wrapper.putAll(Collections.emptyMap()))
            .doVoidMethodDelegationCheck();
    }

    @Test
    public void testDelegationOfClear() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForVoidMethodInvocation(HashPMap::clear)
            .defineWrapperVoidMethodInvocation(PCollectionsImmutableMap::clear)
            .doVoidMethodDelegationCheck();
    }


    @Test
    public void testDelegationOfKeySet() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(HashPMap::keySet, Collections.emptySet())
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableMap::keySet, identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfValues() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(HashPMap::values, Collections.emptySet())
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableMap::values, identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfEntrySet() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(HashPMap::entrySet, Collections.emptySet())
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableMap::entrySet, identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testEquals() {
        final HashPMap<Object, Object> mock = mock(HashPMap.class);
        assertEquals(new PCollectionsImmutableMap<>(mock), new PCollectionsImmutableMap<>(mock));
        final HashPMap<Object, Object> someOtherMock = mock(HashPMap.class);
        assertNotEquals(new PCollectionsImmutableMap<>(mock), new PCollectionsImmutableMap<>(someOtherMock));
    }

    @Test
    public void testHashCode() {
        final HashPMap<Object, Object> mock = mock(HashPMap.class);
        assertEquals(mock.hashCode(), new PCollectionsImmutableMap<>(mock).hashCode());
        final HashPMap<Object, Object> someOtherMock = mock(HashPMap.class);
        assertNotEquals(mock.hashCode(), new PCollectionsImmutableMap<>(someOtherMock).hashCode());
    }

    @Test
    public void testDelegationOfGetOrDefault() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.getOrDefault(eq(this), eq(this)), this)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.getOrDefault(this, this), identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfForEach() {
        final BiConsumer<Object, Object> mockBiConsumer = mock(BiConsumer.class);
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForVoidMethodInvocation(mock -> mock.forEach(eq(mockBiConsumer)))
            .defineWrapperVoidMethodInvocation(wrapper -> wrapper.forEach(mockBiConsumer))
            .doVoidMethodDelegationCheck();
    }

    @Test
    public void testDelegationOfReplaceAll() {
        final BiFunction<Object, Object, Object> mockBiFunction = mock(BiFunction.class);
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForVoidMethodInvocation(mock -> mock.replaceAll(eq(mockBiFunction)))
            .defineWrapperVoidMethodInvocation(wrapper -> wrapper.replaceAll(mockBiFunction))
            .doVoidMethodDelegationCheck();
    }

    @Test
    public void testDelegationOfPutIfAbsent() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.putIfAbsent(eq(this), eq(this)), this)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.putIfAbsent(this, this), identity())
            .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfRemoveByKeyAndValue(boolean mockFunctionReturnValue) {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.remove(eq(this), eq(this)), mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.remove(this, this), identity())
            .doFunctionDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDelegationOfReplaceWhenMappedToSpecificValue(boolean mockFunctionReturnValue) {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.replace(eq(this), eq(this), eq(this)), mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.replace(this, this, this), identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfReplaceWhenMappedToAnyValue() {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(mock -> mock.replace(eq(this), eq(this)), this)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(wrapper -> wrapper.replace(this, this), identity())
            .doFunctionDelegationCheck();
    }

    @Test
    public void testDelegationOfComputeIfAbsent() {
        final Function<Object, Object> mockFunction = mock(Function.class);
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForVoidMethodInvocation(mock -> mock.computeIfAbsent(eq(this), eq(mockFunction)))
            .defineWrapperVoidMethodInvocation(wrapper -> wrapper.computeIfAbsent(this, mockFunction))
            .doVoidMethodDelegationCheck();
    }

    @Test
    public void testDelegationOfComputeIfPresent() {
        final BiFunction<Object, Object, Object> mockBiFunction = mock(BiFunction.class);
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForVoidMethodInvocation(mock -> mock.computeIfPresent(eq(this), eq(mockBiFunction)))
            .defineWrapperVoidMethodInvocation(wrapper -> wrapper.computeIfPresent(this, mockBiFunction))
            .doVoidMethodDelegationCheck();
    }

    @Test
    public void testDelegationOfCompute() {
        final BiFunction<Object, Object, Object> mockBiFunction = mock(BiFunction.class);
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForVoidMethodInvocation(mock -> mock.compute(eq(this), eq(mockBiFunction)))
            .defineWrapperVoidMethodInvocation(wrapper -> wrapper.compute(this, mockBiFunction))
            .doVoidMethodDelegationCheck();
    }

    @Test
    public void testDelegationOfMerge() {
        final BiFunction<Object, Object, Object> mockBiFunction = mock(BiFunction.class);
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForVoidMethodInvocation(mock -> mock.merge(eq(this), eq(this), eq(mockBiFunction)))
            .defineWrapperVoidMethodInvocation(wrapper -> wrapper.merge(this, this, mockBiFunction))
            .doVoidMethodDelegationCheck();
    }

    @ParameterizedTest
    @ValueSource(strings = {"a", "b"})
    public void testDelegationOfToString(String mockFunctionReturnValue) {
        new PCollectionsHashMapWrapperDelegationChecker<>()
            .defineMockConfigurationForFunctionInvocation(HashPMap::toString, mockFunctionReturnValue)
            .defineWrapperFunctionInvocationAndMockReturnValueTransformation(PCollectionsImmutableMap::toString,
                text -> "PCollectionsImmutableMap{underlying=" + text + "}")
            .doFunctionDelegationCheck();
    }
}
