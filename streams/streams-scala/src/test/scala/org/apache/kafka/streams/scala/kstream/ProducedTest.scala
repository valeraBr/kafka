/*
 * Copyright (C) 2018 Joan Goyeau.
 *
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
package org.apache.kafka.streams.scala.kstream

import org.apache.kafka.streams.kstream.internals.ProducedInternal
import org.apache.kafka.streams.processor.StreamPartitioner
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.scala.Serdes._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class ProducedTest extends FlatSpec with Matchers {

  "Create a Produced" should "create a Produced with Serdes" in {
    val produced: Produced[String, Long] = Produced.`with`[String, Long]

    val internalProduced = produced.asInstanceOf[ProducedInternal[String, Long]]
    internalProduced.keySerde.getClass shouldBe Serdes.String.getClass
    internalProduced.valueSerde.getClass shouldBe Serdes.Long.getClass
  }

  "Create a Produced with timestampExtractor and resetPolicy" should "create a Consumed with Serdes, timestampExtractor and resetPolicy" in {
    val partitioner = new StreamPartitioner[String, Long] {
      override def partition(topic: String, key: String, value: Long, numPartitions: Int): Integer = 0
    }
    val produced: Produced[String, Long] = Produced.`with`(partitioner)

    val internalProduced = produced.asInstanceOf[ProducedInternal[String, Long]]
    internalProduced.keySerde.getClass shouldBe Serdes.String.getClass
    internalProduced.valueSerde.getClass shouldBe Serdes.Long.getClass
    internalProduced.streamPartitioner shouldBe partitioner
  }
}
