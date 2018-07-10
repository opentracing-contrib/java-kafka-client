/*
 * Copyright 2017-2018 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.opentracing.contrib.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;

public class OperationNameSpanNameTest {
  private final ConsumerRecord<String, Integer> consumerRecord = new ConsumerRecord("example_topic", 0, 0, "KEY", 999);
  private final ProducerRecord<String, Integer> producerRecord = new ProducerRecord("example_topic", 0, System.currentTimeMillis(), "KEY", 999);
  private BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider;
  private BiFunction<String, ProducerRecord, String> producerSpanNameProvider;

  @Test
  public void operationNameSpanNameTest() {
    consumerSpanNameProvider = ClientSpanNameProvider.CONSUMER_OPERATION_NAME;
    producerSpanNameProvider = ClientSpanNameProvider.PRODUCER_OPERATION_NAME;

    assertEquals("receive", consumerSpanNameProvider.apply("receive", consumerRecord));
    assertEquals("send", producerSpanNameProvider.apply("send", producerRecord));

    assertEquals("unknown", consumerSpanNameProvider.apply(null, consumerRecord));
    assertEquals("unknown", producerSpanNameProvider.apply(null, producerRecord));

    assertEquals("receive", consumerSpanNameProvider.apply("receive", null));
    assertEquals("send", producerSpanNameProvider.apply("send", null));

    assertEquals("unknown", consumerSpanNameProvider.apply(null, null));
    assertEquals("unknown", producerSpanNameProvider.apply(null, null));
  }

  @Test
  public void prefixedOperationNameSpanNameTest() {
    consumerSpanNameProvider = ClientSpanNameProvider.CONSUMER_PREFIXED_OPERATION_NAME("KafkaClient: ");
    producerSpanNameProvider = ClientSpanNameProvider.PRODUCER_PREFIXED_OPERATION_NAME("KafkaClient: ");

    assertEquals("KafkaClient: receive", consumerSpanNameProvider.apply("receive", consumerRecord));
    assertEquals("KafkaClient: send", producerSpanNameProvider.apply("send", producerRecord));

    assertEquals("KafkaClient: unknown", consumerSpanNameProvider.apply(null, consumerRecord));
    assertEquals("KafkaClient: unknown", producerSpanNameProvider.apply(null, producerRecord));

    assertEquals("KafkaClient: receive", consumerSpanNameProvider.apply("receive", null));
    assertEquals("KafkaClient: send", producerSpanNameProvider.apply("send", null));

    assertEquals("KafkaClient: unknown", consumerSpanNameProvider.apply(null, null));
    assertEquals("KafkaClient: unknown", producerSpanNameProvider.apply(null, null));

    consumerSpanNameProvider = ClientSpanNameProvider.CONSUMER_PREFIXED_OPERATION_NAME(null);
    producerSpanNameProvider = ClientSpanNameProvider.PRODUCER_PREFIXED_OPERATION_NAME(null);

    assertEquals("receive", consumerSpanNameProvider.apply("receive", consumerRecord));
    assertEquals("send", producerSpanNameProvider.apply("send", producerRecord));

    assertEquals("unknown", consumerSpanNameProvider.apply(null, consumerRecord));
    assertEquals("unknown", producerSpanNameProvider.apply(null, producerRecord));

    assertEquals("receive", consumerSpanNameProvider.apply("receive", null));
    assertEquals("send", producerSpanNameProvider.apply("send", null));

    assertEquals("unknown", consumerSpanNameProvider.apply(null, null));
    assertEquals("unknown", producerSpanNameProvider.apply(null, null));
  }
}
