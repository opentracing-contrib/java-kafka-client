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

public class TopicSpanNameTest {

  private final ConsumerRecord<String, Integer> consumerRecord = new ConsumerRecord("example_topic", 0, 0, "KEY", 999);
  private final ProducerRecord<String, Integer> producerRecord = new ProducerRecord("example_topic", 0, System.currentTimeMillis(), "KEY", 999);
  private BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider;
  private BiFunction<String, ProducerRecord, String> producerSpanNameProvider;

  @Test
  public void topicSpanNameTest() {

    consumerSpanNameProvider = ClientSpanNameProvider.CONSUMER_TOPIC;
    producerSpanNameProvider = ClientSpanNameProvider.PRODUCER_TOPIC;

    assertEquals("example_topic", consumerSpanNameProvider.apply("receive", consumerRecord));
    assertEquals("example_topic", producerSpanNameProvider.apply("send", producerRecord));

    assertEquals("example_topic", consumerSpanNameProvider.apply(null, consumerRecord));
    assertEquals("example_topic", producerSpanNameProvider.apply(null, producerRecord));

    assertEquals("unknown", consumerSpanNameProvider.apply("receive", null));
    assertEquals("unknown", producerSpanNameProvider.apply("send", null));
  }

  @Test
  public void prefixedTopicSpanNameTest() {
    consumerSpanNameProvider = ClientSpanNameProvider.CONSUMER_PREFIXED_TOPIC("KafkaClient: ");
    producerSpanNameProvider = ClientSpanNameProvider.PRODUCER_PREFIXED_TOPIC("KafkaClient: ");

    assertEquals("KafkaClient: example_topic", consumerSpanNameProvider.apply("receive", consumerRecord));
    assertEquals("KafkaClient: example_topic", producerSpanNameProvider.apply("send", producerRecord));

    assertEquals("KafkaClient: example_topic", consumerSpanNameProvider.apply(null, consumerRecord));
    assertEquals("KafkaClient: example_topic", producerSpanNameProvider.apply(null, producerRecord));

    assertEquals("KafkaClient: unknown", consumerSpanNameProvider.apply("receive", null));
    assertEquals("KafkaClient: unknown", producerSpanNameProvider.apply("send", null));

    consumerSpanNameProvider = ClientSpanNameProvider.CONSUMER_PREFIXED_TOPIC(null);
    producerSpanNameProvider = ClientSpanNameProvider.PRODUCER_PREFIXED_TOPIC(null);

    assertEquals("example_topic", consumerSpanNameProvider.apply("receive", consumerRecord));
    assertEquals("example_topic", producerSpanNameProvider.apply("send", producerRecord));

    assertEquals("example_topic", consumerSpanNameProvider.apply(null, consumerRecord));
    assertEquals("example_topic", producerSpanNameProvider.apply(null, producerRecord));

    assertEquals("unknown", consumerSpanNameProvider.apply("receive", null));
    assertEquals("unknown", producerSpanNameProvider.apply("send", null));
  }
}
