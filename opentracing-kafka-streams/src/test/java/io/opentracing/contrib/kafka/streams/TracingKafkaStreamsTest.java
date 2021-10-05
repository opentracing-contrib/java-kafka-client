/*
 * Copyright 2017-2020 The OpenTracing Authors
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
package io.opentracing.contrib.kafka.streams;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.opentracing.contrib.kafka.TracingKafkaProducer;
import io.opentracing.contrib.kafka.TracingKafkaUtils;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TracingKafkaStreamsTest {

  private static EmbeddedKafkaCluster cluster;
  private MockTracer mockTracer = new MockTracer();

  @BeforeClass
  public static void init() throws InterruptedException, IOException {
    cluster = new EmbeddedKafkaCluster(2);
    cluster.start();
    cluster.createTopic("stream-test", 2, 2);
  }

  @AfterClass
  public static void shutdown() {
    cluster.stop();
  }

  @Before
  public void before() {
    mockTracer.reset();
  }

  @Test
  public void test() {
    Map<String, Object> senderProps = producerProps();

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-app");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, senderProps.get("bootstrap.servers"));
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    Producer<Integer, String> producer = createProducer();
    ProducerRecord<Integer, String> record = new ProducerRecord<>("stream-test", 1, "test");
    producer.send(record);

    final Serde<String> stringSerde = Serdes.String();
    final Serde<Integer> intSerde = Serdes.Integer();

    StreamsBuilder builder = new StreamsBuilder();
    KStream<Integer, String> kStream = builder.stream("stream-test");

    kStream.map((key, value) -> new KeyValue<>(key, value + "map"))
        .to("stream-out", Produced.with(intSerde, stringSerde));

    KafkaStreams streams = new KafkaStreams(builder.build(), config,
        new TracingKafkaClientSupplier(mockTracer));
    streams.start();

    await().atMost(15, TimeUnit.SECONDS).until(reportedSpansSize(), equalTo(3));

    streams.close();
    producer.close();

    List<MockSpan> spans = mockTracer.finishedSpans();
    assertEquals(3, spans.size());
    checkSpans(spans);

    assertNull(mockTracer.activeSpan());
  }

  private Producer<Integer, String> createProducer() {
    Map<String, Object> senderProps = producerProps();
    KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(senderProps);
    return new TracingKafkaProducer<>(kafkaProducer, mockTracer);
  }

  private void checkSpans(List<MockSpan> mockSpans) {
    for (MockSpan mockSpan : mockSpans) {
      String operationName = mockSpan.operationName();
      if (operationName.equals(TracingKafkaUtils.TO_PREFIX + "stream-test")) {
        assertEquals(Tags.SPAN_KIND_PRODUCER, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
        String topicName = (String) mockSpan.tags().get(Tags.MESSAGE_BUS_DESTINATION.getKey());
        assertTrue(topicName.equals("stream-out") || topicName.equals("stream-test"));
      } else if (operationName.equals(TracingKafkaUtils.FROM_PREFIX + "stream-test")) {
        assertEquals(Tags.SPAN_KIND_CONSUMER, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
        assertEquals(0, mockSpan.tags().get("partition"));
        long offset = (Long) mockSpan.tags().get("offset");
        assertTrue(offset == 0L || offset == 1L || offset == 2L);
        String topicName = (String) mockSpan.tags().get(Tags.MESSAGE_BUS_DESTINATION.getKey());
        assertTrue(topicName.equals("stream-out") || topicName.equals("stream-test"));
      }
      assertEquals("java-kafka", mockSpan.tags().get(Tags.COMPONENT.getKey()));
      assertEquals(0, mockSpan.generatedErrors().size());
      assertTrue(operationName.equals(TracingKafkaUtils.TO_PREFIX + "stream-test")
          || operationName.equals(TracingKafkaUtils.FROM_PREFIX + "stream-test")
          || operationName.equals(TracingKafkaUtils.FROM_PREFIX + "stream-out")
          || operationName.equals(TracingKafkaUtils.TO_PREFIX + "stream-out"));
    }
  }

  private Callable<Integer> reportedSpansSize() {
    return () -> mockTracer.finishedSpans().size();
  }

  private Map<String, Object> producerProps() {
    final Map<String, Object> producerProps = new HashMap<>();

    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
    producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    return producerProps;
  }

}
