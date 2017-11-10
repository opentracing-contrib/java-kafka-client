/*
 * Copyright 2017 The OpenTracing Authors
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
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.ThreadLocalActiveSpanSource;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

public class TracingKafkaStreamsTest {

  @ClassRule
  public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(2, true, 2, "stream-test");

  private MockTracer mockTracer = new MockTracer(new ThreadLocalActiveSpanSource(),
      MockTracer.Propagator.TEXT_MAP);

  @Before
  public void before() throws Exception {
    mockTracer.reset();
  }

  @Test
  public void test() throws Exception {
    Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);

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

    KStreamBuilder builder = new KStreamBuilder();
    KStream<Integer, String> kStream = builder
        .stream(intSerde, stringSerde, "stream-test");

    kStream.map((key, value) -> new KeyValue<>(key, value + "map")).to("stream-out");

    KafkaStreams streams = new KafkaStreams(builder, new StreamsConfig(config),
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
    Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
    KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(senderProps);
    return new TracingKafkaProducer<>(kafkaProducer, mockTracer);
  }

  private void checkSpans(List<MockSpan> mockSpans) {
    for (MockSpan mockSpan : mockSpans) {
      assertEquals(Tags.SPAN_KIND_CLIENT, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
      assertEquals("java-kafka", mockSpan.tags().get(Tags.COMPONENT.getKey()));
      assertEquals(0, mockSpan.generatedErrors().size());
      String operationName = mockSpan.operationName();
      assertTrue(operationName.equals("send")
          || operationName.equals("receive"));
    }
  }

  private Callable<Integer> reportedSpansSize() {
    return () -> mockTracer.finishedSpans().size();
  }
}
