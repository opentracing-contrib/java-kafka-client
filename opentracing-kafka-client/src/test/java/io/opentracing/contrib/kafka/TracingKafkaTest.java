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
package io.opentracing.contrib.kafka;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;

public class TracingKafkaTest {

  @ClassRule
  public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(2, true, 2, "messages");
  private static final MockTracer mockTracer = new MockTracer();

  @BeforeClass
  public static void init() {
    GlobalTracer.registerIfAbsent(mockTracer);
  }

  @Before
  public void before() {
    mockTracer.reset();
  }

  @Test
  public void with_interceptors() throws Exception {
    Map<String, Object> senderProps = KafkaTestUtils
        .producerProps(embeddedKafka.getEmbeddedKafka());
    senderProps
        .put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, TracingProducerInterceptor.class.getName());
    KafkaProducer<Integer, String> producer = new KafkaProducer<>(senderProps);

    producer.send(new ProducerRecord<>("messages", 1, "test"));

    final CountDownLatch latch = new CountDownLatch(1);
    createConsumer(latch, 1, true, null);

    producer.close();

    List<MockSpan> mockSpans = mockTracer.finishedSpans();
    assertEquals(2, mockSpans.size());
    checkSpans(mockSpans);
    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void test() throws Exception {
    Producer<Integer, String> producer = createTracingProducer();

    // Send 1
    producer.send(new ProducerRecord<>("messages", 1, "test"));

    // Send 2
    producer.send(new ProducerRecord<>("messages", 1, "test"),
        (metadata, exception) -> assertEquals("messages", metadata.topic()));

    final CountDownLatch latch = new CountDownLatch(2);
    createConsumer(latch, 1, false, null);

    producer.close();

    List<MockSpan> mockSpans = mockTracer.finishedSpans();
    assertEquals(4, mockSpans.size());
    checkSpans(mockSpans);
    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void testWithParentContext() throws Exception {
    TracingKafkaProducer<Integer, String> producer = createTracingProducer();

    final MockSpan parent = mockTracer.buildSpan("parent").start();

    // Send 1
    producer.send(new ProducerRecord<>("messages", 1, "test"), parent.context());

    // Send 2
    producer.send(new ProducerRecord<>("messages", 1, "test"),
        (metadata, exception) -> assertEquals("messages", metadata.topic()), parent.context());

    final CountDownLatch latch = new CountDownLatch(2);
    createConsumer(latch, 1, false, null);

    producer.close();

    List<MockSpan> mockSpans = mockTracer.finishedSpans();
    assertEquals(4, mockSpans.size());
    checkSpans(mockSpans);
    for (MockSpan span : mockSpans) {
      assertEquals(parent.context().traceId(), span.context().traceId());
    }

    final List<MockSpan> sendSpans = getByOperationNameAll(mockSpans,
        TracingKafkaUtils.TO_PREFIX + "messages");
    assertEquals(2, sendSpans.size());
    for (MockSpan sendSpan : sendSpans) {
      assertEquals(parent.context().spanId(), sendSpan.parentId());
    }

    parent.finish();

    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void testNotTracedProducer() throws Exception {
    Producer<Integer, String> producer = createProducer();

    // Send 1
    producer.send(new ProducerRecord<>("messages", 1, "test"));

    // Send 2
    producer.send(new ProducerRecord<>("messages", 1, "test"),
        (metadata, exception) -> assertEquals("messages", metadata.topic()));

    final CountDownLatch latch = new CountDownLatch(2);
    createConsumer(latch, 1, false, null);

    producer.close();

    List<MockSpan> mockSpans = mockTracer.finishedSpans();
    assertEquals(2, mockSpans.size());
    checkSpans(mockSpans);
    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void testWithTopicNameProvider() throws Exception {
    Producer<Integer, String> producer = createNameProvidedProducer(
        ClientSpanNameProvider.PRODUCER_TOPIC);

    // Send 1
    producer.send(new ProducerRecord<>("messages", 1, "test"));

    // Send 2
    producer.send(new ProducerRecord<>("messages", 1, "test"),
        (metadata, exception) -> assertEquals("messages", metadata.topic()));

    final CountDownLatch latch = new CountDownLatch(2);
    createConsumer(latch, 1, false, ClientSpanNameProvider.CONSUMER_TOPIC);
    producer.close();

    List<MockSpan> mockSpans = mockTracer.finishedSpans();
    assertEquals(4, mockSpans.size());
    for (MockSpan mockSpan : mockSpans) {
      String operationName = mockSpan.operationName();
      assertEquals("messages", operationName);
      String spanKind = (String) mockSpan.tags().get(Tags.SPAN_KIND.getKey());
      assertTrue(
          spanKind.equals(Tags.SPAN_KIND_CONSUMER) || spanKind.equals(Tags.SPAN_KIND_PRODUCER));
    }
    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void with_parent() throws Exception {
    Producer<Integer, String> producer = createTracingProducer();

    final MockSpan parent = mockTracer.buildSpan("parent").start();
    try (Scope ignored = mockTracer.activateSpan(parent)) {
      producer.send(new ProducerRecord<>("messages", 1, "test"));
    }
    parent.finish();

    final CountDownLatch latch = new CountDownLatch(1);
    createConsumer(latch, 1, false, null);

    producer.close();

    List<MockSpan> mockSpans = mockTracer.finishedSpans();
    assertEquals(3, mockSpans.size());

    assertNotNull(parent);

    for (MockSpan span : mockSpans) {
      assertEquals(parent.context().traceId(), span.context().traceId());
    }

    MockSpan sendSpan = getByOperationName(mockSpans, TracingKafkaUtils.TO_PREFIX + "messages");
    assertNotNull(sendSpan);

    MockSpan receiveSpan = getByOperationName(mockSpans,
        TracingKafkaUtils.FROM_PREFIX + "messages");
    assertNotNull(receiveSpan);

    assertEquals(sendSpan.context().spanId(), receiveSpan.parentId());
    assertEquals(parent.context().spanId(), sendSpan.parentId());

    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void nullKey() throws Exception {
    Producer<Integer, String> producer = createTracingProducer();

    ProducerRecord<Integer, String> record = new ProducerRecord<>("messages", "test");
    producer.send(record);

    final Map<String, Object> consumerProps = KafkaTestUtils
        .consumerProps("sampleRawConsumer", "false", embeddedKafka.getEmbeddedKafka());
    consumerProps.put("auto.offset.reset", "earliest");

    final CountDownLatch latch = new CountDownLatch(1);
    createConsumer(latch, null, false, null);

    producer.close();
  }

  @Test
  public void testSeekInConsumerAndCloseInProducer() throws InterruptedException {

    Producer<Integer, String> producer = createTracingProducer();

    // Send 1
    producer.send(new ProducerRecord<>("messages-for-seek", 1, "test"));

    producer.close(Duration.ofSeconds(40));

    final CountDownLatch latch = new CountDownLatch(1);
    Integer key = 1;

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    final Map<String, Object> consumerProps = KafkaTestUtils
        .consumerProps("sampleRawConsumer", "false", embeddedKafka.getEmbeddedKafka());
    consumerProps.put("auto.offset.reset", "earliest");

    executorService.execute(() -> {
      KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);
      Consumer<Integer, String> consumer;

      consumer = new TracingKafkaConsumer<>(kafkaConsumer, mockTracer, null);

      TopicPartition tp = new TopicPartition("messages-for-seek", 0);
      consumer.assign(Collections.singletonList(tp));

      consumer.seek(tp, new OffsetAndMetadata(0));

      while (latch.getCount() > 0) {
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<Integer, String> record : records) {
          SpanContext spanContext = TracingKafkaUtils
              .extractSpanContext(record.headers(), mockTracer);
          assertNotNull(spanContext);
          assertEquals("test", record.value());
          assertEquals(key, record.key());

          consumer.commitSync();
          latch.countDown();
        }
      }
      kafkaConsumer.close();
    });

    assertTrue(latch.await(30, TimeUnit.SECONDS));
  }

  @Test
  public void testProducerBuilderWithDecorators() throws InterruptedException {
    Producer<Integer, String> producer = createProducerWithDecorators(null);

    producer.send(new ProducerRecord<>("messages", 1, "test"));

    producer = createProducerWithDecorators(
        Arrays.asList(SpanDecorator.STANDARD_TAGS, createDecorator()));

    producer.send(new ProducerRecord<>("messages", 1, "test"));

    producer = createProducerWithDecorators(new ArrayList());

    producer.send(new ProducerRecord<>("messages", 1, "test"));

    final CountDownLatch latch = new CountDownLatch(3);
    createConsumer(latch, 1, false, null);

    producer.close();

    List<MockSpan> mockSpans = mockTracer.finishedSpans();

    // With only standard decorator
    MockSpan standardSpan = mockSpans.get(0);
    checkSpans(Collections.singletonList(standardSpan));
    assertEquals("kafka", standardSpan.tags().get("peer.service"));

    // With standard and custom decorator
    MockSpan customSpan = mockSpans.get(1);
    checkSpans(Collections.singletonList(customSpan));
    assertEquals("overwritten", customSpan.tags().get("peer.service"));
    assertEquals("new-producer-test", customSpan.tags().get("new.tag.test"));

    // Without any decorator
    assertEquals("producer", mockSpans.get(2).tags().get("span.kind"));
  }

  @Test
  public void testProducerBuilderWithSpanNameProvider() throws InterruptedException {
    Producer<Integer, String> producer = createProducerWithSpanNameProvider(null);
    ProducerRecord<Integer, String> record1 = new ProducerRecord<>("messages", 1, "test");

    producer.send(record1);

    BiFunction<String, ProducerRecord, String> operationNameProvider =
        (operationName, producerRecord) -> createSpanNameProvider();
    producer = createProducerWithSpanNameProvider(operationNameProvider);

    ProducerRecord<Integer, String> record2 = new ProducerRecord<>("messages", 1, "test");

    producer.send(record2);

    final CountDownLatch latch = new CountDownLatch(2);
    createConsumer(latch, 1, false, null);

    producer.close();

    List<MockSpan> mockSpans = mockTracer.finishedSpans();

    // With standard span name provider
    assertEquals("To_" + record1.topic(), mockSpans.get(0).operationName());

    // With custom span name provider
    assertEquals("Test_SpanNameProvider", mockSpans.get(1).operationName());
  }

  @Test
  public void testConsumerBuilderWithStandardDecorators() throws InterruptedException {
    Producer<Integer, String> producer = createTracingProducer();
    producer.send(new ProducerRecord<>("messages", 1, "test"));

    producer.close();

    assertEquals(1, mockTracer.finishedSpans().size());

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    final CountDownLatch latch = new CountDownLatch(1);

    executorService.execute(() -> {
      Consumer<Integer, String> consumer = createConsumerWithDecorators(null);

      while (latch.getCount() > 0) {
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<Integer, String> record : records) {
          SpanContext spanContext = TracingKafkaUtils
              .extractSpanContext(record.headers(), mockTracer);
          assertNotNull(spanContext);
          assertEquals("test", record.value());
          assertEquals((Integer) 1, record.key());

          consumer.commitSync();
          latch.countDown();
        }
      }
      consumer.close();
    });

    assertTrue(latch.await(30, TimeUnit.SECONDS));

    List<MockSpan> mockSpans = mockTracer.finishedSpans();
    checkSpans(mockSpans);

    MockSpan standardSpan = mockSpans.get(1);
    assertEquals("kafka", standardSpan.tags().get("peer.service"));
  }

  @Test
  public void testConsumerBuilderWithCustomDecorators() throws InterruptedException {
    Producer<Integer, String> producer = createTracingProducer();
    producer.send(new ProducerRecord<>("messages", 1, "test"));

    producer.close();

    assertEquals(1, mockTracer.finishedSpans().size());

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    final CountDownLatch latch = new CountDownLatch(1);

    executorService.execute(() -> {
      Consumer<Integer, String> consumer =
          createConsumerWithDecorators(
              Arrays.asList(SpanDecorator.STANDARD_TAGS, createDecorator()));

      while (latch.getCount() > 0) {
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<Integer, String> record : records) {
          SpanContext spanContext = TracingKafkaUtils
              .extractSpanContext(record.headers(), mockTracer);
          assertNotNull(spanContext);
          assertEquals("test", record.value());
          assertEquals((Integer) 1, record.key());

          consumer.commitSync();
          latch.countDown();
        }
      }
      consumer.close();
    });

    assertTrue(latch.await(30, TimeUnit.SECONDS));

    List<MockSpan> mockSpans = mockTracer.finishedSpans();
    checkSpans(mockSpans);

    MockSpan customSpan = mockSpans.get(1);
    assertEquals("overwritten", customSpan.tags().get("peer.service"));
    assertEquals("new-consumer-test", customSpan.tags().get("new.tag.test"));
  }

  @Test
  public void testConsumerBuilderWithoutDecorators() throws InterruptedException {
    Producer<Integer, String> producer = createTracingProducer();
    producer.send(new ProducerRecord<>("messages", 1, "test"));

    producer.close();

    assertEquals(1, mockTracer.finishedSpans().size());

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    final CountDownLatch latch = new CountDownLatch(1);

    executorService.execute(() -> {
      Consumer<Integer, String> consumer = createConsumerWithDecorators(new ArrayList());

      while (latch.getCount() > 0) {
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<Integer, String> record : records) {
          SpanContext spanContext = TracingKafkaUtils
              .extractSpanContext(record.headers(), mockTracer);
          assertNotNull(spanContext);
          assertEquals("test", record.value());
          assertEquals((Integer) 1, record.key());

          consumer.commitSync();
          latch.countDown();
        }
      }
      consumer.close();
    });

    assertTrue(latch.await(30, TimeUnit.SECONDS));

    List<MockSpan> mockSpans = mockTracer.finishedSpans();

    MockSpan span = mockSpans.get(1);
    assertEquals("consumer", span.tags().get("span.kind"));
  }

  @Test
  public void testConsumerBuilderWithCustomSpanNameProvider() throws InterruptedException {
    Producer<Integer, String> producer = createTracingProducer();
    producer.send(new ProducerRecord<>("messages", 1, "test"));
    producer.close();

    assertEquals(1, mockTracer.finishedSpans().size());

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    final CountDownLatch latch = new CountDownLatch(1);

    executorService.execute(() -> {
      BiFunction<String, ConsumerRecord, String> operationNameProvider =
          (operationName, consumerRecord) -> createSpanNameProvider();
      Consumer<Integer, String> consumer = createConsumerWithSpanNameProvider(
          operationNameProvider);

      while (latch.getCount() > 0) {
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<Integer, String> record : records) {
          SpanContext spanContext = TracingKafkaUtils
              .extractSpanContext(record.headers(), mockTracer);
          assertNotNull(spanContext);
          assertEquals("test", record.value());
          assertEquals((Integer) 1, record.key());

          consumer.commitSync();
          latch.countDown();
        }
      }
      consumer.close();
    });

    assertTrue(latch.await(30, TimeUnit.SECONDS));

    assertEquals("Test_SpanNameProvider", mockTracer.finishedSpans().get(1).operationName());
  }

  @Test
  public void testConsumerBuilderWithStandardSpanNameProvider() throws InterruptedException {
    Producer<Integer, String> producer = createTracingProducer();
    producer.send(new ProducerRecord<>("messages", 1, "test"));
    producer.close();

    assertEquals(1, mockTracer.finishedSpans().size());

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    final CountDownLatch latch = new CountDownLatch(1);

    executorService.execute(() -> {
      Consumer<Integer, String> consumer = createConsumerWithSpanNameProvider(null);

      while (latch.getCount() > 0) {
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<Integer, String> record : records) {
          SpanContext spanContext = TracingKafkaUtils
              .extractSpanContext(record.headers(), mockTracer);
          assertNotNull(spanContext);
          assertEquals("test", record.value());
          assertEquals((Integer) 1, record.key());

          consumer.commitSync();
          latch.countDown();
        }
      }
      consumer.close();
    });

    assertTrue(latch.await(30, TimeUnit.SECONDS));

    assertEquals("From_messages", mockTracer.finishedSpans().get(1).operationName());
  }

  private TracingKafkaProducer<Integer, String> createTracingProducer() {
    return new TracingKafkaProducer<>(createProducer(), mockTracer);
  }

  private SpanDecorator createDecorator() {
    return new SpanDecorator() {
      @Override
      public <K, V> void onSend(ProducerRecord<K, V> record, Span span) {
        span.setTag("peer.service", "overwritten");
        span.setTag("new.tag.test", "new-producer-test");
      }

      @Override
      public <K, V> void onResponse(ConsumerRecord<K, V> record, Span span) {
        span.setTag("peer.service", "overwritten");
        span.setTag("new.tag.test", "new-consumer-test");
      }

      @Override
      public <K, V> void onError(Exception exception, Span span) {
      }
    };
  }

  private Producer<Integer, String> createProducer() {
    Map<String, Object> senderProps = KafkaTestUtils
        .producerProps(embeddedKafka.getEmbeddedKafka());
    return new KafkaProducer<>(senderProps);
  }

  private Producer<Integer, String> createNameProvidedProducer(
      BiFunction<String, ProducerRecord, String> producerSpanNameProvider) {
    return new TracingKafkaProducer<>(createProducer(), mockTracer, producerSpanNameProvider);
  }

  private Consumer<Integer, String> createConsumerWithDecorators(
      Collection<SpanDecorator> spanDecorators) {
    Map<String, Object> consumerProps = KafkaTestUtils
        .consumerProps("sampleRawConsumer", "false", embeddedKafka.getEmbeddedKafka());
    consumerProps.put("auto.offset.reset", "earliest");
    KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);
    TracingKafkaConsumerBuilder tracingKafkaConsumerBuilder =
        new TracingKafkaConsumerBuilder(kafkaConsumer, mockTracer);

    if (spanDecorators != null) {
      tracingKafkaConsumerBuilder = tracingKafkaConsumerBuilder.withDecorators(spanDecorators);
    }
    TracingKafkaConsumer tracingKafkaConsumer = tracingKafkaConsumerBuilder.build();
    tracingKafkaConsumer.subscribe(Collections.singletonList("messages"));

    return tracingKafkaConsumer;
  }

  private Consumer<Integer, String> createConsumerWithSpanNameProvider(
      BiFunction<String, ConsumerRecord, String> spanNameProvider) {
    Map<String, Object> consumerProps = KafkaTestUtils
        .consumerProps("sampleRawConsumer", "false", embeddedKafka.getEmbeddedKafka());
    consumerProps.put("auto.offset.reset", "earliest");
    KafkaConsumer kafkaConsumer = new KafkaConsumer<>(consumerProps);
    TracingKafkaConsumerBuilder tracingKafkaConsumerBuilder =
        new TracingKafkaConsumerBuilder(kafkaConsumer, mockTracer);

    if (spanNameProvider != null) {
      tracingKafkaConsumerBuilder = tracingKafkaConsumerBuilder
          .withSpanNameProvider(spanNameProvider);
    }
    TracingKafkaConsumer tracingKafkaConsumer = tracingKafkaConsumerBuilder.build();
    tracingKafkaConsumer.subscribe(Collections.singletonList("messages"));

    return tracingKafkaConsumer;
  }

  private Producer<Integer, String> createProducerWithDecorators(
      Collection<SpanDecorator> spanDecorators) {
    Map<String, Object> senderProps = KafkaTestUtils
        .producerProps(embeddedKafka.getEmbeddedKafka());
    KafkaProducer kafkaProducer = new KafkaProducer<>(senderProps);
    TracingKafkaProducerBuilder tracingKafkaProducerBuilder =
        new TracingKafkaProducerBuilder<>(kafkaProducer, mockTracer);
    if (spanDecorators != null) {
      tracingKafkaProducerBuilder = tracingKafkaProducerBuilder.withDecorators(spanDecorators);
    }

    return tracingKafkaProducerBuilder.build();
  }

  private Producer<Integer, String> createProducerWithSpanNameProvider(
      BiFunction<String, ProducerRecord, String> spanNameProvider) {
    Map<String, Object> senderProps = KafkaTestUtils
        .producerProps(embeddedKafka.getEmbeddedKafka());
    KafkaProducer kafkaProducer = new KafkaProducer<>(senderProps);
    TracingKafkaProducerBuilder tracingKafkaProducerBuilder =
        new TracingKafkaProducerBuilder<>(kafkaProducer, mockTracer);
    if (spanNameProvider != null) {
      tracingKafkaProducerBuilder = tracingKafkaProducerBuilder
          .withSpanNameProvider(spanNameProvider);
    }

    return tracingKafkaProducerBuilder.build();
  }

  private void createConsumer(final CountDownLatch latch, final Integer key,
      final boolean withInterceptor,
      final BiFunction<String, ConsumerRecord, String> consumerNameProvider)
      throws InterruptedException {

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    final Map<String, Object> consumerProps = KafkaTestUtils
        .consumerProps("sampleRawConsumer", "false", embeddedKafka.getEmbeddedKafka());
    consumerProps.put("auto.offset.reset", "earliest");
    if (withInterceptor) {
      consumerProps.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
          TracingConsumerInterceptor.class.getName());
    }

    executorService.execute(() -> {
      KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);
      Consumer<Integer, String> consumer;
      if (withInterceptor) {
        consumer = kafkaConsumer;
      } else {
        consumer = new TracingKafkaConsumer<>(kafkaConsumer, mockTracer, consumerNameProvider);
      }
      consumer.subscribe(Collections.singletonList("messages"));

      while (latch.getCount() > 0) {
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<Integer, String> record : records) {
          SpanContext spanContext = TracingKafkaUtils
              .extractSpanContext(record.headers(), mockTracer);
          assertNotNull(spanContext);
          assertEquals("test", record.value());
          if (key != null) {
            assertEquals(key, record.key());
          }
          consumer.commitSync();
          latch.countDown();
        }
      }
      kafkaConsumer.close();
    });

    assertTrue(latch.await(30, TimeUnit.SECONDS));

  }

  private static String createSpanNameProvider() {
    return "Test_SpanNameProvider";
  }

  private void checkSpans(List<MockSpan> mockSpans) {
    for (MockSpan mockSpan : mockSpans) {
      String operationName = mockSpan.operationName();
      if (operationName.equals(TracingKafkaUtils.TO_PREFIX + "messages")) {
        assertEquals(Tags.SPAN_KIND_PRODUCER, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
        assertEquals("messages", mockSpan.tags().get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
      } else if (operationName.equals(TracingKafkaUtils.FROM_PREFIX + "messages")) {
        assertEquals(Tags.SPAN_KIND_CONSUMER, mockSpan.tags().get(Tags.SPAN_KIND.getKey()));
        assertEquals(0, mockSpan.tags().get("partition"));
        long offset = (Long) mockSpan.tags().get("offset");
        assertTrue(offset >= 0L);
        assertEquals("messages", mockSpan.tags().get(Tags.MESSAGE_BUS_DESTINATION.getKey()));
      }
      assertEquals(StandardSpanDecorator.COMPONENT_NAME,
          mockSpan.tags().get(Tags.COMPONENT.getKey()));
      assertEquals(0, mockSpan.generatedErrors().size());
      assertTrue(operationName.equals(TracingKafkaUtils.TO_PREFIX + "messages")
          || operationName.equals(TracingKafkaUtils.FROM_PREFIX + "messages"));
    }
  }

  private MockSpan getByOperationName(List<MockSpan> spans, String operationName) {
    List<MockSpan> found = new ArrayList<>();
    for (MockSpan span : spans) {
      if (operationName.equals(span.operationName())) {
        found.add(span);
      }
    }

    if (found.size() > 1) {
      throw new RuntimeException("Ups, too many spans (" + found.size() + ") with operation name '"
          + operationName + "'");
    }

    return found.isEmpty() ? null : found.get(0);
  }

  private List<MockSpan> getByOperationNameAll(List<MockSpan> spans, String operationName) {
    List<MockSpan> found = new ArrayList<>();
    for (MockSpan span : spans) {
      if (operationName.equals(span.operationName())) {
        found.add(span);
      }
    }
    return found;
  }

}
