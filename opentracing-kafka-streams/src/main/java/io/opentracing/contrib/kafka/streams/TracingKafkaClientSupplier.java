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

import static io.opentracing.contrib.kafka.SpanDecorator.STANDARD_TAGS;

import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.ClientSpanNameProvider;
import io.opentracing.contrib.kafka.SpanDecorator;
import io.opentracing.contrib.kafka.TracingKafkaConsumerBuilder;
import io.opentracing.contrib.kafka.TracingKafkaProducerBuilder;
import io.opentracing.util.GlobalTracer;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaClientSupplier;

public class TracingKafkaClientSupplier implements KafkaClientSupplier {

  private final Tracer tracer;
  private final Collection<SpanDecorator> spanDecorators;
  private final BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider;
  private final BiFunction<String, ProducerRecord, String> producerSpanNameProvider;

  public TracingKafkaClientSupplier(Tracer tracer) {
    this(tracer, null, null, null);
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingKafkaClientSupplier() {
    this(GlobalTracer.get(), null, null, null);
  }

  public TracingKafkaClientSupplier(Tracer tracer, Collection<SpanDecorator> spanDecorators) {
    this(tracer, spanDecorators, null, null);
  }

  public TracingKafkaClientSupplier(Tracer tracer,
      BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider,
      BiFunction<String, ProducerRecord, String> producerSpanNameProvider) {
    this(tracer, null, consumerSpanNameProvider, producerSpanNameProvider);
  }

  public TracingKafkaClientSupplier(Tracer tracer,
      Collection<SpanDecorator> spanDecorators,
      BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider,
      BiFunction<String, ProducerRecord, String> producerSpanNameProvider) {
    this.tracer = tracer;
    this.spanDecorators = (spanDecorators == null)
        ? Collections.singletonList(STANDARD_TAGS)
        : spanDecorators;
    this.consumerSpanNameProvider = (consumerSpanNameProvider == null)
        ? ClientSpanNameProvider.CONSUMER_OPERATION_NAME
        : consumerSpanNameProvider;
    this.producerSpanNameProvider = (producerSpanNameProvider == null)
        ? ClientSpanNameProvider.PRODUCER_OPERATION_NAME
        : producerSpanNameProvider;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingKafkaClientSupplier(
      BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider,
      BiFunction<String, ProducerRecord, String> producerSpanNameProvider) {
    this(GlobalTracer.get(), null, consumerSpanNameProvider, producerSpanNameProvider);
  }

  // This method is required by Kafka Streams >=3.0
  @Override
  public Admin getAdmin(final Map<String, Object> config) {
    // create a new client upon each call; but expect this call to be only triggered once so this should be fine
    return Admin.create(config);
  }

  // This method is required by Kafka Streams >=1.1, and optional for Kafka Streams <1.1
  public AdminClient getAdminClient(final Map<String, Object> config) {
    // create a new client upon each call; but expect this call to be only triggered once so this should be fine
    return AdminClient.create(config);
  }

  @Override
  public Producer<byte[], byte[]> getProducer(Map<String, Object> config) {
    return new TracingKafkaProducerBuilder<>(
        new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer()),
        tracer).withDecorators(spanDecorators).withSpanNameProvider(producerSpanNameProvider)
        .build();
  }

  @Override
  public Consumer<byte[], byte[]> getConsumer(Map<String, Object> config) {
    return new TracingKafkaConsumerBuilder<>(
        new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer()),
        tracer).withDecorators(spanDecorators).withSpanNameProvider(consumerSpanNameProvider)
        .build();
  }

  @Override
  public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> config) {
    return new TracingKafkaConsumerBuilder<>(
        new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer()),
        tracer).withDecorators(spanDecorators).withSpanNameProvider(consumerSpanNameProvider)
        .build();
  }

  @Override
  public Consumer<byte[], byte[]> getGlobalConsumer(Map<String, Object> config) {
    return new TracingKafkaConsumerBuilder<>(
        new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer()),
        tracer).withDecorators(spanDecorators).withSpanNameProvider(consumerSpanNameProvider)
        .build();
  }
}
