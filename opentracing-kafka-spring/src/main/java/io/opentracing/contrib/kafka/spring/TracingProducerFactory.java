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
package io.opentracing.contrib.kafka.spring;

import static io.opentracing.contrib.kafka.SpanDecorator.STANDARD_TAGS;

import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.ClientSpanNameProvider;
import io.opentracing.contrib.kafka.SpanDecorator;
import io.opentracing.contrib.kafka.TracingKafkaProducerBuilder;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.kafka.core.ProducerFactory;

public class TracingProducerFactory<K, V> implements ProducerFactory<K, V>, DisposableBean {

  private final ProducerFactory<K, V> producerFactory;
  private final Tracer tracer;
  private final Collection<SpanDecorator> spanDecorators;
  private final BiFunction<String, ProducerRecord, String> producerSpanNameProvider;

  public TracingProducerFactory(ProducerFactory<K, V> producerFactory, Tracer tracer) {
    this(producerFactory, tracer, null, null);
  }

  public TracingProducerFactory(ProducerFactory<K, V> producerFactory, Tracer tracer,
      Collection<SpanDecorator> spanDecorators) {
    this(producerFactory, tracer, spanDecorators, null);
  }

  public TracingProducerFactory(ProducerFactory<K, V> producerFactory, Tracer tracer,
      BiFunction<String, ProducerRecord, String> producerSpanNameProvider) {
    this(producerFactory, tracer, null, producerSpanNameProvider);
  }

  public TracingProducerFactory(ProducerFactory<K, V> producerFactory, Tracer tracer,
      Collection<SpanDecorator> spanDecorators,
      BiFunction<String, ProducerRecord, String> producerSpanNameProvider) {
    this.producerFactory = producerFactory;
    this.tracer = tracer;
    this.spanDecorators = (spanDecorators == null)
        ? Collections.singletonList(STANDARD_TAGS)
        : spanDecorators;
    this.producerSpanNameProvider = (producerSpanNameProvider == null)
        ? ClientSpanNameProvider.PRODUCER_OPERATION_NAME
        : producerSpanNameProvider;
  }

  @Override
  public Producer<K, V> createProducer() {
    return new TracingKafkaProducerBuilder<>(producerFactory.createProducer(), tracer)
        .withDecorators(spanDecorators)
        .withSpanNameProvider(producerSpanNameProvider).build();
  }

  @Override
  public Producer<K, V> createProducer(String txIdPrefix) {
    return new TracingKafkaProducerBuilder<>(producerFactory.createProducer(txIdPrefix), tracer)
        .withDecorators(spanDecorators).withSpanNameProvider(producerSpanNameProvider).build();
  }

  @Override
  public boolean transactionCapable() {
    return producerFactory.transactionCapable();
  }

  @Override
  public void closeProducerFor(String transactionIdSuffix) {
    producerFactory.closeProducerFor(transactionIdSuffix);
  }

  @Override
  public boolean isProducerPerConsumerPartition() {
    return producerFactory.isProducerPerConsumerPartition();
  }

  @Override
  public void closeThreadBoundProducer() {
    producerFactory.closeThreadBoundProducer();
  }

  @Override
  public void destroy() throws Exception {
    if (producerFactory instanceof DisposableBean) {
      ((DisposableBean) producerFactory).destroy();
    }
  }

  @Override
  public Producer<K, V> createNonTransactionalProducer() {
    return producerFactory.createNonTransactionalProducer();
  }

  @Override
  public void reset() {
    producerFactory.reset();
  }

  @Override
  public Map<String, Object> getConfigurationProperties() {
    return producerFactory.getConfigurationProperties();
  }

  @Override
  public Supplier<Serializer<V>> getValueSerializerSupplier() {
    return producerFactory.getValueSerializerSupplier();
  }

  @Override
  public Supplier<Serializer<K>> getKeySerializerSupplier() {
    return producerFactory.getKeySerializerSupplier();
  }

  @Override
  public boolean isProducerPerThread() {
    return producerFactory.isProducerPerThread();
  }

  @Override
  public String getTransactionIdPrefix() {
    return producerFactory.getTransactionIdPrefix();
  }

  @Override
  public Duration getPhysicalCloseTimeout() {
    return producerFactory.getPhysicalCloseTimeout();
  }
}
