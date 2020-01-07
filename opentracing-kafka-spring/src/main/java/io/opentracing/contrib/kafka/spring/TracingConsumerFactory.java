/*
 * Copyright 2017-2019 The OpenTracing Authors
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
import io.opentracing.contrib.kafka.TracingKafkaConsumerBuilder;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiFunction;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.core.ConsumerFactory;

public class TracingConsumerFactory<K, V> implements ConsumerFactory<K, V> {

  private final ConsumerFactory<K, V> consumerFactory;
  private final Tracer tracer;
  private final Collection<SpanDecorator> spanDecorators;
  private final BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider;

  public TracingConsumerFactory(ConsumerFactory<K, V> consumerFactory, Tracer tracer) {
    this(consumerFactory, tracer, null, null);
  }

  public TracingConsumerFactory(ConsumerFactory<K, V> consumerFactory, Tracer tracer,
      Collection<SpanDecorator> spanDecorators) {
    this(consumerFactory, tracer, spanDecorators, null);
  }

  public TracingConsumerFactory(ConsumerFactory<K, V> consumerFactory, Tracer tracer,
      BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider) {
    this(consumerFactory, tracer, null, consumerSpanNameProvider);
  }

  public TracingConsumerFactory(ConsumerFactory<K, V> consumerFactory, Tracer tracer,
      Collection<SpanDecorator> spanDecorators, BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider) {
    this.tracer = tracer;
    this.consumerFactory = consumerFactory;
    this.spanDecorators = (spanDecorators == null)
        ? Collections.singletonList(STANDARD_TAGS)
        : spanDecorators;
    this.consumerSpanNameProvider = (consumerSpanNameProvider == null)
        ? ClientSpanNameProvider.CONSUMER_OPERATION_NAME
        : consumerSpanNameProvider;
  }

  @Override
  public Consumer<K, V> createConsumer() {
    return new TracingKafkaConsumerBuilder<>(consumerFactory.createConsumer(), tracer)
        .withDecorators(spanDecorators).withSpanNameProvider(consumerSpanNameProvider).build();
  }

  @Override
  public Consumer<K, V> createConsumer(String clientIdSuffix) {
    return new TracingKafkaConsumerBuilder<>(consumerFactory.createConsumer(clientIdSuffix), tracer)
        .withDecorators(spanDecorators).withSpanNameProvider(consumerSpanNameProvider).build();
  }

  @Override
  public Consumer<K, V> createConsumer(String groupId, String clientIdSuffix) {
    return new TracingKafkaConsumerBuilder<>(consumerFactory.createConsumer(groupId, clientIdSuffix), tracer)
        .withDecorators(spanDecorators).withSpanNameProvider(consumerSpanNameProvider).build();
  }

  @Override
  public Consumer<K, V> createConsumer(String groupId, String clientIdPrefix, String clientIdSuffix) {
    return new TracingKafkaConsumerBuilder<>(consumerFactory.createConsumer(groupId, clientIdPrefix, clientIdSuffix),
        tracer).withDecorators(spanDecorators).withSpanNameProvider(consumerSpanNameProvider).build();
  }

  @Override
  public Consumer<K, V> createConsumer(String groupId, String clientIdPrefix,
      String clientIdSuffix, Properties properties) {
    return new TracingKafkaConsumerBuilder<>(consumerFactory.createConsumer(groupId, clientIdPrefix,
        clientIdSuffix, properties), tracer).withDecorators(spanDecorators)
        .withSpanNameProvider(consumerSpanNameProvider).build();
  }

  @Override
  public boolean isAutoCommit() {
    return consumerFactory.isAutoCommit();
  }

  @Override
  public Map<String, Object> getConfigurationProperties() {
    return consumerFactory.getConfigurationProperties();
  }

  @Override
  public Deserializer<K> getKeyDeserializer() {
    return consumerFactory.getKeyDeserializer();
  }

  @Override
  public Deserializer<V> getValueDeserializer() {
    return consumerFactory.getValueDeserializer();
  }
}
