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
package io.opentracing.contrib.kafka.spring;

import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.ClientSpanNameProvider;
import io.opentracing.contrib.kafka.TracingKafkaConsumer;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.core.ConsumerFactory;

public class TracingConsumerFactory<K, V> implements ConsumerFactory<K, V> {

  private final ConsumerFactory<K, V> consumerFactory;
  private final Tracer tracer;
  private final BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider;

  public TracingConsumerFactory(ConsumerFactory<K, V> consumerFactory, Tracer tracer) {
    this.tracer = tracer;
    this.consumerFactory = consumerFactory;
    this.consumerSpanNameProvider = ClientSpanNameProvider.CONSUMER_OPERATION_NAME;
  }

  public TracingConsumerFactory(ConsumerFactory<K, V> consumerFactory, Tracer tracer,
                                BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider) {
    this.tracer = tracer;
    this.consumerFactory = consumerFactory;
    this.consumerSpanNameProvider = (consumerSpanNameProvider == null)
      ? ClientSpanNameProvider.CONSUMER_OPERATION_NAME
      : consumerSpanNameProvider;
  }

  @Override
  public Consumer<K, V> createConsumer() {
    return new TracingKafkaConsumer<>(consumerFactory.createConsumer(), tracer, consumerSpanNameProvider);
  }

  @Override
  public Consumer<K, V> createConsumer(String clientIdSuffix) {
    return new TracingKafkaConsumer<>(consumerFactory.createConsumer(clientIdSuffix), tracer, consumerSpanNameProvider);
  }

  @Override
  public Consumer<K, V> createConsumer(String groupId, String clientIdSuffix) {
    return new TracingKafkaConsumer<>(consumerFactory.createConsumer(groupId, clientIdSuffix),
        tracer, consumerSpanNameProvider);
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
