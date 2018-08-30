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
import io.opentracing.contrib.kafka.TracingKafkaProducer;
import io.opentracing.util.GlobalTracer;
import java.util.function.BiFunction;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.kafka.core.ProducerFactory;

public class TracingProducerFactory<K, V> implements ProducerFactory<K, V>, DisposableBean {

  private final ProducerFactory<K, V> producerFactory;
  private final Tracer tracer;
  private final BiFunction<String, ProducerRecord, String> producerSpanNameProvider;

  public TracingProducerFactory(ProducerFactory<K, V> producerFactory, Tracer tracer) {
    this.producerFactory = producerFactory;
    this.tracer = tracer;
    this.producerSpanNameProvider = ClientSpanNameProvider.PRODUCER_OPERATION_NAME;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingProducerFactory(ProducerFactory<K, V> producerFactory) {
    this(producerFactory, GlobalTracer.get());
  }

  public TracingProducerFactory(ProducerFactory<K, V> producerFactory, Tracer tracer,
      BiFunction<String, ProducerRecord, String> producerSpanNameProvider) {
    this.producerFactory = producerFactory;
    this.tracer = tracer;
    this.producerSpanNameProvider = (producerSpanNameProvider == null)
        ? ClientSpanNameProvider.PRODUCER_OPERATION_NAME
        : producerSpanNameProvider;
  }

  /**
   * GlobalTracer is used to get tracer
   */
  public TracingProducerFactory(ProducerFactory<K, V> producerFactory,
      BiFunction<String, ProducerRecord, String> producerSpanNameProvider) {
    this(producerFactory, GlobalTracer.get(), producerSpanNameProvider);
  }

  @Override
  public Producer<K, V> createProducer() {
    return new TracingKafkaProducer<>(producerFactory.createProducer(), tracer,
        producerSpanNameProvider);
  }

  @Override
  public boolean transactionCapable() {
    return producerFactory.transactionCapable();
  }

  @Override
  public void destroy() throws Exception {
    if (producerFactory instanceof DisposableBean) {
      ((DisposableBean) producerFactory).destroy();
    }
  }
}
