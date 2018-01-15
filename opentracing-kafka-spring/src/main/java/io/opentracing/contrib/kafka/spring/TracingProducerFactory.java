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
import io.opentracing.contrib.kafka.TracingKafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.kafka.core.ProducerFactory;

public class TracingProducerFactory<K, V> implements ProducerFactory<K, V> {

  private final ProducerFactory<K, V> producerFactory;
  private final Tracer tracer;

  public TracingProducerFactory(ProducerFactory<K, V> producerFactory, Tracer tracer) {
    this.producerFactory = producerFactory;
    this.tracer = tracer;
  }

  @Override
  public Producer<K, V> createProducer() {
    return new TracingKafkaProducer<>(producerFactory.createProducer(), tracer);
  }

  @Override
  public boolean transactionCapable() {
    return producerFactory.transactionCapable();
  }
}
