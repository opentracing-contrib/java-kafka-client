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

import io.opentracing.Tracer;
import java.util.Collection;
import java.util.Collections;
import java.util.function.BiFunction;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TracingKafkaProducerBuilder<K, V> {
  private Collection<SpanDecorator> spanDecorators;
  private Producer<K, V> producer;
  private Tracer tracer;
  private BiFunction<String, ProducerRecord, String> producerSpanNameProvider;

  public TracingKafkaProducerBuilder(Producer<K, V> producer, Tracer tracer) {
    this.tracer = tracer;
    this.producer = producer;
    this.spanDecorators = Collections.singletonList(SpanDecorator.STANDARD_TAGS);
    this.producerSpanNameProvider = ClientSpanNameProvider.PRODUCER_OPERATION_NAME;
  }

  public TracingKafkaProducerBuilder withDecorators(Collection<SpanDecorator> spanDecorators) {
    this.spanDecorators = Collections.unmodifiableCollection(spanDecorators);
    return this;
  }

  public TracingKafkaProducerBuilder withSpanNameProvider(
      BiFunction<String, ProducerRecord, String> producerSpanNameProvider) {
    this.producerSpanNameProvider = producerSpanNameProvider;
    return this;
  }

  public TracingKafkaProducer<K, V> build() {
    return new TracingKafkaProducer<>(producer, tracer, spanDecorators, producerSpanNameProvider);
  }
}
