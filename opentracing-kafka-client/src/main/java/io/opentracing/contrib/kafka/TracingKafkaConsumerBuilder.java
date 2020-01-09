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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class TracingKafkaConsumerBuilder<K, V> {
  private Collection<SpanDecorator> spanDecorators;
  private Consumer<K, V> consumer;
  private Tracer tracer;
  private BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider;

  public TracingKafkaConsumerBuilder(Consumer<K, V> consumer, Tracer tracer) {
    this.tracer = tracer;
    this.consumer = consumer;
    this.spanDecorators = Collections.singletonList(SpanDecorator.STANDARD_TAGS);
    this.consumerSpanNameProvider = ClientSpanNameProvider.CONSUMER_OPERATION_NAME;
  }

  public TracingKafkaConsumerBuilder withDecorators(Collection<SpanDecorator> spanDecorators) {
    this.spanDecorators = Collections.unmodifiableCollection(spanDecorators);
    return this;
  }

  public TracingKafkaConsumerBuilder withSpanNameProvider(
      BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider) {
    this.consumerSpanNameProvider = consumerSpanNameProvider;
    return this;
  }

  public TracingKafkaConsumer<K, V> build() {
    return new TracingKafkaConsumer<>(consumer, tracer, spanDecorators, consumerSpanNameProvider);
  }
}
