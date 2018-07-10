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
package io.opentracing.contrib.kafka;


import io.opentracing.References;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;

public class TracingKafkaUtils {
  private static final Logger logger = LoggerFactory.getLogger(TracingKafkaUtils.class);

  /**
   * Extract Span Context from record headers
   *
   * @param headers record headers
   * @return span context
   */
  static SpanContext extract(Headers headers, Tracer tracer) {
    return tracer
        .extract(Format.Builtin.TEXT_MAP, new HeadersMapExtractAdapter(headers, false));
  }

  /**
   * Extract Span Context from Consumer record headers
   *
   * @param headers Consumer record headers
   * @return span context
   */
  public static SpanContext extractSpanContext(Headers headers, Tracer tracer) {
    return tracer
        .extract(Format.Builtin.TEXT_MAP, new HeadersMapExtractAdapter(headers, true));
  }

  /**
   * Inject Span Context to record headers
   *
   * @param spanContext Span Context
   * @param headers record headers
   */
  static void inject(SpanContext spanContext, Headers headers,
      Tracer tracer) {
    tracer.inject(spanContext, Format.Builtin.TEXT_MAP,
        new HeadersMapInjectAdapter(headers, false));
  }

  /**
   * Inject second Span Context to record headers
   *
   * @param spanContext Span Context
   * @param headers record headers
   */
  static void injectSecond(SpanContext spanContext, Headers headers,
      Tracer tracer) {
    tracer.inject(spanContext, Format.Builtin.TEXT_MAP,
        new HeadersMapInjectAdapter(headers, true));
  }

  static <K,V> Scope buildAndInjectSpan(ProducerRecord<K, V> record, Tracer tracer) {
    return buildAndInjectSpan(record, tracer, ClientSpanNameProvider.PRODUCER_OPERATION_NAME);
  }

  static <K,V> Scope buildAndInjectSpan(ProducerRecord<K, V> record, Tracer tracer,
                                        BiFunction<String, ProducerRecord, String> producerSpanNameProvider) {
    Tracer.SpanBuilder spanBuilder = tracer.buildSpan(producerSpanNameProvider.apply("send", record))
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_PRODUCER);

    SpanContext spanContext = TracingKafkaUtils.extract(record.headers(), tracer);

    if (spanContext != null) {
      spanBuilder.asChildOf(spanContext);
    }

    Scope scope = spanBuilder.startActive(false);
    SpanDecorator.onSend(record, scope.span());

    try {
      TracingKafkaUtils.inject(scope.span().context(), record.headers(), tracer);
    } catch (Exception e) {
      // it can happen if headers are read only (when record is sent second time)
      logger.error("failed to inject span context. sending record second time?", e);
    }

    return scope;
  }

  static <K,V> void buildAndFinishChildSpan(ConsumerRecord<K, V> record, Tracer tracer) {
    buildAndFinishChildSpan(record, tracer, ClientSpanNameProvider.CONSUMER_OPERATION_NAME);
  }

  static <K,V> void buildAndFinishChildSpan(ConsumerRecord<K, V> record, Tracer tracer,
                                            BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider) {
    SpanContext parentContext = TracingKafkaUtils.extract(record.headers(), tracer);

    if (parentContext != null) {

      Tracer.SpanBuilder spanBuilder = tracer.buildSpan(consumerSpanNameProvider.apply("receive", record))
          .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER);

      spanBuilder.addReference(References.FOLLOWS_FROM, parentContext);

      Span span = spanBuilder.start();
      SpanDecorator.onResponse(record, span);
      span.finish();

      // Inject created span context into record headers for extraction by client to continue span chain
      TracingKafkaUtils.injectSecond(span.context(), record.headers(), tracer);
    }
  }
}
