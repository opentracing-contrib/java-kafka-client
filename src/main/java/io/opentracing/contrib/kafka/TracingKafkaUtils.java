package io.opentracing.contrib.kafka;


import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import org.apache.kafka.common.header.Headers;

class TracingKafkaUtils {

  /**
   * Extract Span Context from record headers
   *
   * @param headers record headers
   * @return span context
   */
  static SpanContext extract(Headers headers, Tracer tracer) {
    return tracer
        .extract(Format.Builtin.TEXT_MAP, new HeadersMapExtractAdapter(headers));
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
        new HeadersMapInjectAdapter(headers));
  }
}
