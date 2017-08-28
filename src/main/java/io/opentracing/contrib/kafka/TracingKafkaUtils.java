package io.opentracing.contrib.kafka;


import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import org.apache.kafka.common.header.Headers;

public class TracingKafkaUtils {

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
}
