package io.opentracing.contrib.kafka;


import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.propagation.TextMapInjectAdapter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

public class TracingKafkaUtils {

  public static final String TRACING_SPAN_CONTEXT_KEY = "TRACING_SPAN_CONTEXT";

  /**
   * Extract Span Context from KafkaSpanContext
   *
   * @param kafkaSpanContext KafkaSpanContext
   * @return span context
   */
  public static SpanContext extract(KafkaSpanContext kafkaSpanContext, Tracer tracer) {
    return tracer.extract(Format.Builtin.TEXT_MAP, new TextMapExtractAdapter(kafkaSpanContext.getMap()));
  }

  /**
   * Inject Span Context to KafkaSpanContext
   *
   * @param spanContext Span Context
   * @param kafkaSpanContext KafkaSpanContext
   */
  public static void inject(SpanContext spanContext, KafkaSpanContext kafkaSpanContext, Tracer tracer) {
    tracer.inject(spanContext, Format.Builtin.TEXT_MAP, new TextMapInjectAdapter(kafkaSpanContext.getMap()));
  }

  public static KafkaSpanContext deserializeContext(Headers headers){
    Iterator<Header> iterator = headers.headers(TracingKafkaUtils.TRACING_SPAN_CONTEXT_KEY).iterator();
    if(iterator.hasNext()){
      Header spanContextHeader = iterator.next();
      byte[] spanContextData = spanContextHeader.value();

      return new KafkaSpanContextDeserializer().deserialize(spanContextData);
    } else {
      return new KafkaSpanContext();
    }
  }

  static int getMapLength(byte[] data) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(Arrays.copyOfRange(data, 0, 4));
    return byteBuffer.getInt(0);
  }
}
