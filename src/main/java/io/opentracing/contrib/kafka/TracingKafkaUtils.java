package io.opentracing.contrib.kafka;


import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.propagation.TextMapInjectAdapter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Utils;

public class TracingKafkaUtils {

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
  public static <K> void inject(SpanContext spanContext, KafkaSpanContext kafkaSpanContext,
      Tracer tracer) {
    tracer.inject(spanContext, Format.Builtin.TEXT_MAP,
        new TextMapInjectAdapter(kafkaSpanContext.getMap()));
  }

  public static <T> T getInstance(Object value, Class<T> t) {
    String trimmed = null;
    Class<?> c = null;
    if (value instanceof String) {
      trimmed = ((String) value).trim();
    }

    if (value instanceof Class) {
      c = (Class<?>) value;
    } else if (value instanceof String) {
      try {
        c = Class.forName(trimmed, true, Utils.getContextOrKafkaClassLoader());
      } catch (ClassNotFoundException e) {
        throw new KafkaException(e);
      }
    }

    if (c == null) {
      return null;
    }

    Object o = Utils.newInstance(c);

    if (!t.isInstance(o)) {
      throw new KafkaException(c.getName() + " is not an instance of " + t.getName());
    }

    return t.cast(o);
  }

  static int getMapLength(byte[] data) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(Arrays.copyOfRange(data, 0, 4));
    return byteBuffer.getInt(0);
  }
}
