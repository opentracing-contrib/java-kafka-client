package io.opentracing.contrib.kafka;

import io.opentracing.Span;
import io.opentracing.tag.Tags;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;


public class SpanDecorator {

  public static final String COMPONENT_NAME = "java-kafka";

  /**
   * Called before record is sent by producer
   */
  public static <K, V> void onSend(ProducerRecord<KafkaSpanContext<K>, V> record, Span span) {
    Tags.COMPONENT.set(span, COMPONENT_NAME);
    Tags.MESSAGE_BUS_DESTINATION.set(span, record.topic());
  }

  /**
   * Called when record is received in consumer
   */
  public static void onResponse(Span span) {
    Tags.COMPONENT.set(span, COMPONENT_NAME);
  }

  public static void onError(Exception exception, Span span) {
    Tags.ERROR.set(span, Boolean.TRUE);
    span.log(errorLogs(exception));
  }

  private static Map<String, Object> errorLogs(Throwable throwable) {
    Map<String, Object> errorLogs = new HashMap<>(4);
    errorLogs.put("event", Tags.ERROR.getKey());
    errorLogs.put("error.kind", throwable.getClass().getName());
    errorLogs.put("error.object", throwable);

    errorLogs.put("message", throwable.getMessage());

    StringWriter sw = new StringWriter();
    throwable.printStackTrace(new PrintWriter(sw));
    errorLogs.put("stack", sw.toString());

    return errorLogs;
  }
}
