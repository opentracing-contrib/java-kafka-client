package io.opentracing.contrib.kafka;

import io.opentracing.ActiveSpan;
import io.opentracing.Span;
import io.opentracing.tag.Tags;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;


class SpanDecorator {

  static final String COMPONENT_NAME = "java-kafka";

  /**
   * Called before record is sent by producer
   */
  static <K, V> void onSend(ProducerRecord<K, V> record, ActiveSpan span) {
    Tags.COMPONENT.set(span, COMPONENT_NAME);
    Tags.MESSAGE_BUS_DESTINATION.set(span, record.topic());
    if (record.partition() != null) {
      span.setTag("partition", record.partition());
    }
  }

  /**
   * Called when record is received in consumer
   */
  static <K, V> void onResponse(ConsumerRecord<K, V> record, Span span) {
    Tags.COMPONENT.set(span, COMPONENT_NAME);
    span.setTag("partition", record.partition());
    span.setTag("topic", record.topic());
    span.setTag("offset", record.offset());

  }

  static void onError(Exception exception, ActiveSpan span) {
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
