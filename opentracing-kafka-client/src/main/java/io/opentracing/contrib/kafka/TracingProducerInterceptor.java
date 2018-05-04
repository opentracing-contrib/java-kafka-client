package io.opentracing.contrib.kafka;

import io.opentracing.Scope;
import io.opentracing.util.GlobalTracer;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class TracingProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

  @Override
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> producerRecord) {
    try (Scope scope = TracingKafkaUtils.buildAndInjectSpan(producerRecord, GlobalTracer.get())) {
      scope.span().finish();
    }
    return producerRecord;
  }

  @Override
  public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}
