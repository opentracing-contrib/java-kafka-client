package io.opentracing.contrib.kafka;


import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Serializer;

public class TracingKafkaProducer<K, V> implements Producer<K, V> {

  private KafkaProducer<KafkaSpanContext<K>, V> producer;
  private final Tracer tracer;

  @SuppressWarnings("unchecked")
  public TracingKafkaProducer(Map<String, Object> configs, Tracer tracer) {
    this.tracer = tracer;
    Object keySerializerValue = configs.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
    Serializer<K> keySerializer = TracingKafkaUtils
        .getInstance(keySerializerValue, Serializer.class);

    setPartitioner(configs);

    this.producer = new KafkaProducer<>(configs, new KafkaSpanContextSerializer<>(keySerializer),
        null);
  }

  public TracingKafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer,
      Serializer<V> valueSerializer, Tracer tracer) {
    this.tracer = tracer;
    setPartitioner(configs);
    this.producer = new KafkaProducer<>(configs, new KafkaSpanContextSerializer<>(keySerializer),
        valueSerializer);
  }

  @SuppressWarnings("unchecked")
  public TracingKafkaProducer(Properties properties, Tracer tracer) {
    this.tracer = tracer;
    Object keySerializerValue = properties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
    Serializer<K> keySerializer = TracingKafkaUtils
        .getInstance(keySerializerValue, Serializer.class);

    setPartitioner(properties);

    this.producer = new KafkaProducer<>(properties, new KafkaSpanContextSerializer<>(keySerializer),
        null);
  }

  public TracingKafkaProducer(Properties properties, Serializer<K> keySerializer,
      Serializer<V> valueSerializer, Tracer tracer) {
    this.tracer = tracer;
    setPartitioner(properties);
    this.producer = new KafkaProducer<>(properties, new KafkaSpanContextSerializer<>(keySerializer),
        valueSerializer);
  }

  private void setPartitioner(Properties properties) {
    Object partitionerClass = properties.get(ProducerConfig.PARTITIONER_CLASS_CONFIG);
    if (partitionerClass != null) {
      Partitioner partitioner = TracingKafkaUtils.getInstance(partitionerClass, Partitioner.class);
      if (partitioner instanceof TracingPartitioner) {
        return;
      }
    }

    properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, TracingPartitioner.class);
  }

  private void setPartitioner(Map<String, Object> configs) {
    Object partitionerClass = configs.get(ProducerConfig.PARTITIONER_CLASS_CONFIG);
    if (partitionerClass != null) {
      Partitioner partitioner = TracingKafkaUtils.getInstance(partitionerClass, Partitioner.class);
      if (partitioner instanceof TracingPartitioner) {
        return;
      }
    }

    configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, TracingPartitioner.class);
  }


  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return send(record, null);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    ProducerRecord<KafkaSpanContext<K>, V> wrappedRecord = new ProducerRecord<>(record.topic(),
        record.partition(), record.timestamp(), new KafkaSpanContext<>(record.key()),
        record.value());

    Callback wrappedCallback = callback;
    if (!(callback instanceof TracingCallback)) {
      Span span = buildAndInjectSpan(wrappedRecord);
      wrappedCallback = new TracingCallback(callback, span);
    }
    return producer.send(wrappedRecord, wrappedCallback);
  }

  @Override
  public void flush() {
    producer.flush();
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return producer.partitionsFor(topic);
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return producer.metrics();
  }

  @Override
  public void close() {
    producer.close();
  }

  @Override
  public void close(long timeout, TimeUnit timeUnit) {
    producer.close(timeout, timeUnit);
  }

  private Span buildAndInjectSpan(ProducerRecord<KafkaSpanContext<K>, V> record) {
    Tracer.SpanBuilder spanBuilder = tracer.buildSpan("send").ignoreActiveSpan()
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT);

    SpanContext spanContext = TracingKafkaUtils.extract(record.key(), tracer);

    if (spanContext == null && tracer.activeSpan() != null) {
      spanContext = tracer.activeSpan().context();
    }

    if (spanContext != null) {
      spanBuilder.asChildOf(spanContext);
    }

    Span span = spanBuilder.startManual();
    SpanDecorator.onSend(record, span);

    TracingKafkaUtils.inject(span.context(), record.key(), tracer);
    return span;
  }
}
