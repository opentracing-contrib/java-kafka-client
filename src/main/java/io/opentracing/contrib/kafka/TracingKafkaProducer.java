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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.Serializer;

public class TracingKafkaProducer<K, V> implements Producer<K, V> {

  private KafkaProducer<K, V> producer;
  private final Tracer tracer;

  @SuppressWarnings("unchecked")
  public TracingKafkaProducer(Map<String, Object> configs, Tracer tracer) {
    this.tracer = tracer;
    this.producer = new KafkaProducer<>(configs);
  }

  public TracingKafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer,
      Serializer<V> valueSerializer, Tracer tracer) {
    this.tracer = tracer;
    this.producer = new KafkaProducer<>(configs, keySerializer, valueSerializer);
  }

  @SuppressWarnings("unchecked")
  public TracingKafkaProducer(Properties properties, Tracer tracer) {
    this.tracer = tracer;
    this.producer = new KafkaProducer<>(properties);
  }

  public TracingKafkaProducer(Properties properties, Serializer<K> keySerializer,
      Serializer<V> valueSerializer, Tracer tracer) {
    this.tracer = tracer;
    this.producer = new KafkaProducer<>(properties, keySerializer, valueSerializer);
  }

  @Override
  public void initTransactions() {
    producer.initTransactions();
  }

  @Override
  public void beginTransaction() throws ProducerFencedException {
    producer.beginTransaction();
  }

  @Override
  public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> map, String s) throws ProducerFencedException {
    producer.sendOffsetsToTransaction(map, s);
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    producer.commitTransaction();
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    producer.abortTransaction();
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return send(record, null);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    /*ProducerRecord<K, V> wrappedRecord = new ProducerRecord<>(record.topic(),
        record.partition(), record.timestamp(), record.key(),
        record.value(), record.headers());

    wrappedRecord.headers().add("TRACING_SPAN_CONTEXT", new KafkaSpanContextSerializer().serialize(new KafkaSpanContext()));*/

    ProducerRecord<K, V> wrappedRecord = new ProducerRecord<>(record.topic(),
        record.partition(), record.timestamp(), record.key(),
        record.value(), record.headers());

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

  private Span buildAndInjectSpan(ProducerRecord<K, V> record) {
    Tracer.SpanBuilder spanBuilder = tracer.buildSpan("send").ignoreActiveSpan()
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT);

    KafkaSpanContext kafkaSpanContext = TracingKafkaUtils.deserializeContext(record.headers());

    SpanContext spanContext = TracingKafkaUtils.extract(kafkaSpanContext, tracer);

    if (spanContext == null && tracer.activeSpan() != null) {
      spanContext = tracer.activeSpan().context();
    }

    if (spanContext != null) {
      spanBuilder.asChildOf(spanContext);
    }

    Span span = spanBuilder.startManual();
    SpanDecorator.onSend(record, span);

    TracingKafkaUtils.inject(span.context(), kafkaSpanContext, tracer);

    record.headers()
        .remove("TRACING_SPAN_CONTEXT")
        .add("TRACING_SPAN_CONTEXT", new KafkaSpanContextSerializer().serialize(kafkaSpanContext));

    return span;
  }
}
