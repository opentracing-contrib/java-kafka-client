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


import io.opentracing.Scope;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TracingKafkaProducer<K, V> implements Producer<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(TracingKafkaProducer.class);

  private Producer<K, V> producer;
  private final Tracer tracer;

  public TracingKafkaProducer(Producer<K, V> producer, Tracer tracer) {
    this.producer = producer;
    this.tracer = tracer;
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
  public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> map, String s)
      throws ProducerFencedException {
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
    /*
    // Create wrappedRecord because headers can be read only in record (if record is sent second time)
    ProducerRecord<K, V> wrappedRecord = new ProducerRecord<>(record.topic(),
        record.partition(),
        record.timestamp(),
        record.key(),
        record.value(),
        record.headers());
    */

    try (Scope scope = buildAndInjectSpan(record)) {
      Callback wrappedCallback = new TracingCallback(callback, scope);
      return producer.send(record, wrappedCallback);
    }
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

  private Scope buildAndInjectSpan(ProducerRecord<K, V> record) {
    Tracer.SpanBuilder spanBuilder = tracer.buildSpan("send")
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT);

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
}
