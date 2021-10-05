/*
 * Copyright 2017-2020 The OpenTracing Authors
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


import static io.opentracing.contrib.kafka.SpanDecorator.STANDARD_TAGS;

import io.opentracing.Tracer;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class TracingKafkaConsumer<K, V> implements Consumer<K, V> {

  private final Tracer tracer;
  private final Consumer<K, V> consumer;
  private Collection<SpanDecorator> spanDecorators;
  private final BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider;

  TracingKafkaConsumer(Consumer<K, V> consumer, Tracer tracer,
      Collection<SpanDecorator> spanDecorators,
      BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider) {
    this.consumer = consumer;
    this.tracer = tracer;
    this.spanDecorators = Collections.unmodifiableCollection(spanDecorators);
    this.consumerSpanNameProvider = (consumerSpanNameProvider == null)
        ? ClientSpanNameProvider.CONSUMER_OPERATION_NAME
        : consumerSpanNameProvider;
  }

  public TracingKafkaConsumer(Consumer<K, V> consumer, Tracer tracer) {
    this.consumer = consumer;
    this.tracer = tracer;
    this.spanDecorators = Collections.singletonList(STANDARD_TAGS);
    this.consumerSpanNameProvider = ClientSpanNameProvider.CONSUMER_OPERATION_NAME;
  }

  public TracingKafkaConsumer(Consumer<K, V> consumer, Tracer tracer,
      BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider) {
    this.consumer = consumer;
    this.tracer = tracer;
    this.spanDecorators = Collections.singletonList(STANDARD_TAGS);
    this.consumerSpanNameProvider = (consumerSpanNameProvider == null)
        ? ClientSpanNameProvider.CONSUMER_OPERATION_NAME
        : consumerSpanNameProvider;
  }

  @Override
  public Set<TopicPartition> assignment() {
    return consumer.assignment();
  }

  @Override
  public Set<String> subscription() {
    return consumer.subscription();
  }

  @Override
  public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
    consumer.subscribe(topics, listener);
  }

  @Override
  public void subscribe(Collection<String> topics) {
    consumer.subscribe(topics);
  }

  @Override
  public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
    consumer.subscribe(pattern, listener);
  }

  @Override
  public void subscribe(Pattern pattern) {
    consumer.subscribe(pattern);
  }

  @Override
  public void unsubscribe() {
    consumer.unsubscribe();
  }

  @Override
  public void assign(Collection<TopicPartition> partitions) {
    consumer.assign(partitions);
  }

  @Override
  @Deprecated
  public ConsumerRecords<K, V> poll(long timeout) {
    ConsumerRecords<K, V> records = consumer.poll(timeout);

    for (ConsumerRecord<K, V> record : records) {
      TracingKafkaUtils
          .buildAndFinishChildSpan(record, tracer, consumerSpanNameProvider, spanDecorators);
    }

    return records;
  }

  @Override
  public ConsumerRecords<K, V> poll(Duration duration) {
    ConsumerRecords<K, V> records = consumer.poll(duration);

    for (ConsumerRecord<K, V> record : records) {
      TracingKafkaUtils
          .buildAndFinishChildSpan(record, tracer, consumerSpanNameProvider, spanDecorators);
    }

    return records;
  }

  @Override
  public void commitSync() {
    consumer.commitSync();
  }

  @Override
  public void commitSync(Duration duration) {
    consumer.commitSync(duration);
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    consumer.commitSync(offsets);
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> map, Duration duration) {
    consumer.commitSync(map, duration);
  }

  @Override
  public void commitAsync() {
    consumer.commitAsync();
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    consumer.commitAsync(callback);
  }

  @Override
  public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets,
      OffsetCommitCallback callback) {
    consumer.commitAsync(offsets, callback);
  }

  @Override
  public void seek(TopicPartition partition, long offset) {
    consumer.seek(partition, offset);
  }

  @Override
  public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
    consumer.seek(partition, offsetAndMetadata);
  }

  @Override
  public void seekToBeginning(Collection<TopicPartition> partitions) {
    consumer.seekToBeginning(partitions);
  }

  @Override
  public void seekToEnd(Collection<TopicPartition> partitions) {
    consumer.seekToEnd(partitions);
  }

  @Override
  public long position(TopicPartition partition) {
    return consumer.position(partition);
  }

  @Override
  public long position(TopicPartition topicPartition, Duration duration) {
    return consumer.position(topicPartition, duration);
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition) {
    return consumer.committed(partition);
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition topicPartition, Duration duration) {
    return consumer.committed(topicPartition, duration);
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
    return consumer.committed(partitions);
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions,
      final Duration timeout) {
    return consumer.committed(partitions, timeout);
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return consumer.metrics();
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return consumer.partitionsFor(topic);
  }

  @Override
  public List<PartitionInfo> partitionsFor(String s, Duration duration) {
    return consumer.partitionsFor(s, duration);
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    return consumer.listTopics();
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics(Duration duration) {
    return consumer.listTopics(duration);
  }

  @Override
  public void pause(Collection<TopicPartition> partitions) {
    consumer.pause(partitions);
  }

  @Override
  public void resume(Collection<TopicPartition> partitions) {
    consumer.resume(partitions);
  }

  @Override
  public Set<TopicPartition> paused() {
    return consumer.paused();
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
      Map<TopicPartition, Long> timestampsToSearch) {
    return consumer.offsetsForTimes(timestampsToSearch);
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> map,
      Duration duration) {
    return consumer.offsetsForTimes(map, duration);
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    return consumer.beginningOffsets(partitions);
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> collection,
      Duration duration) {
    return consumer.beginningOffsets(collection, duration);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    return consumer.endOffsets(partitions);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> collection,
      Duration duration) {
    return consumer.endOffsets(collection, duration);
  }

  @Override
  public OptionalLong currentLag(TopicPartition topicPartition) {
    return consumer.currentLag(topicPartition);
  }

  @Override
  public ConsumerGroupMetadata groupMetadata() {
    return consumer.groupMetadata();
  }

  @Override
  public void enforceRebalance() {
    consumer.enforceRebalance();
  }

  @Override
  public void close() {
    consumer.close();
  }

  @Override
  public void close(Duration duration) {
    consumer.close(duration);
  }

  @Override
  public void wakeup() {
    consumer.wakeup();
  }
}
