package io.opentracing.contrib.kafka;


import io.opentracing.ActiveSpan;
import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

public class TracingKafkaConsumer<K, V> implements Consumer<K, V> {

  private final Tracer tracer;
  private final KafkaConsumer<KafkaSpanContext<K>, V> consumer;


  @SuppressWarnings("unchecked")
  public TracingKafkaConsumer(Map<String, Object> configs, Tracer tracer) {
    this.tracer = tracer;
    Object keyDeserializerValue = configs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
    Deserializer<K> keyDeserializer = TracingKafkaUtils
        .getInstance(keyDeserializerValue, Deserializer.class);

    this.consumer = new KafkaConsumer<>(configs,
        new KafkaSpanContextDeserializer<>(keyDeserializer), null);
  }

  public TracingKafkaConsumer(Map<String, Object> configs, Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer, Tracer tracer) {
    this.tracer = tracer;
    this.consumer = new KafkaConsumer<>(configs,
        new KafkaSpanContextDeserializer<>(keyDeserializer), valueDeserializer);
  }

  @SuppressWarnings("unchecked")
  public TracingKafkaConsumer(Properties properties, Tracer tracer) {
    this.tracer = tracer;
    Object keyDeserializerValue = properties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
    Deserializer<K> keyDeserializer = TracingKafkaUtils
        .getInstance(keyDeserializerValue, Deserializer.class);

    this.consumer = new KafkaConsumer<>(properties,
        new KafkaSpanContextDeserializer<>(keyDeserializer), null);
  }

  public TracingKafkaConsumer(Properties properties, Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer, Tracer tracer) {
    this.tracer = tracer;
    this.consumer = new KafkaConsumer<>(properties,
        new KafkaSpanContextDeserializer<>(keyDeserializer), valueDeserializer);
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
  public void unsubscribe() {
    consumer.unsubscribe();
  }

  @Override
  public void assign(Collection<TopicPartition> partitions) {
    consumer.assign(partitions);
  }

  @Override
  public ConsumerRecords<K, V> poll(long timeout) {
    ConsumerRecords<KafkaSpanContext<K>, V> wrappedRecords = consumer.poll(timeout);

    for (ConsumerRecord<KafkaSpanContext<K>, V> wrappedRecord : wrappedRecords) {
      buildAndFinishChildSpan(wrappedRecord.key());
    }

    Map<TopicPartition, List<ConsumerRecord<K, V>>> records = new HashMap<>();

    for (TopicPartition topicPartition : wrappedRecords.partitions()) {
      List<ConsumerRecord<KafkaSpanContext<K>, V>> recordsList = wrappedRecords
          .records(topicPartition);

      List<ConsumerRecord<K, V>> list = new ArrayList<>();

      for (ConsumerRecord<KafkaSpanContext<K>, V> record : recordsList) {
        ConsumerRecord<K, V> consumerRecord = new ConsumerRecord<>(record.topic(),
            record.partition(),
            record.offset(), record.timestamp(), record.timestampType(), record.checksum(),
            record.serializedKeySize(), record.serializedValueSize(), record.key().getKey(),
            record.value());
        list.add(consumerRecord);
      }

      records.put(topicPartition, list);

    }

    return new ConsumerRecords<>(records);
  }

  @Override
  public void commitSync() {
    consumer.commitSync();
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    consumer.commitSync(offsets);
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
  public OffsetAndMetadata committed(TopicPartition partition) {
    return consumer.committed(partition);
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
  public Map<String, List<PartitionInfo>> listTopics() {
    return consumer.listTopics();
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
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    return consumer.beginningOffsets(partitions);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    return consumer.endOffsets(partitions);
  }

  @Override
  public void close() {
    consumer.close();
  }

  @Override
  public void close(long l, TimeUnit timeUnit) {
    consumer.close(l, timeUnit);
  }

  @Override
  public void wakeup() {
    consumer.wakeup();
  }

  private void buildAndFinishChildSpan(KafkaSpanContext kafkaSpanContext) {
    SpanContext parentContext = extract(kafkaSpanContext);

    if (parentContext != null) {

      Tracer.SpanBuilder spanBuilder = tracer.buildSpan("receive").ignoreActiveSpan()
          .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT);

      spanBuilder.addReference(References.FOLLOWS_FROM, parentContext);

      Span span = spanBuilder.startManual();
      SpanDecorator.onResponse(span);
      span.finish();
    }
  }

  private SpanContext extract(KafkaSpanContext kafkaSpanContext) {
    SpanContext spanContext = TracingKafkaUtils.extract(kafkaSpanContext, tracer);
    if (spanContext != null) {
      return spanContext;
    }

    ActiveSpan span = tracer.activeSpan();
    if (span != null) {
      return span.context();
    }
    return null;
  }
}
