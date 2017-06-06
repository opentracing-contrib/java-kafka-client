package io.opentracing.contrib.kafka;

import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;


public class TracingPartitioner implements Partitioner {

  /**
   * Partitioner is protected for overriding in constructor of subclasses
   */
  protected Partitioner partitioner = new DefaultPartitioner();

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
      Cluster cluster) {

    if (key instanceof KafkaSpanContext) {
      KafkaSpanContext kafkaSpanContext = (KafkaSpanContext) key;
      int mapLength = TracingKafkaUtils.getMapLength(keyBytes);
      byte[] keyBytesWithoutMap = Arrays.copyOfRange(keyBytes, mapLength + 4, keyBytes.length);

      return partitioner
          .partition(topic, kafkaSpanContext.getKey(), keyBytesWithoutMap, value, valueBytes,
              cluster);
    }

    return partitioner.partition(topic, key, keyBytes, value, valueBytes, cluster);
  }

  @Override
  public void close() {
  }

  @Override
  public void configure(Map<String, ?> configs) {

  }
}
