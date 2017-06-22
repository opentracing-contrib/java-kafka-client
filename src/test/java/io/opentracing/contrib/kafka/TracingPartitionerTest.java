package io.opentracing.contrib.kafka;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;


public class TracingPartitionerTest {

  @Test
  public void notNullKey() {
    partition("key");
  }

  @Test
  public void nullKey() {
    partition(null);
  }

  private void partition(String key) {
    TracingPartitioner partitioner = new TracingPartitioner();
    StringSerializer stringSerializer = new StringSerializer();
    KafkaSpanContextSerializer serializer = new KafkaSpanContextSerializer();
    KafkaSpanContext context = new KafkaSpanContext();
    context.getMap().put("one", "two");

    byte[] serialized = serializer.serialize(context);

    Map<TopicPartition, PartitionInfo> partitions = new HashMap<>();
    partitions.put(new TopicPartition("topic", 1), new PartitionInfo("topic", 1, null, null, null));
    partitions.put(new TopicPartition("topic", 2), new PartitionInfo("topic", 2, null, null, null));
    Cluster cluster = Cluster.empty().withPartitions(partitions);

    int partition = partitioner
        .partition("topic", context, serialized, "value", "value".getBytes(), cluster);

    int defaultPartition = new DefaultPartitioner()
        .partition("topic", key, stringSerializer.serialize("topic", "key"),
            "value", "value".getBytes(), cluster);

    assertEquals(defaultPartition, partition);
  }

}