package io.opentracing.contrib.kafka;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Deserializer;

public class KafkaSpanContextDeserializer<K> implements Deserializer<KafkaSpanContext<K>> {

  private final Deserializer<K> keyDeserializer;

  public KafkaSpanContextDeserializer(Deserializer<K> keyDeserializer) {
    this.keyDeserializer = keyDeserializer;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  @SuppressWarnings("unchecked")
  public KafkaSpanContext<K> deserialize(String topic, byte[] data) {
    int mapLength = TracingKafkaUtils.getMapLength(data);
    byte[] serializedMap = Arrays.copyOfRange(data, 4, mapLength + 4);
    byte[] serializedKey = Arrays.copyOfRange(data, mapLength + 4, data.length);

    K key = null;
    if (serializedKey.length > 0) {
      key = keyDeserializer.deserialize(topic, serializedKey);
    }

    KafkaSpanContext<K> kafkaSpanContext = new KafkaSpanContext<>(key);
    ByteArrayInputStream byteIn = new ByteArrayInputStream(serializedMap);
    try (ObjectInputStream in = new ObjectInputStream(byteIn)) {
      Map<String, String> map = (Map<String, String>) in.readObject();
      kafkaSpanContext.getMap().putAll(map);
    } catch (IOException | ClassNotFoundException e) {
      throw new KafkaException(e);
    }

    return kafkaSpanContext;
  }

  @Override
  public void close() {
  }
}
