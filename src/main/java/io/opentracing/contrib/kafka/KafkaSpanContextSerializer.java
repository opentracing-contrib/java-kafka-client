package io.opentracing.contrib.kafka;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Serializer;


public class KafkaSpanContextSerializer<K> implements Serializer<KafkaSpanContext<K>> {

  private final Serializer<K> keySerializer;

  public KafkaSpanContextSerializer(Serializer<K> keySerializer) {
    this.keySerializer = keySerializer;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public byte[] serialize(String topic, KafkaSpanContext<K> data) {
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(byteOut)) {
      out.writeObject(data.getMap());
    } catch (IOException e) {
      throw new KafkaException(e);
    }

    byte[] serializedMap = byteOut.toByteArray();
    byte[] serializedKey;
    if (keySerializer == null || data.getKey() == null) {
      serializedKey = new byte[0];
    } else {
      serializedKey = keySerializer.serialize(topic, data.getKey());
    }
    ByteBuffer byteBuffer = ByteBuffer.allocate(4 + serializedMap.length + serializedKey.length);
    byteBuffer.putInt(serializedMap.length);
    byteBuffer.put(serializedMap);
    byteBuffer.put(serializedKey);
    return byteBuffer.array();
  }

  @Override
  public void close() {
  }
}
