package io.opentracing.contrib.kafka;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;
import org.apache.kafka.common.KafkaException;

public class KafkaSpanContextDeserializer {

  @SuppressWarnings("unchecked")
  public KafkaSpanContext deserialize(byte[] serializedMap) {
      KafkaSpanContext kafkaSpanContext = new KafkaSpanContext();
      ByteArrayInputStream byteIn = new ByteArrayInputStream(serializedMap);
      try (ObjectInputStream in = new ObjectInputStream(byteIn)) {
        Map<String, String> map = (Map<String, String>) in.readObject();
        kafkaSpanContext.getMap().putAll(map);
      } catch (IOException | ClassNotFoundException e) {
        throw new KafkaException(e);
      }

      return kafkaSpanContext;
  }
}
