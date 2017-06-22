package io.opentracing.contrib.kafka;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.ExtendedDeserializer;

public class KafkaSpanContextDeserializer {

  @SuppressWarnings("unchecked")
  public KafkaSpanContext deserialize() {
    return new KafkaSpanContext();
  }

  @SuppressWarnings("unchecked")
  public KafkaSpanContext deserialize(Headers headers) {
    Iterator<Header> iterator = headers.headers("TRACING_SPAN_CONTEXT").iterator();
    if(iterator.hasNext()){
      Header spanContextHeader = iterator.next();
      byte[] spanContextData = spanContextHeader.value();

      /*int mapLength = TracingKafkaUtils.getMapLength(spanContextData);
      byte[] serializedMap = Arrays.copyOfRange(spanContextData, 4, mapLength + 4);*/

      KafkaSpanContext kafkaSpanContext = new KafkaSpanContext();
      ByteArrayInputStream byteIn = new ByteArrayInputStream(spanContextData);
      try (ObjectInputStream in = new ObjectInputStream(byteIn)) {
        Map<String, String> map = (Map<String, String>) in.readObject();
        kafkaSpanContext.getMap().putAll(map);
      } catch (IOException | ClassNotFoundException e) {
        throw new KafkaException(e);
      }

      return kafkaSpanContext;
    } else {
      return new KafkaSpanContext();
    }
  }

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
