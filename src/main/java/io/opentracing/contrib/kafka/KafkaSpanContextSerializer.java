package io.opentracing.contrib.kafka;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ExtendedSerializer;
import org.apache.kafka.common.serialization.Serializer;


public class KafkaSpanContextSerializer {


  public byte[] serialize() {
    return new byte[0];
  }

  public byte[] serialize(KafkaSpanContext data) {
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(byteOut)) {
      out.writeObject(data.getMap());
    } catch (IOException e) {
      throw new KafkaException(e);
    }

    return byteOut.toByteArray();
  }
}
