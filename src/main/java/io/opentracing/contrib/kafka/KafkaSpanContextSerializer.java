package io.opentracing.contrib.kafka;

import java.io.*;
import org.apache.kafka.common.KafkaException;

public class KafkaSpanContextSerializer {

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
