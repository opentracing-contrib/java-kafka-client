package io.opentracing.contrib.kafka;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class KafkaSpanContextSerializerTest {

  @Test
  public void serializeAndDeserialize() {
    KafkaSpanContextSerializer serializer = new KafkaSpanContextSerializer();

    KafkaSpanContextDeserializer deserializer = new KafkaSpanContextDeserializer();

    KafkaSpanContext context = new KafkaSpanContext();
    context.getMap().put("one", "two");

    byte[] serialized = serializer.serialize(context);
    KafkaSpanContext deserializedContext = deserializer.deserialize(serialized);

    assertEquals("two", deserializedContext.getMap().get("one"));
  }

}