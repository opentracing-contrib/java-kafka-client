package io.opentracing.contrib.kafka;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;


public class KafkaSpanContextSerializerTest {

  @Test
  public void serializeAndDeserialize() {
    StringSerializer stringSerializer = new StringSerializer();
    KafkaSpanContextSerializer<String> serializer = new KafkaSpanContextSerializer<>(
        stringSerializer);

    StringDeserializer stringDeserializer = new StringDeserializer();
    KafkaSpanContextDeserializer<String> deserializer = new KafkaSpanContextDeserializer<>(
        stringDeserializer);

    KafkaSpanContext<String> context = new KafkaSpanContext<>("key");
    context.getMap().put("one", "two");

    byte[] serialized = serializer.serialize("topic", context);
    KafkaSpanContext<String> deserializedContext = deserializer.deserialize("topic", serialized);

    assertEquals("key", deserializedContext.getKey());
    assertEquals("two", deserializedContext.getMap().get("one"));
  }


  @Test
  public void withNullSerializer() {
    KafkaSpanContextSerializer<String> serializer = new KafkaSpanContextSerializer<>(null);
    KafkaSpanContextDeserializer<String> deserializer = new KafkaSpanContextDeserializer<>(null);

    KafkaSpanContext<String> context = new KafkaSpanContext<>("key");
    context.getMap().put("one", "two");

    byte[] serialized = serializer.serialize("topic", context);
    KafkaSpanContext<String> deserializedContext = deserializer.deserialize("topic", serialized);

    assertNull(deserializedContext.getKey());
    assertEquals("two", deserializedContext.getMap().get("one"));
  }

  @Test
  public void withNullKey() {
    StringSerializer stringSerializer = new StringSerializer();
    KafkaSpanContextSerializer<String> serializer = new KafkaSpanContextSerializer<>(
        stringSerializer);

    StringDeserializer stringDeserializer = new StringDeserializer();
    KafkaSpanContextDeserializer<String> deserializer = new KafkaSpanContextDeserializer<>(
        stringDeserializer);

    KafkaSpanContext<String> context = new KafkaSpanContext<>(null);
    context.getMap().put("one", "two");

    byte[] serialized = serializer.serialize("topic", context);
    KafkaSpanContext<String> deserializedContext = deserializer.deserialize("topic", serialized);

    assertNull(deserializedContext.getKey());
    assertEquals("two", deserializedContext.getMap().get("one"));
  }
}