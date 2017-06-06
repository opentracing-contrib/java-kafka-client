package io.opentracing.contrib.kafka;


import java.util.HashMap;
import java.util.Map;

public class KafkaSpanContext<K> {

  private final Map<String, String> map = new HashMap<>();
  private K key;

  public KafkaSpanContext(K key) {
    this.key = key;
  }

  public K getKey() {
    return key;
  }

  public Map<String, String> getMap() {
    return map;
  }
}
