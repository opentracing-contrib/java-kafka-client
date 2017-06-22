package io.opentracing.contrib.kafka;


import java.util.HashMap;
import java.util.Map;

public class KafkaSpanContext {

  private final Map<String, String> map = new HashMap<>();

  public KafkaSpanContext() {
  }

  public Map<String, String> getMap() {
    return map;
  }
}
