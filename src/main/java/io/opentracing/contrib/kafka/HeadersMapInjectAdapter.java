package io.opentracing.contrib.kafka;


import io.opentracing.propagation.TextMap;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.kafka.common.header.Headers;

public class HeadersMapInjectAdapter implements TextMap {

  private final Headers headers;
  private final boolean second;

  HeadersMapInjectAdapter(Headers headers, boolean second) {
    this.headers = headers;
    this.second = second;
  }

  @Override
  public Iterator<Entry<String, String>> iterator() {
    throw new UnsupportedOperationException("iterator should never be used with Tracer.inject()");
  }

  @Override
  public void put(String key, String value) {
    if (second) {
      headers.add("second_span_" + key, value.getBytes(StandardCharsets.UTF_8));
    } else {
      headers.add(key, value.getBytes(StandardCharsets.UTF_8));
    }
  }
}
