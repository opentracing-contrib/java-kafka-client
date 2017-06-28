package io.opentracing.contrib.kafka;

import io.opentracing.propagation.TextMap;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;


public class HeadersMapExtractAdapter implements TextMap {

  private final Map<String, String> map = new HashMap<>();

  HeadersMapExtractAdapter(Headers headers) {
    for (Header header : headers) {
      map.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
    }
  }

  @Override
  public Iterator<Entry<String, String>> iterator() {
    return map.entrySet().iterator();
  }

  @Override
  public void put(String key, String value) {
    throw new UnsupportedOperationException(
        "HeadersMapExtractAdapter should only be used with Tracer.extract()");
  }
}
