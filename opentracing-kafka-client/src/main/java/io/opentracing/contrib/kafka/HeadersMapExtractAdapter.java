/*
 * Copyright 2017 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
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

  HeadersMapExtractAdapter(Headers headers, boolean second) {
    for (Header header : headers) {
      if (second) {
        if (header.key().startsWith("second_span_")) {
          map.put(header.key().replaceFirst("^second_span_", ""),
              new String(header.value(), StandardCharsets.UTF_8));
        }
      } else {
        map.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
      }
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
