/*
 * Copyright 2017-2019 The OpenTracing Authors
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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

import java.util.Map.Entry;

import static org.junit.Assert.*;


public class HeadersMapExtractAdapterTest {

  @Test
  public void verifyNullHeaderHandled() {
    Headers headers = new RecordHeaders();
    headers.add("test_null_header", null);
    HeadersMapExtractAdapter headersMapExtractAdapter = new HeadersMapExtractAdapter(headers, false);
    Entry<String, String> header = headersMapExtractAdapter.iterator().next();
    assertNotNull(header);
    assertEquals(header.getKey(), "test_null_header");
    assertNull(header.getValue());

  }
}
