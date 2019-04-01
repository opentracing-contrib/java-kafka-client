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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockSpan.MockContext;
import io.opentracing.mock.MockTracer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Before;
import org.junit.Test;


public class TracingKafkaUtilsTest {

  private MockTracer mockTracer = new MockTracer();

  @Before
  public void before() {
    mockTracer.reset();
  }

  @Test
  public void inject() {
    MockSpan span = mockTracer.buildSpan("test").start();
    Headers headers = new RecordHeaders();
    assertEquals(0, headers.toArray().length);

    TracingKafkaUtils.inject(span.context(), headers, mockTracer);

    assertTrue(headers.toArray().length > 0);
  }

  @Test
  public void extract() {
    MockSpan span = mockTracer.buildSpan("test").start();
    Headers headers = new RecordHeaders();
    TracingKafkaUtils.inject(span.context(), headers, mockTracer);

    MockSpan.MockContext spanContext = (MockSpan.MockContext) TracingKafkaUtils
        .extract(headers, mockTracer);

    assertEquals(span.context().spanId(), spanContext.spanId());
    assertEquals(span.context().traceId(), spanContext.traceId());
  }

  @Test
  public void extract_no_context() {
    Headers headers = new RecordHeaders();

    // first
    MockSpan.MockContext spanContext = (MockSpan.MockContext) TracingKafkaUtils
        .extract(headers, mockTracer);
    assertNull(spanContext);

    // second
    MockSpan.MockContext spanContext2 = (MockContext) TracingKafkaUtils
        .extractSpanContext(headers, mockTracer);
    assertNull(spanContext2);
  }

  @Test
  public void extract_second_no_context() {
    MockSpan span = mockTracer.buildSpan("first").start();
    Headers headers = new RecordHeaders();
    assertEquals(0, headers.toArray().length);

    // inject first
    TracingKafkaUtils.inject(span.context(), headers, mockTracer);
    int headersLength = headers.toArray().length;
    assertTrue(headersLength > 0);

    // check second
    MockSpan.MockContext spanContext2 = (MockContext) TracingKafkaUtils
        .extractSpanContext(headers, mockTracer);
    assertNull(spanContext2);
  }

  @Test
  public void inject_and_extract_two_contexts() {
    MockSpan span = mockTracer.buildSpan("first").start();
    Headers headers = new RecordHeaders();
    assertEquals(0, headers.toArray().length);

    // inject first
    TracingKafkaUtils.inject(span.context(), headers, mockTracer);
    int headersLength = headers.toArray().length;
    assertTrue(headersLength > 0);

    // inject second
    MockSpan span2 = mockTracer.buildSpan("second").asChildOf(span.context()).start();
    TracingKafkaUtils.injectSecond(span2.context(), headers, mockTracer);
    assertTrue(headers.toArray().length > headersLength);

    // check first
    MockSpan.MockContext spanContext = (MockSpan.MockContext) TracingKafkaUtils
        .extract(headers, mockTracer);
    assertEquals(span.context().spanId(), spanContext.spanId());
    assertEquals(span.context().traceId(), spanContext.traceId());

    // check second
    MockSpan.MockContext spanContext2 = (MockContext) TracingKafkaUtils
        .extractSpanContext(headers, mockTracer);
    assertEquals(span2.context().spanId(), spanContext2.spanId());
    assertEquals(span2.context().traceId(), spanContext2.traceId());
    assertEquals(spanContext.traceId(), spanContext2.traceId());
    assertNotEquals(spanContext.spanId(), spanContext2.spanId());
  }
}