/*
 * Copyright 2017-2020 The OpenTracing Authors
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
import static org.junit.Assert.assertNull;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;


public class TracingCallbackTest {

  private MockTracer mockTracer = new MockTracer();

  @Before
  public void before() {
    mockTracer.reset();
  }

  @Test
  public void onCompletionWithError() {
    Span span = mockTracer.buildSpan("test").start();
    try (Scope ignored = mockTracer.activateSpan(span)) {
      TracingCallback callback = new TracingCallback(null, span, mockTracer);
      callback.onCompletion(null, new RuntimeException("test"));
    }

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());
    assertEquals(1, finished.get(0).logEntries().size());
    assertEquals(true, finished.get(0).tags().get(Tags.ERROR.getKey()));
  }

  @Test
  public void onCompletionWithCustomErrorDecorators() {
    Span span = mockTracer.buildSpan("test").start();
    try (Scope ignored = mockTracer.activateSpan(span)) {
      TracingCallback callback = new TracingCallback(null, span, mockTracer,
          Arrays.asList(SpanDecorator.STANDARD_TAGS, createDecorator()));
      callback.onCompletion(null, new RuntimeException("test"));
    }

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());
    assertEquals(true, finished.get(0).tags().get(Tags.ERROR.getKey()));
    assertEquals("overwritten", finished.get(0).tags().get("error.of"));
    assertEquals("error-test", finished.get(0).tags().get("new.error.tag"));
  }

  @Test
  public void onCompletion() {
    Span span = mockTracer.buildSpan("test").start();
    try (Scope ignored = mockTracer.activateSpan(span)) {
      TracingCallback callback = new TracingCallback(null, span, mockTracer);
      callback.onCompletion(null, null);
    }

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());
    assertEquals(0, finished.get(0).logEntries().size());
    assertNull(finished.get(0).tags().get(Tags.ERROR.getKey()));
  }

  private SpanDecorator createDecorator() {
    return new SpanDecorator() {
      @Override
      public <K, V> void onSend(ProducerRecord<K, V> record, Span span) {
      }

      @Override
      public <K, V> void onResponse(ConsumerRecord<K, V> record, Span span) {
      }

      @Override
      public <K, V> void onError(Exception exception, Span span) {
        span.setTag("error.of", "overwritten");
        span.setTag("new.error.tag", "error-test");
      }
    };
  }
}