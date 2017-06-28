package io.opentracing.contrib.kafka;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.ThreadLocalActiveSpanSource;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Before;
import org.junit.Test;


public class TracingKafkaUtilsTest {

  private MockTracer mockTracer = new MockTracer(new ThreadLocalActiveSpanSource(),
      MockTracer.Propagator.TEXT_MAP);

  @Before
  public void before() {
    mockTracer.reset();
  }

  @Test
  public void inject() {
    MockSpan span = mockTracer.buildSpan("test").start();
    Headers headers = new RecordHeaders();
    assertTrue(headers.toArray().length == 0);

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
  }
}