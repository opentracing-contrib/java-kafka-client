package io.opentracing.contrib.kafka;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import java.nio.ByteBuffer;
import org.junit.Before;
import org.junit.Test;


public class TracingKafkaUtilsTest {

  private MockTracer mockTracer = new MockTracer(MockTracer.Propagator.TEXT_MAP);

  @Before
  public void before() {
    mockTracer.reset();
  }

  @Test
  public void getInstance() {
    String string = TracingKafkaUtils.getInstance(String.class.getName(), String.class);
    assertNotNull(string);
  }

  @Test
  public void inject() {
    MockSpan span = mockTracer.buildSpan("test").start();
    KafkaSpanContext kafkaSpanContext = new KafkaSpanContext();
    assertTrue(kafkaSpanContext.getMap().isEmpty());

    TracingKafkaUtils.inject(span.context(), kafkaSpanContext, mockTracer);
    assertFalse(kafkaSpanContext.getMap().isEmpty());
  }

  @Test
  public void extract() {
    MockSpan span = mockTracer.buildSpan("test").start();
    KafkaSpanContext kafkaSpanContext = new KafkaSpanContext();
    TracingKafkaUtils.inject(span.context(), kafkaSpanContext, mockTracer);

    MockSpan.MockContext spanContext = (MockSpan.MockContext) TracingKafkaUtils
        .extract(kafkaSpanContext, mockTracer);

    assertEquals(span.context().spanId(), spanContext.spanId());
  }

  @Test
  public void getMapLength() {
    byte[] data = ByteBuffer.allocate(100).putInt(123).array();
    assertEquals(123, TracingKafkaUtils.getMapLength(data));
  }
}