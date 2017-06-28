package io.opentracing.contrib.kafka;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.opentracing.ActiveSpan;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.ThreadLocalActiveSpanSource;
import java.util.List;
import org.junit.Before;
import org.junit.Test;


public class TracingCallbackTest {

  private MockTracer mockTracer = new MockTracer(new ThreadLocalActiveSpanSource(),
      MockTracer.Propagator.TEXT_MAP);

  @Before
  public void before() {
    mockTracer.reset();
  }

  @Test
  public void onCompletionWithError() throws Exception {
    ActiveSpan span = mockTracer.buildSpan("test").startActive();

    TracingCallback callback = new TracingCallback(null, span.capture());
    callback.onCompletion(null, new RuntimeException("test"));

    span.deactivate();

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());
    assertEquals(1, finished.get(0).logEntries().size());
    assertEquals(true, finished.get(0).tags().get(Tags.ERROR.getKey()));
  }

  @Test
  public void onCompletion() throws Exception {
    ActiveSpan span = mockTracer.buildSpan("test").startActive();

    TracingCallback callback = new TracingCallback(null, span.capture());
    callback.onCompletion(null, null);

    span.deactivate();

    List<MockSpan> finished = mockTracer.finishedSpans();
    assertEquals(1, finished.size());
    assertEquals(0, finished.get(0).logEntries().size());
    assertNull(finished.get(0).tags().get(Tags.ERROR.getKey()));
  }
}