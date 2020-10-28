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
package io.opentracing.contrib.kafka.spring;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {TestConfiguration.class})
public class TracingSpringKafkaTest {

  @ClassRule
  public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(2, true, 2, "spring");

  @Autowired
  private MockTracer mockTracer;

  @Autowired
  private KafkaTemplate<Integer, String> kafkaTemplate;

  @Before
  public void before() {
    mockTracer.reset();
  }

  @Test
  public void test() {
    kafkaTemplate.send("spring", "message");

    await().atMost(15, TimeUnit.SECONDS).until(reportedSpansSize(), greaterThanOrEqualTo(3));

    List<MockSpan> spans = mockTracer.finishedSpans();
    assertThat(spans, contains(
        new SpanMatcher("To_spring"),
        new SpanMatcher("From_spring"),
        new SpanMatcher("KafkaListener_spring")));
  }

  private Callable<Integer> reportedSpansSize() {
    return () -> mockTracer.finishedSpans().size();
  }

  private static class SpanMatcher extends BaseMatcher<MockSpan> {

    private final String operationName;

    private SpanMatcher(String operationName) {
      this.operationName = operationName;
    }

    @Override
    public boolean matches(Object actual) {
      return actual instanceof MockSpan && operationName
          .equals(((MockSpan) actual).operationName());
    }

    @Override
    public void describeTo(Description description) {
      description.appendText(operationName);
    }
  }
}
