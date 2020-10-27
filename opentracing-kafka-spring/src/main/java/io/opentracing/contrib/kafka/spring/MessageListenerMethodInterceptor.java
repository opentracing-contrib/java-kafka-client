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

import io.opentracing.References;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaUtils;
import io.opentracing.tag.Tags;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.kafka.clients.consumer.ConsumerRecord;

class MessageListenerMethodInterceptor implements MethodInterceptor {

  private static final String SPAN_PREFIX = "KafkaListener_";

  private final Tracer tracer;

  MessageListenerMethodInterceptor(Tracer tracer) {
    this.tracer = tracer;
  }

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    if (!"onMessage".equals(invocation.getMethod().getName())) {
      return invocation.proceed();
    }
    Object[] arguments = invocation.getArguments();
    ConsumerRecord<?, ?> record = getConsumerRecord(arguments);
    if (record == null) {
      return invocation.proceed();
    }

    Tracer.SpanBuilder spanBuilder = tracer.buildSpan(SPAN_PREFIX + record.topic())
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER);

    SpanContext parentContext = TracingKafkaUtils.extractSpanContext(record.headers(), tracer);
    if (parentContext != null) {
      spanBuilder.addReference(References.FOLLOWS_FROM, parentContext);
    }
    Span span = spanBuilder.start();
    try (Scope ignored = tracer.activateSpan(span)) {
      return invocation.proceed();
    } catch (Exception e) {
      Tags.ERROR.set(span, Boolean.TRUE);
      throw e;
    } finally {
      span.finish();
    }
  }

  private ConsumerRecord<?, ?> getConsumerRecord(Object[] arguments) {
    for (Object object : arguments) {
      if (object instanceof ConsumerRecord) {
        return (ConsumerRecord<?, ?>) object;
      }
    }
    return null;
  }

}
