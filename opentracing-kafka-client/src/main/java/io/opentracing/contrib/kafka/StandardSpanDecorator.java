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

import io.opentracing.Span;
import io.opentracing.tag.Tags;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

class StandardSpanDecorator implements SpanDecorator {

  static final String COMPONENT_NAME = "java-kafka";
  static final String KAFKA_SERVICE = "kafka";

  public <K, V> void onSend(ProducerRecord<K, V> record, Span span) {
    setCommonTags(span);
    Tags.MESSAGE_BUS_DESTINATION.set(span, record.topic());
    if (record.partition() != null) {
      span.setTag("partition", record.partition());
    }
  }

  public <K, V> void onResponse(ConsumerRecord<K, V> record, Span span) {
    setCommonTags(span);
    Tags.MESSAGE_BUS_DESTINATION.set(span, record.topic());
    span.setTag("partition", record.partition());
    span.setTag("offset", record.offset());
  }

  public void onError(Exception exception, Span span) {
    Tags.ERROR.set(span, Boolean.TRUE);
    span.log(errorLogs(exception));
  }

  private static Map<String, Object> errorLogs(Throwable throwable) {
    Map<String, Object> errorLogs = new HashMap<>(4);
    errorLogs.put("event", Tags.ERROR.getKey());
    errorLogs.put("error.kind", throwable.getClass().getName());
    errorLogs.put("error.object", throwable);
    errorLogs.put("message", throwable.getMessage());

    StringWriter sw = new StringWriter();
    throwable.printStackTrace(new PrintWriter(sw));
    errorLogs.put("stack", sw.toString());

    return errorLogs;
  }

  private static void setCommonTags(Span span) {
    Tags.COMPONENT.set(span, COMPONENT_NAME);
    Tags.PEER_SERVICE.set(span, KAFKA_SERVICE);
  }
}
