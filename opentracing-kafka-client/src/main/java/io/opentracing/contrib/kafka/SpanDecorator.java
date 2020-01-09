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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface SpanDecorator {

  /**
   * Method called before record is sent by producer
   */
  <K, V> void onSend(ProducerRecord<K, V> record, Span span);

  /**
   * Method called when record is received in consumer
   */
  <K, V> void onResponse(ConsumerRecord<K, V> record, Span span);

  /**
   * Method called when an error occurs
   */
  <K, V> void onError(Exception exception, Span span);

  /**
   * Gives a SpanDecorator with the standard tags
   */
  SpanDecorator STANDARD_TAGS = new StandardSpanDecorator();
}
