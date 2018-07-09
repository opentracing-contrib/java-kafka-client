/*
 * Copyright 2017-2018 The OpenTracing Authors
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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.function.BiFunction;

/**
 * @author Jordan J Lopez
 *  Returns a string to be used as the name of the spans, based on
 *  the operation preformed and the record the span is based off of.
 */
public class ClientSpanNameProvider {

  // Operation Name as Span Name
  public static BiFunction<String, ConsumerRecord, String> CONSUMER_OPERATION_NAME =
      (operationName, consumerRecord) -> replaceIfNull(operationName, "unknown");
  public static BiFunction<String, ProducerRecord, String> PRODUCER_OPERATION_NAME =
      (operationName, producerRecord) -> replaceIfNull(operationName, "unknown");

  public static BiFunction<String, ConsumerRecord, String> CONSUMER_PREFIXED_OPERATION_NAME(final String prefix) {
    return (operationName, consumerRecord) -> replaceIfNull(prefix, "")
        + replaceIfNull(operationName, "unknown");
  }
  public static BiFunction<String, ProducerRecord, String> PRODUCER_PREFIXED_OPERATION_NAME(final String prefix) {
    return (operationName, producerRecord) -> replaceIfNull(prefix, "")
        + replaceIfNull(operationName, "unknown");
  }

  // Topic as Span Name
  public static BiFunction<String, ConsumerRecord, String> CONSUMER_TOPIC =
      (operationName, consumerRecord) -> replaceIfNull(consumerRecord, "unknown");
  public static BiFunction<String, ProducerRecord, String> PRODUCER_TOPIC =
      (operationName, producerRecord) -> replaceIfNull(producerRecord, "unknown");

  public static BiFunction<String, ConsumerRecord, String> CONSUMER_PREFIXED_TOPIC(final String prefix) {
    return (operationName, consumerRecord) -> replaceIfNull(prefix, "")
        + replaceIfNull(consumerRecord, "unknown");
  }
  public static BiFunction<String, ProducerRecord, String> PRODUCER_PREFIXED_TOPIC(final String prefix) {
    return (operationName, producerRecord) -> replaceIfNull(prefix, "")
        + replaceIfNull(producerRecord, "unknown");
  }

  // Operation Name and Topic as Span Name
  public static BiFunction<String, ConsumerRecord, String> CONSUMER_OPERATION_NAME_TOPIC =
      (operationName, consumerRecord) -> replaceIfNull(operationName, "unknown")
      + " - " + replaceIfNull(consumerRecord, "unknown");
  public static BiFunction<String, ProducerRecord, String> PRODUCER_OPERATION_NAME_TOPIC =
      (operationName, producerRecord) -> replaceIfNull(operationName, "unknown")
      + " - " + replaceIfNull(producerRecord, "unknown");

  public static BiFunction<String, ConsumerRecord, String> CONSUMER_PREFIXED_OPERATION_NAME_TOPIC(final String prefix) {
    return (operationName, consumerRecord) -> replaceIfNull(prefix, "")
        + replaceIfNull(operationName, "unknown")
        + " - " + replaceIfNull(consumerRecord, "unknown");
  }
  public static BiFunction<String, ProducerRecord, String> PRODUCER_PREFIXED_OPERATION_NAME_TOPIC(final String prefix) {
    return (operationName, producerRecord) -> replaceIfNull(prefix, "")
        + replaceIfNull(operationName, "unknown")
        + " - " + replaceIfNull(producerRecord, "unknown");
  }

  private static String replaceIfNull(String input, String replacement) {
    return (input == null) ? replacement : input;
  }

  private static String replaceIfNull(ConsumerRecord input, String replacement) {
    return ((input == null) ? replacement : input.topic());
  }

  private static String replaceIfNull(ProducerRecord input, String replacement) {
    return ((input == null) ? replacement : input.topic());
  }

}
