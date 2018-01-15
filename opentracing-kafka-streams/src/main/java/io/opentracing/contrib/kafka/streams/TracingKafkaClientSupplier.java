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
package io.opentracing.contrib.kafka.streams;

import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaConsumer;
import io.opentracing.contrib.kafka.TracingKafkaProducer;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaClientSupplier;

public class TracingKafkaClientSupplier implements KafkaClientSupplier {

  private final Tracer tracer;

  public TracingKafkaClientSupplier(Tracer tracer) {
    this.tracer = tracer;
  }

  @Override
  public Producer<byte[], byte[]> getProducer(Map<String, Object> config) {
    return new TracingKafkaProducer<>(
        new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer()), tracer);
  }

  @Override
  public Consumer<byte[], byte[]> getConsumer(Map<String, Object> config) {
    return new TracingKafkaConsumer<>(
        new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer()),
        tracer);
  }

  @Override
  public Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> config) {
    return new TracingKafkaConsumer<>(
        new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer()),
        tracer);
  }
}
