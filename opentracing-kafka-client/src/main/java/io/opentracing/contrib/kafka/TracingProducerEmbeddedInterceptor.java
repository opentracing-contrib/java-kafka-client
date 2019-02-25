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

import io.opentracing.Scope;
import io.opentracing.Tracer;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class TracingProducerEmbeddedInterceptor<K, V> implements ProducerInterceptor<K, V> {

  private Map<String, Tracer> tracerMapping;

  @Override
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {

    if (tracerMapping != null) {

      Tracer tracer = tracerMapping.get(record.topic());

      if (tracer != null) {
  
        try (Scope scope = TracingKafkaUtils.buildAndInjectSpan(record, tracer)) {
          scope.span().finish();
        }
  
      }

    }

    return record;

  }

  @Override
  public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {

    if (configs.containsKey(TracingKafkaUtils.CONFIG_FILE_PROP)) {

      String configFileName = (String) configs.get(TracingKafkaUtils.CONFIG_FILE_PROP);

      try {
        tracerMapping = TracingKafkaUtils.buildTracerMapping(configFileName);
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
 
    }

  }
  
}