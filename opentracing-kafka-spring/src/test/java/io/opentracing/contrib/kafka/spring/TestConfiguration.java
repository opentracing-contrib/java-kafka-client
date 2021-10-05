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

import static io.opentracing.contrib.kafka.spring.TracingSpringKafkaTest.cluster;

import io.opentracing.mock.MockTracer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
@EnableKafka
@ComponentScan
@EnableAspectJAutoProxy
public class TestConfiguration {

  @Bean
  public MockTracer tracer() {
    return new MockTracer();
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<Integer, String>
  kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    return factory;
  }

  @Bean
  public ConsumerFactory<Integer, String> consumerFactory() {
    final Map<String, Object> consumerProps = consumerProps("sampleRawConsumer", true);

    return new TracingConsumerFactory<>(new DefaultKafkaConsumerFactory<>(consumerProps), tracer());
  }


  @Bean
  public ProducerFactory<Integer, String> producerFactory() {
    return new TracingProducerFactory<>(new DefaultKafkaProducerFactory<>(producerProps()), tracer());
  }

  @Bean
  public KafkaTemplate<Integer, String> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

  @Bean
  public TracingKafkaAspect tracingKafkaAspect() {
    return new TracingKafkaAspect(tracer());
  }

  private Map<String, Object> producerProps() {
    final Map<String, Object> producerProps = new HashMap<>();

    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
    producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    return producerProps;
  }

  private Map<String, Object> consumerProps(String consumerGroup, boolean autoCommit)  {
    final Map<String, Object> consumerProps = new HashMap<>();

    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
    consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10");
    consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return consumerProps;
  }
}
