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
package io.opentracing.contrib.kafka.spring;

import static io.opentracing.contrib.kafka.spring.TracingSpringKafkaTest.embeddedKafka;

import io.opentracing.mock.MockTracer;
import io.opentracing.mock.MockTracer.Propagator;
import io.opentracing.util.ThreadLocalScopeManager;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;

@Configuration
@EnableKafka
@ComponentScan
public class TestConfiguration {

  @Bean
  public MockTracer tracer() {
    return new MockTracer(new ThreadLocalScopeManager(), Propagator.TEXT_MAP);
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
    final Map<String, Object> consumerProps = KafkaTestUtils
        .consumerProps("sampleRawConsumer", "false", embeddedKafka);
    consumerProps.put("auto.offset.reset", "earliest");

    return new TracingConsumerFactory<>(new DefaultKafkaConsumerFactory<>(consumerProps), tracer());
  }


  @Bean
  public ProducerFactory<Integer, String> producerFactory() {
    return new TracingProducerFactory<>(new DefaultKafkaProducerFactory<>(
        KafkaTestUtils.producerProps(embeddedKafka)), tracer());
  }

  @Bean
  public KafkaTemplate<Integer, String> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }
}
