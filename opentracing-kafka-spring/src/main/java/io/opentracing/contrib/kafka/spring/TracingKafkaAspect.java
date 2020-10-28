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

import io.opentracing.Tracer;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.aop.framework.ProxyFactoryBean;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * Wraps a {@link MessageListener} into a tracing proxy, to support {@link
 * org.springframework.kafka.annotation.KafkaListener} beans.
 * <p>
 * A port of Spring Sleuth implementation.
 */
@Aspect
public class TracingKafkaAspect {
  private final Tracer tracer;

  public TracingKafkaAspect(Tracer tracer) {
    this.tracer = tracer;
  }

  @Pointcut("execution(public * org.springframework.kafka.config.KafkaListenerContainerFactory.createListenerContainer(..))")
  private void anyCreateListenerContainer() {
  }

  @Pointcut("execution(public * org.springframework.kafka.config.KafkaListenerContainerFactory.createContainer(..))")
  private void anyCreateContainer() {
  }

  @Around("anyCreateListenerContainer() || anyCreateContainer()")
  public Object wrapListenerContainerCreation(ProceedingJoinPoint pjp) throws Throwable {
    MessageListenerContainer listener = (MessageListenerContainer) pjp.proceed();
    if (listener instanceof AbstractMessageListenerContainer) {
      AbstractMessageListenerContainer<?, ?> container = (AbstractMessageListenerContainer<?, ?>) listener;
      Object someMessageListener = container.getContainerProperties().getMessageListener();
      if (someMessageListener instanceof MessageListener) {
        container.setupMessageListener(createProxy(someMessageListener));
      }
    }
    return listener;
  }

  Object createProxy(Object bean) {
    ProxyFactoryBean factory = new ProxyFactoryBean();
    factory.setProxyTargetClass(true);
    factory.addAdvice(new MessageListenerMethodInterceptor(this.tracer));
    factory.setTarget(bean);
    return factory.getObject();
  }

}
