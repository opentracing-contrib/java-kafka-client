/*
 * Copyright 2017 The OpenTracing Authors
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


import io.opentracing.ActiveSpan;
import io.opentracing.ActiveSpan.Continuation;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Callback executed after the producer has finished sending a message
 */
public class TracingCallback implements Callback {

  private final Callback callback;
  private final Continuation continuation;

  TracingCallback(Callback callback, Continuation continuation) {
    this.callback = callback;
    this.continuation = continuation;
  }

  @Override
  public void onCompletion(RecordMetadata metadata, Exception exception) {
    ActiveSpan activeSpan = continuation.activate();

    if (exception != null) {
      SpanDecorator.onError(exception, activeSpan);
    }

    try {
      if (callback != null) {
        callback.onCompletion(metadata, exception);
      }
    } finally {
      activeSpan.deactivate();
    }
  }
}
