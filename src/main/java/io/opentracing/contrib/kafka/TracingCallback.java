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
