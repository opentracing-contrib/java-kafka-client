package io.opentracing.contrib.kafka;


import io.opentracing.Span;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Callback executed after the producer has finished sending a message
 */
public class TracingCallback implements Callback {

  private final Callback callback;
  private final Span span;

  public TracingCallback(Callback callback, Span span) {
    this.callback = callback;
    this.span = span;
  }

  @Override
  public void onCompletion(RecordMetadata metadata, Exception exception) {
    if (exception != null) {
      SpanDecorator.onError(exception, span);
    }

    try {
      if (callback != null) {
        callback.onCompletion(metadata, exception);
      }
    } finally {
      span.finish();
    }
  }
}
