package io.opentracing.contrib.kafka;

import io.opentracing.Span;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface SpanDecorator {

    <K, V> void onSend(ProducerRecord<K, V> record, Span span);

    <K, V> void onResponse(ConsumerRecord<K, V> record, Span span);

    SpanDecorator STANDARD_TAGS = new SpanDecorator() {
        @Override
        public <K, V> void onSend(ProducerRecord<K, V> record, Span span) {
            StandardSpanDecorator.onSend(record, span);
        }

        @Override
        public <K, V> void onResponse(ConsumerRecord<K, V> record, Span span) {
            StandardSpanDecorator.onResponse(record, span);
        }
    };
}
