package io.opentracing.contrib.kafka;

import io.opentracing.Tracer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

public class TracingKafkaConsumerBuilder<K, V> {
    private List<SpanDecorator> spanDecorators;
    private Consumer<K, V> consumer;
    private Tracer tracer;
    private BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider;

    public TracingKafkaConsumerBuilder(Consumer<K, V> consumer, Tracer tracer) {
        this.tracer = tracer;
        this.consumer = consumer;
        this.spanDecorators = Collections.singletonList(SpanDecorator.STANDARD_TAGS);
        this.consumerSpanNameProvider = ClientSpanNameProvider.CONSUMER_OPERATION_NAME;
    }

    public TracingKafkaConsumerBuilder withDecorators(List<SpanDecorator> spanDecorators) {
        this.spanDecorators = spanDecorators;
        return this;
    }

    public TracingKafkaConsumerBuilder withSpanNameProvider(BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider) {
        this.consumerSpanNameProvider = consumerSpanNameProvider;
        return this;
    }

    public TracingKafkaConsumer<K, V> build() {
        return new TracingKafkaConsumer<>(consumer, tracer, spanDecorators, consumerSpanNameProvider);
    }
}
