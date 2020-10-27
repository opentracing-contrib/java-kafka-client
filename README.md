[![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov] [![Released Version][maven-img]][maven] [![Apache-2.0 license](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


# OpenTracing Apache Kafka Client Instrumentation
OpenTracing instrumentation for Apache Kafka Client.    
Two solutions are provided:
1. Based on decorated Producer and Consumer
1. Based on Interceptors

## Requirements

- Java 8
- Kafka 2.2.0

## Installation

### Kafka Client

pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-kafka-client</artifactId>
    <version>VERSION</version>
</dependency>
```

### Kafka Streams

pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-kafka-streams</artifactId>
    <version>VERSION</version>
</dependency>
```

### Spring Kafka

pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-kafka-spring</artifactId>
    <version>VERSION</version>
</dependency>
```

## Usage

```java

// Instantiate tracer
Tracer tracer = ...

// Optionally register tracer with GlobalTracer
GlobalTracer.register(tracer);
```

### Kafka Client

#### Decorators based solution

```java

// Instantiate KafkaProducer
KafkaProducer<Integer, String> producer = new KafkaProducer<>(senderProps);

//Decorate KafkaProducer with TracingKafkaProducer
TracingKafkaProducer<Integer, String> tracingProducer = new TracingKafkaProducer<>(producer, 
        tracer);

// Send
tracingProducer.send(...);

// Instantiate KafkaConsumer
KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(consumerProps);

// Decorate KafkaConsumer with TracingKafkaConsumer
TracingKafkaConsumer<Integer, String> tracingConsumer = new TracingKafkaConsumer<>(consumer, 
        tracer);

//Subscribe
tracingConsumer.subscribe(Collections.singletonList("messages"));

// Get records
ConsumerRecords<Integer, String> records = tracingConsumer.poll(1000);

// To retrieve SpanContext from polled record (Consumer side)
ConsumerRecord<Integer, String> record = ...
SpanContext spanContext = TracingKafkaUtils.extractSpanContext(record.headers(), tracer);

```

##### Custom Span Names for Decorators based solution
The decorator-based solution includes support for custom span names by passing in a BiFunction object as an additional
argument to the TracingKafkaConsumer or TracingKafkaProducer constructors, either one of the provided BiFunctions or
your own custom one.

```java
// Create BiFunction for the KafkaProducer that operates on
// (String operationName, ProducerRecord consumerRecord) and
// returns a String to be used as the name
BiFunction<String, ProducerRecord, String> producerSpanNameProvider =
    (operationName, producerRecord) -> "CUSTOM_PRODUCER_NAME";

// Instantiate KafkaProducer
KafkaProducer<Integer, String> producer = new KafkaProducer<>(senderProps);

//Decorate KafkaProducer with TracingKafkaProducer
TracingKafkaProducer<Integer, String> tracingProducer = new TracingKafkaProducer<>(producer, 
        tracer,
        producerSpanNameProvider);
// Spans created by the tracingProducer will now have "CUSTOM_PRODUCER_NAME" as the span name.


// Create BiFunction for the KafkaConsumer that operates on
// (String operationName, ConsumerRecord consumerRecord) and
// returns a String to be used as the name
BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider =
    (operationName, consumerRecord) -> operationName.toUpperCase();
// Instantiate KafkaConsumer
KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(consumerProps);
// Decorate KafkaConsumer with TracingKafkaConsumer, passing in the consumerSpanNameProvider BiFunction
TracingKafkaConsumer<Integer, String> tracingConsumer = new TracingKafkaConsumer<>(consumer, 
        tracer,
        consumerSpanNameProvider);
// Spans created by the tracingConsumer will now have the capitalized operation name as the span name.
// "receive" -> "RECEIVE"
```  


#### Interceptors based solution
```java
// Register tracer with GlobalTracer:
GlobalTracer.register(tracer);

// Add TracingProducerInterceptor to sender properties:
senderProps.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, 
          TracingProducerInterceptor.class.getName());

// Instantiate KafkaProducer
KafkaProducer<Integer, String> producer = new KafkaProducer<>(senderProps);

// Send
producer.send(...);

// Add TracingConsumerInterceptor to consumer properties:
consumerProps.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
          TracingConsumerInterceptor.class.getName());

// Instantiate KafkaConsumer
KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(consumerProps);

//Subscribe
consumer.subscribe(Collections.singletonList("messages"));

// Get records
ConsumerRecords<Integer, String> records = consumer.poll(1000);

// To retrieve SpanContext from polled record (Consumer side)
ConsumerRecord<Integer, String> record = ...
SpanContext spanContext = TracingKafkaUtils.extractSpanContext(record.headers(), tracer);

```


### Kafka Streams

```java

// Instantiate TracingKafkaClientSupplier
KafkaClientSupplier supplier = new TracingKafkaClientSupplier(tracer);

// Provide supplier to KafkaStreams
KafkaStreams streams = new KafkaStreams(builder.build(), new StreamsConfig(config), supplier);
streams.start();

```

### Spring Kafka

```java

// Declare Tracer bean
@Bean
public Tracer tracer() {
  return ...
}


// Decorate ConsumerFactory with TracingConsumerFactory
@Bean
public ConsumerFactory<Integer, String> consumerFactory() {
  return new TracingConsumerFactory<>(new DefaultKafkaConsumerFactory<>(consumerProps()), tracer());
}

// Decorate ProducerFactory with TracingProducerFactory
@Bean
public ProducerFactory<Integer, String> producerFactory() {
  return new TracingProducerFactory<>(new DefaultKafkaProducerFactory<>(producerProps()), tracer());
}

// Use decorated ProducerFactory in KafkaTemplate 
@Bean
public KafkaTemplate<Integer, String> kafkaTemplate() {
  return new KafkaTemplate<>(producerFactory());
}

// Use an aspect to decorate @KafkaListeners
@Bean
public TracingKafkaAspect tracingKafkaAspect() {
  return new TracingKafkaAspect(tracer());
}
```

##### Custom Span Names for Spring Kafka
The Spring Kafka factory implementations include support for custom span names by passing in a BiFunction object as an additional
argument to the TracingConsumerFactory or TracingProducerFactory constructors, either one of the provided BiFunctions or
your own custom one.

```java
// Create BiFunction for the KafkaProducerFactory that operates on
// (String operationName, ProducerRecord consumerRecord) and
// returns a String to be used as the name
BiFunction<String, ProducerRecord, String> producerSpanNameProvider =
    (operationName, producerRecord) -> "CUSTOM_PRODUCER_NAME";

// Decorate ProducerFactory with TracingProducerFactory
@Bean
public ProducerFactory<Integer, String> producerFactory() {
  return new TracingProducerFactory<>(new DefaultKafkaProducerFactory<>(producerProps()), tracer());
}
// Spans created by the tracingProducer will now have "CUSTOM_PRODUCER_NAME" as the span name.


// Create BiFunction for the KafkaConsumerFactory that operates on
// (String operationName, ConsumerRecord consumerRecord) and
// returns a String to be used as the name
BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider =
    (operationName, consumerRecord) -> operationName.toUpperCase();

// Decorate ConsumerFactory with TracingConsumerFactory
@Bean
public ConsumerFactory<Integer, String> consumerFactory() {
  return new TracingConsumerFactory<>(new DefaultKafkaConsumerFactory<>(consumerProps()), tracer());
}
// Consumers produced by the traced consumerFactory
```

#### Pre-made Span Name Providers

The following BiFunctions are already included in the ClientSpanNameProvider class, with `CONSUMER_OPERATION_NAME` and `PRODUCER_OPERATION_NAME` being the default should no
spanNameProvider be provided:

- `CONSUMER_OPERATION_NAME` and `PRODUCER_OPERATION_NAME` : Returns the `operationName` as the span name ("receive" for Consumer, "send" for producer).
- `CONSUMER_PREFIXED_OPERATION_NAME(String prefix)` and `PRODUCER_PREFIXED_OPERATION_NAME(String prefix)` : Returns a String concatenation of `prefix` and `operatioName`.
- `CONSUMER_TOPIC` and `PRODUCER_TOPIC` : Returns the Kafka topic name that the record was pushed to/pulled from (`record.topic()`).
- `PREFIXED_CONSUMER_TOPIC(String prefix)` and `PREFIXED_PRODUCER_TOPIC(String prefix)` : Returns a String concatenation of `prefix` and the Kafka topic name (`record.topic()`).
- `CONSUMER_OPERATION_NAME_TOPIC` and `PRODUCER_OPERATION_NAME_TOPIC` : Returns "`operationName` - `record.topic()`".
- `CONSUMER_PREFIXED_OPERATION_NAME_TOPIC(String prefix)` and `PRODUCER_PREFIXED_OPERATION_NAME_TOPIC(String prefix)` : Returns a String concatenation of `prefix` and "`operationName` - `record.topic()`".

## License

[Apache 2.0 License](./LICENSE).

[ci-img]: https://travis-ci.org/opentracing-contrib/java-kafka-client.svg?branch=master
[ci]: https://travis-ci.org/opentracing-contrib/java-kafka-client
[cov-img]: https://coveralls.io/repos/github/opentracing-contrib/java-kafka-client/badge.svg?branch=master
[cov]: https://coveralls.io/github/opentracing-contrib/java-kafka-client?branch=master
[maven-img]: https://img.shields.io/maven-central/v/io.opentracing.contrib/opentracing-kafka-client.svg
[maven]: http://search.maven.org/#search%7Cga%7C1%7Copentracing-kafka-client
