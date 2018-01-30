[![Build Status][ci-img]][ci] [![Released Version][maven-img]][maven]


# OpenTracing Apache Kafka Client Instrumentation
OpenTracing instrumentation for Apache Kafka Client

## Requirements

- Java 8
- Kafka 1.0.0

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

```

### Kafka Client

```java

// Instantiate KafkaProducer
KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(senderProps);

//Decorate KafkaProducer with TracingKafkaProducer
TracingKafkaProducer<Integer, String> tracingKafkaProducer = new TracingKafkaProducer<>(kafkaProducer, 
        tracer);

// Send
tracingKafkaProducer.send(...);

// Instantiate KafkaConsumer
KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);

// Decorate KafkaConsumer with TracingKafkaConsumer
TracingKafkaConsumer<Integer, String> tracingKafkaConsumer = new TracingKafkaConsumer<>(kafkaConsumer, 
        tracer);

//Subscribe
tracingKafkaConsumer.subscribe(Collections.singletonList("messages"));

// Get records
ConsumerRecords<Integer, String> records = tracingKafkaConsumer.poll(1000);

// To retrieve SpanContext from polled record (Consumer side)
ConsumerRecord<Integer, String> record = ...
SpanContext spanContext = TracingKafkaUtils.extractSpanContext(record.headers(), tracer);

```

### Kafka Streams

```java

// Instantiate TracingKafkaClientSupplier
KafkaClientSupplier supplier = TracingKafkaClientSupplier(tracer);

// Provide supplier to KafkaStreams
KafkaStreams streams = new KafkaStreams(builder, new StreamsConfig(config), supplier);
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

```


[ci-img]: https://travis-ci.org/opentracing-contrib/java-kafka-client.svg?branch=master
[ci]: https://travis-ci.org/opentracing-contrib/java-kafka-client
[maven-img]: https://img.shields.io/maven-central/v/io.opentracing.contrib/opentracing-kafka-client.svg
[maven]: http://search.maven.org/#search%7Cga%7C1%7Copentracing-kafka-client
