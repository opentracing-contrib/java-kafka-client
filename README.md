[![Build Status][ci-img]][ci] [![Released Version][maven-img]][maven]


# OpenTracing Apache Kafka Client Instrumentation
OpenTracing instrumentation for Apache Kafka Client

## Requirements

- Java 8
- Scala 2.12
- Kafka_2.12 0.11.0.0

## Installation

pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-kafka-client</artifactId>
    <version>0.0.4</version>
</dependency>
```

## Usage


```java
// Instantiate tracer
Tracer tracer = ...

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

// If needed get Span Context on the Consumer side from polled record
ConsumerRecord<Integer, String> record = ...
SpanContext spanContext = TracingKafkaUtils.extractSpanContext(record.headers(), tracer);

```

[ci-img]: https://travis-ci.org/opentracing-contrib/java-kafka-client.svg?branch=master
[ci]: https://travis-ci.org/opentracing-contrib/java-kafka-client
[maven-img]: https://img.shields.io/maven-central/v/io.opentracing.contrib/opentracing-kafka-client.svg
[maven]: http://search.maven.org/#search%7Cga%7C1%7Copentracing-kafka-client
