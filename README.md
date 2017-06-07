[![Build Status][ci-img]][ci] [![Released Version][maven-img]][maven]

# OpenTracing Apache Kafka Client Instrumentation
OpenTracing instrumentation for Apache Kafka Client

## Design
Span context is injected into Key by `TracingKafkaProducer` and extracted from Key by `TracingKafkaConsumer`.
Therefore it is required that on both sides Producer and Consumer are tracing.
From user perspective it is not visible that Key is modified. Logic is hidden via decorators.

Custom `Partitioner` can be set via extending `TracingPartitioner` and overriding `protected Partitioner partitioner` field.  


## Installation

pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-kafka-client</artifactId>
    <version>0.0.1</version>
</dependency>
```

## Usage


```java
// Instantiate tracer
Tracer tracer = ...

// Instantiate TracinKafkaProducer
TracingKafkaProducer<Integer, String> producer = new TracingKafkaProducer<>(senderProps, tracer);

// Send
producer.send(...);

// Instantiate TracingKafkaConsumer
TracingKafkaConsumer<Integer, String> kafkaConsumer = new TracingKafkaConsumer<>(consumerProps, tracer);

//Subscribe
kafkaConsumer.subscribe(Collections.singletonList("messages"));

// Get records
ConsumerRecords<Integer, String> records = kafkaConsumer.poll(1000);

```

[ci-img]: https://travis-ci.org/opentracing-contrib/java-kafka-client.svg?branch=master
[ci]: https://travis-ci.org/opentracing-contrib/java-kafka-client
[maven-img]: https://img.shields.io/maven-central/v/io.opentracing.contrib/opentracing-kafka-client.svg
[maven]: http://search.maven.org/#search%7Cga%7C1%7Copentracing-kafka-client
