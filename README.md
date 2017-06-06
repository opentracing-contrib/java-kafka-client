[![Build Status][ci-img]][ci] [![Released Version][maven-img]][maven]

# OpenTracing Apache Kafka Client Instrumentation
OpenTracing instrumentation for Apache Kafka Client


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
TracingKafkaProducer<String> producer = new TracingKafkaProducer<>(senderProps, tracer);

// Send
producer.send(...);

// Instantiate TracingKafkaConsumer
TracingKafkaConsumer<String> kafkaConsumer = new TracingKafkaConsumer<>(consumerProps, tracer);

//Subscribe
kafkaConsumer.subscribe(Collections.singletonList("messages"));

// Get records
ConsumerRecords<KafkaSpanContext, String> records = kafkaConsumer.poll(1000);

```

