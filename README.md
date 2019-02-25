[![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov] [![Released Version][maven-img]][maven]


# OpenTracing Apache Kafka Client Instrumentation
OpenTracing instrumentation for Apache Kafka Client.    
Two solutions are provided:
1. Based on decorated Producer and Consumer
1. Based on Interceptors

## Requirements

- Java 8
- Kafka 2.0.0

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
KafkaClientSupplier supplier = TracingKafkaClientSupplier(tracer);

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

### Embedded Interceptors

The interceptors `TracingProducerInterceptor` and `TracingConsumerInterceptor` are great implementations to use with Java-based applications where you own the code and are able to instantiate your own tracers and make them available throughout the JVM. However, there are situations in which records will be produced and consumed from JVMs that are automatically created for you; and you don't have any way to instantiate your own tracers because its source-code is not available, or maybe it is but you are not allowed to change it.

Typical examples include the use of [REST Proxy](https://docs.confluent.io/current/kafka-rest/docs/index.html), [Kafka Connect](https://docs.confluent.io/current/connect/index.html), and [Confluent's KSQL Servers](https://docs.confluent.io/current/ksql/docs/index.html). In this technologies, the JVMs are automatically created by pre-defined scripts and you would need a special type of interceptor, one that can instantiate their own tracers based on configuration specified in an external file. For this particular situation, you can use the `TracingProducerEmbeddedInterceptor` and `TracingConsumerEmbeddedInterceptor` interceptors.

For example, if you want to use these interceptors with a REST Proxy Server, you need to edit the properties configuration file used to start the server and include the following lines:

```java
id=my-rest-proxy-server
schema.registry.url=http://localhost:8081
bootstrap.servers=PLAINTEXT://localhost:9092

################################ OpenTracing Configuration #################################

producer.interceptor.classes=io.opentracing.contrib.kafka.TracingProducerEmbeddedInterceptor
consumer.interceptor.classes=io.opentracing.contrib.kafka.TracingConsumerEmbeddedInterceptor

opentracing.kafka.interceptors.config.file=interceptorsConfig.json

############################################################################################
```
Note that we provided a JSON configuration file via the property `opentracing.kafka.interceptors.config.file`. This file contains the definition of all services that need to be traced, as well as the configuration of the tracer for each service. This is important because the JVM could be used to host multiple services at a time, each one belonging to a different domain. Since each service will probably use different Kafka topics to implement their processing logic, this file helps in keeping a mapping between services and topics so the tracing is executed separately for each service.

Here is an example:

```json
{

   "services" : [

      {

         "service" : "CustomerService",
         "traceConfig" : {
            "samplerType" : "const",
            "samplerParam" : 1,
            "logSpans" : true
         },
         "topics" : ["Topic-1", "Topic-2"]

      },
      {

         "service" : "ProductService",
         "traceConfig" : {
            "samplerType" : "const",
            "samplerParam" : 1,
            "logSpans" : true
         },
         "topics" : ["Topic-3", "Topic-4"]

      }

   ]

}
```
In the example above, two services were defined, `CustomerService` and `ProductService` respectively. In runtime, it will be created one tracer for each one of these services. However, every time a record is either produced or consumed for topic `Topic-1`, the interceptor knows that it should use the tracer associated for `CustomerService`, as well as every time a record is either produced or consumed for topic `Topic-3`, the interceptor knows that it should use the tracer associated for `ProductService`.

The embedded interceptors are contained in the Kafka Client package, so in order to use it you will need the following dependency in the JVM classpath:

```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-kafka-client</artifactId>
    <version>VERSION</version>
</dependency>
```
Since the embedded interceptors instantiate their own tracers using Jaeger, you are going to need to the following dependencies as well:

```xml
<dependency>
    <groupId>io.opentracing</groupId>
    <artifactId>opentracing-api</artifactId>
    <version>VERSION</version>
</dependency>

<dependency>
    <groupId>io.opentracing</groupId>
    <artifactId>opentracing-util</artifactId>
    <version>VERSION</version>
</dependency>

<dependency>
    <groupId>io.jaegertracing</groupId>
    <artifactId>jaeger-client</artifactId>
    <version>VERSION</version>
</dependency>

<dependency>
    <groupId>io.jaegertracing</groupId>
    <artifactId>jaeger-core</artifactId>
    <version>VERSION</version>
</dependency>

<dependency>
    <groupId>io.jaegertracing</groupId>
    <artifactId>jaeger-thrift</artifactId>
    <version>VERSION</version>
</dependency>

<dependency>
    <groupId>org.apache.thrift</groupId>
    <artifactId>libthrift</artifactId>
    <version>VERSION</version>
</dependency>
```

## License

[Apache 2.0 License](./LICENSE).

[ci-img]: https://travis-ci.org/opentracing-contrib/java-kafka-client.svg?branch=master
[ci]: https://travis-ci.org/opentracing-contrib/java-kafka-client
[cov-img]: https://coveralls.io/repos/github/opentracing-contrib/java-kafka-client/badge.svg?branch=master
[cov]: https://coveralls.io/github/opentracing-contrib/java-kafka-client?branch=master
[maven-img]: https://img.shields.io/maven-central/v/io.opentracing.contrib/opentracing-kafka-client.svg
[maven]: http://search.maven.org/#search%7Cga%7C1%7Copentracing-kafka-client
