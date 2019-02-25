/*
 * Copyright 2017-2018 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.opentracing.contrib.kafka;

import io.jaegertracing.Configuration;
import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.opentracing.References;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class TracingKafkaUtils {

  private static final Logger logger = LoggerFactory.getLogger(TracingKafkaUtils.class);

  public static final String CONFIG_FILE_PROP = "opentracing.kafka.interceptors.config.file";
  public static final String TO_PREFIX = "To_";
  public static final String FROM_PREFIX = "From_";

  /**
   * Extract Span Context from record headers
   *
   * @param headers record headers
   * @return span context
   */
  static SpanContext extract(Headers headers, Tracer tracer) {
    return tracer
        .extract(Format.Builtin.TEXT_MAP, new HeadersMapExtractAdapter(headers, false));
  }

  /**
   * Extract Span Context from Consumer record headers
   *
   * @param headers Consumer record headers
   * @return span context
   */
  public static SpanContext extractSpanContext(Headers headers, Tracer tracer) {
    return tracer
        .extract(Format.Builtin.TEXT_MAP, new HeadersMapExtractAdapter(headers, true));
  }

  /**
   * Inject Span Context to record headers
   *
   * @param spanContext Span Context
   * @param headers record headers
   */
  static void inject(SpanContext spanContext, Headers headers,
      Tracer tracer) {
    tracer.inject(spanContext, Format.Builtin.TEXT_MAP,
        new HeadersMapInjectAdapter(headers, false));
  }

  /**
   * Inject second Span Context to record headers
   *
   * @param spanContext Span Context
   * @param headers record headers
   */
  static void injectSecond(SpanContext spanContext, Headers headers,
      Tracer tracer) {
    tracer.inject(spanContext, Format.Builtin.TEXT_MAP,
        new HeadersMapInjectAdapter(headers, true));
  }

  static <K,V> Scope buildAndInjectSpan(ProducerRecord<K, V> record, Tracer tracer) {
    return buildAndInjectSpan(record, tracer, ClientSpanNameProvider.PRODUCER_OPERATION_NAME);
  }

  static <K,V> Scope buildAndInjectSpan(ProducerRecord<K, V> record, Tracer tracer,
                                        BiFunction<String, ProducerRecord, String> producerSpanNameProvider) {

    String producerOper = TO_PREFIX + record.topic(); // <======== It provides better readability in the UI
    Tracer.SpanBuilder spanBuilder = tracer.buildSpan(producerSpanNameProvider.apply(producerOper, record))
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_PRODUCER);

    SpanContext spanContext = TracingKafkaUtils.extract(record.headers(), tracer);

    if (spanContext != null) {
      spanBuilder.asChildOf(spanContext);
    }

    Scope scope = spanBuilder.startActive(false);
    SpanDecorator.onSend(record, scope.span());

    try {
      TracingKafkaUtils.inject(scope.span().context(), record.headers(), tracer);
    } catch (Exception e) {
      // it can happen if headers are read only (when record is sent second time)
      logger.error("failed to inject span context. sending record second time?", e);
    }

    return scope;
  }

  static <K,V> void buildAndFinishChildSpan(ConsumerRecord<K, V> record, Tracer tracer) {
    buildAndFinishChildSpan(record, tracer, ClientSpanNameProvider.CONSUMER_OPERATION_NAME);
  }

  static <K,V> void buildAndFinishChildSpan(ConsumerRecord<K, V> record, Tracer tracer,
                                            BiFunction<String, ConsumerRecord, String> consumerSpanNameProvider) {
    SpanContext parentContext = TracingKafkaUtils.extract(record.headers(), tracer);

    if (parentContext != null) {

      String consumerOper = FROM_PREFIX + record.topic(); // <====== It provides better readability in the UI
      Tracer.SpanBuilder spanBuilder = tracer.buildSpan(consumerSpanNameProvider.apply(consumerOper, record))
          .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER);

      spanBuilder.asChildOf(parentContext);
      spanBuilder.addReference(References.FOLLOWS_FROM, parentContext);

      Span span = spanBuilder.start();
      SpanDecorator.onResponse(record, span);
      span.finish();

      // Inject created span context into record headers for extraction by client to continue span chain
      TracingKafkaUtils.injectSecond(span.context(), record.headers(), tracer);
    }
  }

  /**
   * Utility method to create tracers for the services defined in the
   * interceptors configuration file. Each service defined in the file
   * must has its own tracer.
   */
  private static Tracer createTracer(Service service) {

    SamplerConfiguration samplerConfig = SamplerConfiguration.fromEnv()
      .withType(service.getTraceConfig().getSamplerType())
      .withParam(service.getTraceConfig().getSamplerParam());

    ReporterConfiguration reporterConfig = ReporterConfiguration.fromEnv()
      .withLogSpans(service.getTraceConfig().isLogSpans());

    Configuration config = new Configuration(service.getServiceName())
      .withSampler(samplerConfig)
      .withReporter(reporterConfig);
    
    return config.getTracer();

    // Currently creating a Jaeger tracer but it would be nice if this
    // method could create different type of tracers depending of some
    // parameter set in the interceptors configuration file.

  }

  /**
   * Builds a mapping between topics and tracers, so the interceptors knows
   * which tracer to use given a topic. This mapping is important because it
   * provides a way to cache tracers and we can avoid the time spent during
   * tracer instantiation. Also, it is important to provide a rapid way to
   * retrieve a tracer given the topic name, preferably using a O(1) retrieval
   * method such as a Hashtable.
   * 
   * @param configFileName Configuration file specified as a property
   * 
   */
  public static Map<String, Tracer> buildTracerMapping(String configFileName)
    throws FileNotFoundException, IOException {

    Map<String, Tracer> mapping = null;
    File file = new File(configFileName);

    if (file.exists()) {

      mapping = new HashMap<String, Tracer>();
      List<Service> services = loadServices(file);

      for (Service service : services) {

        Tracer tracer = createTracer(service);

        for (String topic : service.getTopics()) {
          mapping.put(topic, tracer);
        }

      }

    } else {

      throw new FileNotFoundException("The file '" + configFileName + "' does not exist.");

    }

    return mapping;

  }

  /**
   * Load the services from the interceptors configuration file. This file
   * has an single attribute called 'services' of type array, and this array
   * has AT LEAST ONE or multiple services within. Each service MUST have
   * a name and OPTIONAL attributes for 'traceConfig' and 'topics'.
   * 
   * Here is an example of two services defines, each one with their own
   * tracer configuration and topics definition.
   * 
   * {
   * 
   *   "services" : [
   * 
   *      {
   
             "service" : "Service-1",
             "traceConfig" : {
                "samplerType" : "const",
                "samplerParam" : 1,
                "logSpans" : true
             },
             "topics" : ["Topic-1", "Topic-2", Topic-3]
   * 
   *      },
   *      {
   
             "service" : "Service-2",
             "traceConfig" : {
                "samplerType" : "probabilistic",
                "samplerParam" : 0.8,
                "logSpans" : false
             },
             "topics" : ["Topic-4", "Topic-5", Topic-6]
   * 
   *      }
   * 
   *   ]
   * 
   * }
   * 
   * It is important to note that the topics defined in this file
   * will be used as keys to retrieve the correspondent tracer,
   * which in turn is created per service. Therefore, a topic
   * should not belong to two different services at the same
   * time, otherwise there will be collapses and undesirable
   * tracing behavior.
   * 
   */
  private static List<Service> loadServices(File file)
    throws FileNotFoundException, IOException {

    List<Service> services = new ArrayList<Service>();

    try (FileReader reader = new FileReader(file)) {

      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(reader);
      JsonObject root = element.getAsJsonObject();
      JsonArray svcs = root.getAsJsonArray("services");

      for (int i = 0; i < svcs.size(); i++) {

        JsonObject svc = svcs.get(i).getAsJsonObject();

        String serviceName = svc.get("service").getAsString();
        JsonObject trCnf = svc.getAsJsonObject("traceConfig");
        JsonArray topicsArray = svc.getAsJsonArray("topics");
        TraceConfig traceConfig = getTraceConfig(trCnf);
        List<String> topics = getTopics(topicsArray);

        services.add(new Service(serviceName, traceConfig, topics));

      }

    }

    return services;

  }

  private static TraceConfig getTraceConfig(JsonObject trCnf) {

    if (trCnf == null) {

      // If none provided, create a default one...
      return new TraceConfig("const", 1, true);

    }

    JsonElement ele = trCnf.get("samplerType");
    String samplerType = ele != null ? ele.getAsString() : "const";

    ele = trCnf.get("samplerParam");
    double samplerParam = ele != null ? ele.getAsDouble() : 1;

    ele = trCnf.get("logSpans");
    boolean logSpans = ele != null ? ele.getAsBoolean() : true;

    return new TraceConfig(samplerType, samplerParam, logSpans);

  }

  private static List<String> getTopics(JsonArray topicsArray) {

    List<String> topics = new ArrayList<String>();

    if (topicsArray != null) {

      for (int i = 0; i < topicsArray.size(); i++) {
        topics.add(topicsArray.get(i).getAsString());
      }

    }

    return topics;

  }

  private static class Service {

    private String serviceName;
    private TraceConfig traceConfig;
    private List<String> topics;

    public Service(String serviceName,
      TraceConfig traceConfig, List<String> topics) {

        this.serviceName = serviceName;
        this.traceConfig = traceConfig;
        this.topics = topics;

    }

    public String getServiceName() {

      return this.serviceName;

    }

    public TraceConfig getTraceConfig() {

      return this.traceConfig;

    }

    public List<String> getTopics() {

      return this.topics;

    }

  }

  private static class TraceConfig {

    private String samplerType;
    private double samplerParam;
    private boolean logSpans;

    public TraceConfig(String samplerType,
      double samplerParam, boolean logSpans) {

        this.samplerType = samplerType;
        this.samplerParam = samplerParam;
        this.logSpans = logSpans;
      
    }

    public String getSamplerType() {

      return this.samplerType;

    }

    public double getSamplerParam() {

      return this.samplerParam;

    }

    public boolean isLogSpans() {

      return this.logSpans;

    }

  }

}