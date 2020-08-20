/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.flink;

import static com.lyft.streamingplatform.analytics.EventUtils.BACKUP_DB_DATETIME_FORMATTER;
import static com.lyft.streamingplatform.analytics.EventUtils.DB_DATETIME_FORMATTER;
import static com.lyft.streamingplatform.analytics.EventUtils.GMT;
import static com.lyft.streamingplatform.analytics.EventUtils.ISO_DATETIME_FORMATTER;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Lists;
import com.google.auto.service.AutoService;
import com.lyft.streamingplatform.LyftKafkaConsumerBuilder;
import com.lyft.streamingplatform.LyftKafkaProducerBuilder;
import com.lyft.streamingplatform.analytics.Event;
import com.lyft.streamingplatform.analytics.EventField;
import com.lyft.streamingplatform.eventssource.KinesisAndS3EventSource;
import com.lyft.streamingplatform.eventssource.config.EventConfig;
import com.lyft.streamingplatform.eventssource.config.KinesisConfig;
import com.lyft.streamingplatform.eventssource.config.S3Config;
import com.lyft.streamingplatform.eventssource.config.SourceContext;
import com.lyft.streamingplatform.flink.FlinkLyftKinesisConsumer;
import com.lyft.streamingplatform.flink.InitialRoundRobinKinesisShardAssigner;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.NativeTransforms;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.flink.FlinkStreamingPortablePipelineTranslator.PTransformTranslator;
import org.apache.beam.runners.flink.FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.commons.io.IOUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.joda.time.Instant;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LyftFlinkStreamingPortableTranslations {

  private static final Logger LOG =
      LoggerFactory.getLogger(LyftFlinkStreamingPortableTranslations.class.getName());

  private static final String FLINK_KAFKA_URN = "lyft:flinkKafkaInput";
  private static final String FLINK_KAFKA_SINK_URN = "lyft:flinkKafkaSink";
  private static final String FLINK_KINESIS_URN = "lyft:flinkKinesisInput";
  private static final String FLINK_S3_AND_KINESIS_URN = "lyft:flinkS3AndKinesisInput";
  private static final String BYTES_ENCODING = "bytes";
  private static final String LYFT_BASE64_ZLIB_JSON = "lyft-base64-zlib-json";

  @AutoService(NativeTransforms.IsNativeTransform.class)
  public static class IsFlinkNativeTransform implements NativeTransforms.IsNativeTransform {
    @Override
    public boolean test(RunnerApi.PTransform pTransform) {
      return FLINK_KAFKA_URN.equals(PTransformTranslation.urnForTransformOrNull(pTransform))
          || FLINK_KAFKA_SINK_URN.equals(PTransformTranslation.urnForTransformOrNull(pTransform))
          || FLINK_KINESIS_URN.equals(PTransformTranslation.urnForTransformOrNull(pTransform))
          || FLINK_S3_AND_KINESIS_URN.equals(
              PTransformTranslation.urnForTransformOrNull(pTransform));
    }
  }

  public void addTo(
      ImmutableMap.Builder<String, PTransformTranslator<StreamingTranslationContext>>
          translatorMap) {
    translatorMap.put(FLINK_KAFKA_URN, this::translateKafkaInput);
    translatorMap.put(FLINK_KAFKA_SINK_URN, this::translateKafkaSink);
    translatorMap.put(FLINK_KINESIS_URN, this::translateKinesisInput);
    translatorMap.put(FLINK_S3_AND_KINESIS_URN, this::translateS3AndKinesisInputs);
  }

  @VisibleForTesting
  void translateKafkaInput(
      String id,
      RunnerApi.Pipeline pipeline,
      FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext context) {
    RunnerApi.PTransform pTransform = pipeline.getComponents().getTransformsOrThrow(id);

    final Map<String, Object> params;
    try {
      ObjectMapper mapper = new ObjectMapper();
      params = mapper.readValue(pTransform.getSpec().getPayload().toByteArray(), Map.class);
    } catch (IOException e) {
      throw new RuntimeException("Could not parse KafkaConsumer properties.", e);
    }

    final String topic;
    Preconditions.checkNotNull(topic = (String) params.get("topic"), "'topic' needs to be set");
    Map<String, String> consumerProps = (Map) params.get("properties");
    Preconditions.checkNotNull(consumerProps, "'properties' need to be set");
    LOG.info("Configuring kafka consumer for topic {} with properties {}", topic, consumerProps);

    final String userName = (String) params.get("username");
    final String password = (String) params.get("password");

    LyftKafkaConsumerBuilder<WindowedValue<byte[]>> consumerBuilder =
        new LyftKafkaConsumerBuilder<>();

    consumerBuilder.withUsername(userName);
    consumerBuilder.withPassword(password);

    Properties properties = new Properties();
    properties.putAll(consumerProps);
    consumerBuilder.withKafkaProperties(properties);

    FlinkKafkaConsumer011<WindowedValue<byte[]>> kafkaSource =
        consumerBuilder.build(topic, new ByteArrayWindowedValueSchema());

    if (params.getOrDefault("start_from_timestamp_millis", null) != null) {
      kafkaSource.setStartFromTimestamp(
          Long.parseLong(params.get("start_from_timestamp_millis").toString()));
    } else {
      kafkaSource.setStartFromLatest();
    }

    if (params.containsKey("max_out_of_orderness_millis")) {
      Number maxOutOfOrdernessMillis = (Number) params.get("max_out_of_orderness_millis");
      if (maxOutOfOrdernessMillis != null) {
        kafkaSource.assignTimestampsAndWatermarks(
            new WindowedTimestampExtractor<>(
                Time.milliseconds(maxOutOfOrdernessMillis.longValue())));
      }
    }

    context.addDataStream(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        context
            .getExecutionEnvironment()
            .addSource(kafkaSource, FlinkKafkaConsumer011.class.getSimpleName() + "-" + topic));
  }

  /**
   * Deserializer for native Flink Kafka source that produces {@link WindowedValue} expected by Beam
   * operators.
   */
  private static class ByteArrayWindowedValueSchema
      implements KeyedDeserializationSchema<WindowedValue<byte[]>> {
    private static final long serialVersionUID = -1L;

    private final TypeInformation<WindowedValue<byte[]>> ti;

    public ByteArrayWindowedValueSchema() {
      this.ti =
          new CoderTypeInformation<>(
              WindowedValue.getFullCoder(ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE));
    }

    @Override
    public TypeInformation<WindowedValue<byte[]>> getProducedType() {
      return ti;
    }

    @Override
    public WindowedValue<byte[]> deserialize(
        byte[] messageKey, byte[] message, String topic, int partition, long offset) {
      throw new UnsupportedOperationException();
    }

    @Override
    public WindowedValue<byte[]> deserialize(ConsumerRecord<byte[], byte[]> record) {
      return WindowedValue.timestampedValueInGlobalWindow(
          record.value(), new Instant(record.timestamp()));
    }

    @Override
    public boolean isEndOfStream(WindowedValue<byte[]> nextElement) {
      return false;
    }
  }

  // TODO: translation assumes byte[] values, does not support keys and headers
  @VisibleForTesting
  void translateKafkaSink(
      String id, RunnerApi.Pipeline pipeline, StreamingTranslationContext context) {
    RunnerApi.PTransform pTransform = pipeline.getComponents().getTransformsOrThrow(id);

    final Map<String, Object> params;
    try {
      ObjectMapper mapper = new ObjectMapper();
      params = mapper.readValue(pTransform.getSpec().getPayload().toByteArray(), Map.class);
    } catch (IOException e) {
      throw new RuntimeException("Could not parse KafkaConsumer properties.", e);
    }

    final String topic;
    Preconditions.checkNotNull(topic = (String) params.get("topic"), "'topic' needs to be set");
    Map<String, String> producerProps = (Map) params.get("properties");
    Preconditions.checkNotNull(producerProps, "'properties' need to be set");
    LOG.info("Configuring kafka producer for topic {} with properties {}", topic, producerProps);

    final String userName = (String) params.get("username");
    final String password = (String) params.get("password");

    LyftKafkaProducerBuilder<WindowedValue<byte[]>> producerBuilder =
        new LyftKafkaProducerBuilder<>();

    producerBuilder.withUsername(userName);
    producerBuilder.withPassword(password);

    Properties properties = new Properties();
    properties.putAll(producerProps);
    producerBuilder.withKafkaProperties(properties);

    FlinkKafkaProducer011<WindowedValue<byte[]>> producer =
        producerBuilder.build(topic, new ByteArrayWindowedValueSerializer());

    String inputCollectionId = Iterables.getOnlyElement(pTransform.getInputsMap().values());
    DataStream<WindowedValue<byte[]>> inputDataStream =
        context.getDataStreamOrThrow(inputCollectionId);

    // assigner below sets the required Flink record timestamp
    producer.setWriteTimestampToKafka(true);
    inputDataStream
        .transform("setTimestamp", inputDataStream.getType(), new FlinkTimestampAssigner<>())
        .addSink(producer)
        .name(FlinkKafkaProducer011.class.getSimpleName() + "-" + topic);
  }

  public static class ByteArrayWindowedValueSerializer
      implements SerializationSchema<WindowedValue<byte[]>> {
    @Override
    public byte[] serialize(WindowedValue<byte[]> element) {
      return element.getValue();
    }
  }

  /**
   * Assign the timestamp of {@link WindowedValue} as the Flink record timestamp. The Flink
   * timestamp is otherwise not set by the Beam Flink operators but is necessary for native Flink
   * operators such as the Kafka producer.
   *
   * @param <T>
   */
  private static class FlinkTimestampAssigner<T> extends AbstractStreamOperator<WindowedValue<T>>
      implements OneInputStreamOperator<WindowedValue<T>, WindowedValue<T>> {
    {
      super.setChainingStrategy(ChainingStrategy.ALWAYS);
    }

    @Override
    public void processElement(StreamRecord<WindowedValue<T>> element) {
      Instant timestamp = element.getValue().getTimestamp();
      if (timestamp != null && timestamp.getMillis() > 0) {
        super.output.collect(element.replace(element.getValue(), timestamp.getMillis()));
      } else {
        super.output.collect(element);
      }
    }

    @Override
    public void setup(StreamTask containingTask, StreamConfig config, Output output) {
      super.setup(containingTask, config, output);
    }
  }

  private void translateKinesisInput(
      String id,
      RunnerApi.Pipeline pipeline,
      FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext context) {
    RunnerApi.PTransform pTransform = pipeline.getComponents().getTransformsOrThrow(id);

    String stream;
    FlinkLyftKinesisConsumer<WindowedValue<byte[]>> source;

    Properties properties = new Properties();
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode params = mapper.readTree(pTransform.getSpec().getPayload().toByteArray());

      Preconditions.checkNotNull(
          stream = params.path("stream").textValue(), "'stream' needs to be set");

      Map<?, ?> consumerProps = mapper.convertValue(params.path("properties"), Map.class);
      Preconditions.checkNotNull(consumerProps, "'properties' need to be set");
      properties.putAll(consumerProps);

      String encoding = BYTES_ENCODING;
      if (params.hasNonNull("encoding")) {
        encoding = params.get("encoding").asText();
      }

      long maxOutOfOrdernessMillis = 5_000;
      if (params.hasNonNull("max_out_of_orderness_millis")) {
        maxOutOfOrdernessMillis =
            params.get("max_out_of_orderness_millis").numberValue().longValue();
      }

      switch (encoding) {
        case BYTES_ENCODING:
          source =
              FlinkLyftKinesisConsumer.create(
                  stream, new KinesisByteArrayWindowedValueSchema(), properties);
          break;
        case LYFT_BASE64_ZLIB_JSON:
          source =
              FlinkLyftKinesisConsumer.create(stream, new LyftBase64ZlibJsonSchema(), properties);
          source.setPeriodicWatermarkAssigner(
              new WindowedTimestampExtractor<>(Time.milliseconds(maxOutOfOrdernessMillis)));
          break;
        default:
          throw new IllegalArgumentException("Unknown encoding '" + encoding + "'");
      }

      LOG.info(
          "Kinesis consumer for stream {} with properties {} and encoding {}",
          stream,
          properties,
          encoding);

    } catch (IOException e) {
      throw new RuntimeException("Could not parse Kinesis consumer properties.", e);
    }

    source.setShardAssigner(
        InitialRoundRobinKinesisShardAssigner.fromInitialShards(
            properties, stream, context.getExecutionEnvironment().getConfig().getParallelism()));
    context.addDataStream(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        context
            .getExecutionEnvironment()
            .addSource(source, FlinkLyftKinesisConsumer.class.getSimpleName()));
  }

  private void translateS3AndKinesisInputs(
      String id,
      RunnerApi.Pipeline pipeline,
      FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext context) {
    RunnerApi.PTransform pTransform = pipeline.getComponents().getTransformsOrThrow(id);

    ObjectMapper mapper = new ObjectMapper();

    try {
      JsonNode params = mapper.readTree(pTransform.getSpec().getPayload().toByteArray());
      Map<?, ?> jsonMap = mapper.convertValue(params, Map.class);
      String sourceName = (String) jsonMap.get("source_name");
      Preconditions.checkNotNull(sourceName, "Source name has to be set");
      Map<String, JsonNode> userKinesisConfig = mapper.convertValue(
          jsonMap.get("kinesis"), new TypeReference<Map<String, JsonNode>>() {});

      Map<String, JsonNode> userS3Config = mapper.convertValue(
          jsonMap.get("s3"), new TypeReference<Map<String, JsonNode>>() {});

      List<Map<String, JsonNode>> events = mapper.convertValue(
          jsonMap.get("events"), new TypeReference<List<Map<String, JsonNode>>>() {});

      KinesisConfig kinesisConfig = getKinesisConfig(userKinesisConfig, mapper);
      S3Config s3Config = getS3Config(userS3Config);
      List<EventConfig> eventConfigs = getEventConfigs(events);

      SourceContext sourceContext = new SourceContext.Builder()
          .withStreamConfig(kinesisConfig)
          .withS3Config(s3Config)
          .withEventConfigs(eventConfigs)
          .withSourceName(sourceName)
          .build();

      KinesisAndS3EventSource source = new KinesisAndS3EventSource();
      StreamExecutionEnvironment environment = context.getExecutionEnvironment();
      Map<String, DataStream<Event>> eventStreams =
          source.getEventStreams(environment, sourceContext);

      for (String event : eventStreams.keySet()) {
        LOG.info("Created data streams for event: " + event);
      }

      // Add the DataStreams to the beam context
      for (Map.Entry<String, DataStream<Event>> entry : eventStreams.entrySet()) {
        context.addDataStream(
            Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
            entry.getValue());
      }

    } catch (IOException e) {
      throw new RuntimeException("Could not parse provided source json");
    }

  }

  private List<EventConfig> getEventConfigs(List<Map<String, JsonNode>> events) {
    List<EventConfig> eventConfigs = Lists.newArrayList();
    for (Map<String, JsonNode> node : events) {
      Preconditions.checkNotNull(node.get("name"), "Event name has to be set");
      EventConfig.Builder builder = new EventConfig.Builder(node.get("name").asText());

      // Add lateness in sec
      JsonNode latenessInSec = node.get("lateness_in_sec");
      if (latenessInSec != null) {
        builder = builder.withLatenessInSec(latenessInSec.asLong());
      }

      // Add lookback hours
      JsonNode lookbackDays = node.get("lookback_days");
      if (lookbackDays != null) {
        builder = builder.withLookbackInDays(lookbackDays.asInt());
      }

      eventConfigs.add(builder.build());
    }

    return eventConfigs;
  }

  private S3Config getS3Config(Map<String, JsonNode> userS3Config) {
    S3Config.Builder builder = new S3Config.Builder();

    // Add s3 parallelism
    JsonNode parallelism = userS3Config.get("parallelism");
    if (parallelism != null) {
      builder = builder.withParallelism(parallelism.asInt());
    }

    // Add s3 lookback hours
    JsonNode lookbackHours = userS3Config.get("lookback_hours");
    if (lookbackHours != null) {
      builder = builder.withLookbackHours(lookbackHours.asInt());
    }

    return builder.build();
  }

  private KinesisConfig getKinesisConfig(
      Map<String, JsonNode> userKinesisConfig, ObjectMapper mapper) {
    Properties properties = new Properties();
    Preconditions.checkNotNull(userKinesisConfig.get("name"), "Kinesis stream name needs to be set");

    KinesisConfig.Builder builder = new KinesisConfig
        .Builder(userKinesisConfig.get("name").asText());
    // Add kinesis parallelism
    JsonNode kinesisParallelism = userKinesisConfig.get("parallelism");
    if (kinesisParallelism != null) {
      builder = builder.withParallelism(kinesisParallelism.asInt());
    }
    // Add kinesis properties
    Map<String, String> kinesisProps = mapper.convertValue(
        userKinesisConfig.get("properties"), new TypeReference<Map<String, String>>() {});
    if (kinesisProps != null) {
      for (Map.Entry<String ,String> property : kinesisProps.entrySet()) {
        properties.put(property.getKey(), property.getValue());
      }
      builder = builder.withProperties(properties);
    }
    // Add kinesis stream start mode
    JsonNode streamStartMode = userKinesisConfig.get("stream_start_mode");
    if (streamStartMode != null) {
      builder = builder.withStreamStartMode(streamStartMode.asText());
    }

    return builder.build();
  }

  /**
   * Deserializer for native Flink Kafka source that produces {@link WindowedValue} expected by Beam
   * operators.
   */
  private static class KinesisByteArrayWindowedValueSchema
      implements KinesisDeserializationSchema<WindowedValue<byte[]>> {
    private final TypeInformation<WindowedValue<byte[]>> ti;

    public KinesisByteArrayWindowedValueSchema() {
      this.ti =
          new CoderTypeInformation<>(
              WindowedValue.getFullCoder(ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE));
    }

    @Override
    public TypeInformation<WindowedValue<byte[]>> getProducedType() {
      return ti;
    }

    @Override
    public WindowedValue<byte[]> deserialize(
        byte[] recordValue,
        String partitionKey,
        String seqNum,
        long approxArrivalTimestamp,
        String stream,
        String shardId) {
      return WindowedValue.valueInGlobalWindow(recordValue);
    }
  }

  /**
   * Deserializer for Lyft's kinesis event format, which is a zlib-compressed, base64-encoded, json
   * array of event objects. This schema tags events with the occurred_at time of the oldest event
   * in the message.
   *
   * <p>This schema passes through the original message.
   */
  @VisibleForTesting
  static class LyftBase64ZlibJsonSchema
      implements KinesisDeserializationSchema<WindowedValue<byte[]>> {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final TypeInformation<WindowedValue<byte[]>> ti =
        new CoderTypeInformation<>(
            WindowedValue.getFullCoder(ByteArrayCoder.of(), GlobalWindow.Coder.INSTANCE));

    private static String inflate(byte[] deflatedData) throws IOException {
      Inflater inflater = new Inflater();
      inflater.setInput(deflatedData);

      // buffer for inflating data
      byte[] buf = new byte[4096];

      try (ByteArrayOutputStream bos = new ByteArrayOutputStream(deflatedData.length)) {
        while (!inflater.finished()) {
          int count = inflater.inflate(buf);
          bos.write(buf, 0, count);
        }
        return bos.toString("UTF-8");
      } catch (DataFormatException e) {
        throw new IOException("Failed to expand message", e);
      }
    }

    private static long parseDateTime(String datetime) {
      try {
        DateTimeFormatter formatterToUse =
            (datetime.length() - datetime.indexOf('.') == 7)
                ? DB_DATETIME_FORMATTER
                : BACKUP_DB_DATETIME_FORMATTER;
        TemporalAccessor temporalAccessor = formatterToUse.parse(datetime);
        LocalDateTime localDateTime = LocalDateTime.from(temporalAccessor);
        ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, GMT);
        return java.time.Instant.from(zonedDateTime).toEpochMilli();
      } catch (DateTimeParseException e) {
        return java.time.Instant.from(ISO_DATETIME_FORMATTER.parse(datetime)).toEpochMilli();
      }
    }

    @Override
    public WindowedValue<byte[]> deserialize(
        byte[] recordValue,
        String partitionKey,
        String seqNum,
        long approxArrivalTimestamp,
        String stream,
        String shardId)
        throws IOException {
      String inflatedString = inflate(recordValue);

      JsonNode events = mapper.readTree(inflatedString);

      if (!events.isArray()) {
        throw new IOException("Events is not an array");
      }

      // Determine the timestamp as minimum occurred_at of all contained events.
      // For each event, attempt to extract occurred_at from either date string or number.
      // Assume timestamp as logged_at, when occurred_at is an (invalid) future timestamp.

      Iterator<JsonNode> iter = events.elements();
      long timestamp = Long.MAX_VALUE;
      while (iter.hasNext()) {
        JsonNode event = iter.next();
        JsonNode occurredAt = event.path(EventField.EventOccurredAt.fieldName());
        try {
          long occurredAtMillis;
          if (occurredAt.isTextual()) {
            occurredAtMillis = parseDateTime(occurredAt.textValue());
          } else if (occurredAt.isNumber()) {
            occurredAtMillis = occurredAt.asLong();
          } else {
            continue;
          }
          if (event.has(EventField.EventLoggedAt.fieldName())) {
            long loggedAtMillis = event.path(EventField.EventLoggedAt.fieldName()).asLong();
            if (loggedAtMillis > 0) {
              if (loggedAtMillis < Integer.MAX_VALUE) {
                loggedAtMillis = loggedAtMillis * 1000;
              }
              occurredAtMillis = Math.min(occurredAtMillis, loggedAtMillis);
            }
          }
          timestamp = Math.min(occurredAtMillis, timestamp);
        } catch (DateTimeParseException e) {
          // skip this timestamp
        }
      }

      // if we didn't find any valid timestamps, use Long.MIN_VALUE
      if (timestamp == Long.MAX_VALUE) {
        timestamp = Long.MIN_VALUE;
      }

      return WindowedValue.timestampedValueInGlobalWindow(recordValue, new Instant(timestamp));
    }

    @Override
    public TypeInformation<WindowedValue<byte[]>> getProducedType() {
      return ti;
    }
  }

  private static class WindowedTimestampExtractor<T>
      extends BoundedOutOfOrdernessTimestampExtractor<WindowedValue<T>> {
    public WindowedTimestampExtractor(Time maxOutOfOrderness) {
      super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(WindowedValue<T> element) {
      return element.getTimestamp() != null ? element.getTimestamp().getMillis() : Long.MIN_VALUE;
    }
  }
}
