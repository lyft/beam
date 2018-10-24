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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.lyft.streamingplatform.analytics.EventField;
import com.lyft.streamingplatform.flink.FlinkLyftKinesisConsumer;
import com.lyft.streamingplatform.flink.InitialRoundRobinKinesisShardAssigner;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Iterator;
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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LyftFlinkStreamingPortableTranslations {

  private static final Logger logger =
      LoggerFactory.getLogger(LyftFlinkStreamingPortableTranslations.class.getName());

  private static final String FLINK_KAFKA_URN = "lyft:flinkKafkaInput";
  private static final String FLINK_KINESIS_URN = "lyft:flinkKinesisInput";
  private static final String BYTES_ENCODING = "bytes";
  private static final String LYFT_KINESIS_EVENT_ENCODING = "lyft_kinesis_event";

  @AutoService(NativeTransforms.IsNativeTransform.class)
  public static class IsFlinkNativeTransform implements NativeTransforms.IsNativeTransform {
    @Override
    public boolean test(RunnerApi.PTransform pTransform) {
      return FLINK_KAFKA_URN.equals(PTransformTranslation.urnForTransformOrNull(pTransform))
          || FLINK_KINESIS_URN.equals(PTransformTranslation.urnForTransformOrNull(pTransform));
    }
  }

  public void addTo(
      ImmutableMap.Builder<String, PTransformTranslator<StreamingTranslationContext>>
          translatorMap) {
    translatorMap.put(FLINK_KAFKA_URN, this::translateKafkaInput);
    translatorMap.put(FLINK_KINESIS_URN, this::translateKinesisInput);
  }

  private void translateKafkaInput(
      String id,
      RunnerApi.Pipeline pipeline,
      FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext context) {
    RunnerApi.PTransform pTransform = pipeline.getComponents().getTransformsOrThrow(id);

    String topic;
    Properties properties = new Properties();
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode params = mapper.readTree(pTransform.getSpec().getPayload().toByteArray());

      Preconditions.checkNotNull(topic = params.path("topic").textValue(),
          "'topic' needs to be set");

      Map<?, ?> consumerProps = mapper.convertValue(params.path("properties"), Map.class);
      Preconditions.checkNotNull(consumerProps, "'properties' need to be set");
      properties.putAll(consumerProps);
    } catch (IOException e) {
      throw new RuntimeException("Could not parse KafkaConsumer properties.", e);
    }

    logger.info("Kafka consumer for topic {} with properties {}", topic, properties);

    DataStreamSource<WindowedValue<byte[]>> source =
        context
            .getExecutionEnvironment()
            .addSource(
                new FlinkKafkaConsumer010<>(topic, new ByteArrayWindowedValueSchema(), properties)
                    .setStartFromLatest());
    context.addDataStream(Iterables.getOnlyElement(pTransform.getOutputsMap().values()), source);
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
      //System.out.println("###Kafka record: " + new String(message, Charset.defaultCharset()));
      return WindowedValue.valueInGlobalWindow(message);
    }

    @Override
    public boolean isEndOfStream(WindowedValue<byte[]> nextElement) {
      return false;
    }
  }

  private void translateKinesisInput(
      String id,
      RunnerApi.Pipeline pipeline,
      FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext context) {
    RunnerApi.PTransform pTransform = pipeline.getComponents().getTransformsOrThrow(id);

    String stream;
    DeserializationSchema<WindowedValue<byte[]>> deserializationSchema;
    Properties properties = new Properties();
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode params = mapper.readTree(
          pTransform.getSpec().getPayload().toByteArray());

      Preconditions.checkNotNull(
          stream = params.path("stream").textValue(), "'stream' needs to be set");

      String encoding = BYTES_ENCODING;
      if (params.has("encoding")) {
        encoding = params.get("encoding").asText();
      }

      switch (encoding) {
        case BYTES_ENCODING:
          deserializationSchema = new KinesisByteArrayWindowedValueSchema();
          break;
        case LYFT_KINESIS_EVENT_ENCODING:
          deserializationSchema = new LyftBeamKinesisSchema();
          break;
        default:
          throw new IllegalArgumentException("Unknown encoding '" + encoding + "'");
      }

      Map<?, ?> consumerProps = mapper.convertValue(params.path("properties"), Map.class);
      Preconditions.checkNotNull(consumerProps, "'properties' need to be set");
      properties.putAll(consumerProps);

      logger.info("Kinesis consumer for stream {} with properties {} and encoding {}", stream,
          properties, encoding);

    } catch (IOException e) {
      throw new RuntimeException("Could not parse Kinesis consumer properties.", e);
    }

    FlinkLyftKinesisConsumer<WindowedValue<byte[]>> source =
        FlinkLyftKinesisConsumer.create(
            stream, deserializationSchema, properties);
    source.setShardAssigner(
        InitialRoundRobinKinesisShardAssigner.fromInitialShards(
            properties, stream, context.getExecutionEnvironment().getConfig().getParallelism()));
    context.addDataStream(
        Iterables.getOnlyElement(pTransform.getOutputsMap().values()),
        context.getExecutionEnvironment().addSource(source));
  }

  /**
   * Deserializer for native Flink Kafka source that produces {@link WindowedValue} expected by Beam
   * operators.
   */
  // TODO: switch to KinesisDeserializationSchema once FlinkLyftKinesisConsumer.create supports it
  private static class KinesisByteArrayWindowedValueSchema
      implements DeserializationSchema<WindowedValue<byte[]>> {
    private static final long serialVersionUID = -1L;

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
    public WindowedValue<byte[]> deserialize(byte[] recordValue) {
      return WindowedValue.valueInGlobalWindow(recordValue);
    }

    @Override
    public boolean isEndOfStream(WindowedValue<byte[]> nextElement) {
      return false;
    }
  }

  /**
   * Deserializer for Lyft's kinesis event format, which is a zlib-compressed, base64-encoded,
   * json array of event objects. This schema tags events with the occurred_at time of the oldest
   * event in the message.
   *
   * The output of this schema is utf-8 encoded json.
   */
  private static class LyftBeamKinesisSchema implements DeserializationSchema<WindowedValue<byte[]>> {
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
    public WindowedValue<byte[]> deserialize(byte[] message) throws IOException {
      String inflatedString = inflate(message);

      JsonNode events = mapper.readTree(inflatedString);
      if (!events.isArray()) {
        throw new IOException("Events is not an array");
      };

      Iterator<JsonNode> iter = events.elements();
      long timestamp = Long.MAX_VALUE;
      while (iter.hasNext()) {
        JsonNode occurredAt = iter.next().path(EventField.EventOccurredAt.fieldName());
        try {
          timestamp = Math.min(parseDateTime(occurredAt.textValue()), timestamp);
        } catch (DateTimeParseException e) {
          // skip this timestamp
        }

        // if we didn't find any valid timestamps, use Long.MIN_VALUE
        if (timestamp == Long.MAX_VALUE) {
          timestamp = Long.MIN_VALUE;
        }
      }

      return WindowedValue.timestampedValueInGlobalWindow(
          inflatedString.getBytes(Charset.forName("UTF-8")), new Instant(timestamp));
    }

    @Override
    public boolean isEndOfStream(WindowedValue<byte[]> nextElement) {
      return false;
    }

    @Override
    public TypeInformation<WindowedValue<byte[]>> getProducedType() {
      return ti;
    }
  }
}
