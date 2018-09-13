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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Properties;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.flink.FlinkStreamingPortablePipelineTranslator.PTransformTranslator;
import org.apache.beam.runners.flink.FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LyftFlinkStreamingPortableTranslations {

  private static final Logger logger =
      LoggerFactory.getLogger(LyftFlinkStreamingPortableTranslations.class.getName());

  public void addTo(
      ImmutableMap.Builder<String, PTransformTranslator<StreamingTranslationContext>>
          translatorMap) {
    translatorMap.put("lyft:flinkKafkaInput", this::translateKafkaInput);
    translatorMap.put("lyft:flinkKinesisInput", this::translateKinesisInput);
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
      Map<String, Object> params =
          mapper.readValue(pTransform.getSpec().getPayload().toByteArray(), Map.class);

      Preconditions.checkNotNull(topic = (String) params.get("topic"), "'topic' needs to be set");
      Map<?, ?> consumerProps = (Map) params.get("properties");
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
      System.out.println("###Kafka record: " + new String(message, Charset.defaultCharset()));
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
    Properties properties = new Properties();
    ObjectMapper mapper = new ObjectMapper();
    try {
      Map<String, Object> params =
          mapper.readValue(pTransform.getSpec().getPayload().toByteArray(), Map.class);

      Preconditions.checkNotNull(
          stream = (String) params.get("stream"), "'stream' needs to be set");
      Map<?, ?> consumerProps = (Map) params.get("properties");
      Preconditions.checkNotNull(consumerProps, "'properties' need to be set");
      properties.putAll(consumerProps);
    } catch (IOException e) {
      throw new RuntimeException("Could not parse KafkaConsumer properties.", e);
    }

    logger.info("Kinesis consumer for stream {} with properties {}", stream, properties);

    DataStreamSource<WindowedValue<byte[]>> source =
        context
            .getExecutionEnvironment()
            .addSource(
                new FlinkKinesisConsumer<>(
                    stream, new KinesisByteArrayWindowedValueSchema(), properties));
    context.addDataStream(Iterables.getOnlyElement(pTransform.getOutputsMap().values()), source);
  }

  /**
   * Deserializer for native Flink Kafka source that produces {@link WindowedValue} expected by Beam
   * operators.
   */
  private static class KinesisByteArrayWindowedValueSchema
      implements KinesisDeserializationSchema<WindowedValue<byte[]>> {
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
    public WindowedValue<byte[]> deserialize(
        byte[] recordValue,
        String partitionKey,
        String seqNum,
        long approxArrivalTimestamp,
        String stream,
        String shardId)
        throws IOException {
      return WindowedValue.valueInGlobalWindow(recordValue);
    }
  }
}
