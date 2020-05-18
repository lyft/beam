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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Properties;
import java.util.TimeZone;
import java.util.zip.Deflater;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.flink.LyftFlinkStreamingPortableTranslations.LyftBase64ZlibJsonSchema;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link LyftFlinkStreamingPortableTranslations}
 */
public class LyftFlinkStreamingPortableTranslationsTest {

  @Mock
  private FlinkStreamingPortablePipelineTranslator.StreamingTranslationContext streamingContext;

  @Mock
  private StreamExecutionEnvironment streamingEnvironment;

  @Before
  public void before() {
    MockitoAnnotations.initMocks(this);
    when(streamingContext.getExecutionEnvironment())
            .thenReturn(streamingEnvironment);
  }

  @Test
  public void testBeamKinesisSchema() throws IOException {
    // [{"event_id": 1, "occurred_at": "2018-10-27 00:20:02.900"}]"
    byte[] message =
        Base64.getDecoder()
            .decode(
                "eJyLrlZKLUvNK4nPTFGyUjDUUVDKT04uLSpKTYlPLAGKKBkZ"
                    + "GFroGhroGpkrGBhYGRlYGRjpWRoYKNXGAgARiA/1");

    LyftBase64ZlibJsonSchema schema = new LyftBase64ZlibJsonSchema();
    WindowedValue<byte[]> value = schema.deserialize(message, "", "", 0, "", "");

    Assert.assertArrayEquals(message, value.getValue());
    Assert.assertEquals(1540599602000L, value.getTimestamp().getMillis());
  }

  @Test
  public void testBeamKinesisSchemaLongTimestamp() throws IOException {
    // [{"event_id": 1, "occurred_at": "2018-10-27 00:20:02.900"}]"
    byte[] message =
        Base64.getDecoder()
            .decode(
                "eJyLrlZKLUvNK4nPTFGyUjDUUVDKT04uL" + "SpKTYlPLAGJmJqYGBhbGlsYmhlZ1MYCAGYeDek=");

    LyftBase64ZlibJsonSchema schema = new LyftBase64ZlibJsonSchema();
    WindowedValue<byte[]> value = schema.deserialize(message, "", "", 0, "", "");

    Assert.assertArrayEquals(message, value.getValue());
    Assert.assertEquals(1544039381628L, value.getTimestamp().getMillis());
  }

  @Test
  public void testBeamKinesisSchemaNoTimestamp() throws IOException {
    byte[] message = encode("[{\"event_id\": 1}]");

    LyftBase64ZlibJsonSchema schema = new LyftBase64ZlibJsonSchema();
    WindowedValue<byte[]> value = schema.deserialize(message, "", "", 0, "", "");

    Assert.assertArrayEquals(message, value.getValue());
    Assert.assertEquals(Long.MIN_VALUE, value.getTimestamp().getMillis());
  }

  @Test
  public void testBeamKinesisSchemaMultipleRecords() throws IOException {
    // [{"event_id": 1, "occurred_at": "2018-10-27 00:20:02.900"},
    //  {"event_id": 2, "occurred_at": "2018-10-27 00:38:13.005"}]
    byte[] message =
        Base64.getDecoder()
            .decode(
                "eJyLrlZKLUvNK4nPTFGyUjDUUVDKT04uLSpKTYlPLAGKKBkZGFroGhroGpkr"
                    + "GBhYGRlYGRjpWRoYKNXqKKBoNSKk1djCytBYz8DAVKk2FgC35B+F");

    LyftBase64ZlibJsonSchema schema = new LyftBase64ZlibJsonSchema();
    WindowedValue<byte[]> value = schema.deserialize(message, "", "", 0, "", "");

    Assert.assertArrayEquals(message, value.getValue());
    // we should output the oldest timestamp in the bundle
    Assert.assertEquals(1540599602000L, value.getTimestamp().getMillis());
  }

  @Test
  public void testBeamKinesisSchemaFutureOccurredAtTimestamp() throws Exception {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

    long loggedAtMillis = sdf.parse("2018-10-27 00:10:02.000000").getTime();
    String events =
        "[{\"event_id\": 1, \"occurred_at\": \"2018-10-27 00:20:02.900\", \"logged_at\": "
            + loggedAtMillis / 1000
            + "}]";
    byte[] message = encode(events);
    LyftBase64ZlibJsonSchema schema = new LyftBase64ZlibJsonSchema();
    WindowedValue<byte[]> value = schema.deserialize(message, "", "", 0, "", "");

    Assert.assertArrayEquals(message, value.getValue());
    Assert.assertEquals(loggedAtMillis, value.getTimestamp().getMillis());
  }

  @Test
  public void testRequiredPropertiesForKafkaInput() throws JsonProcessingException {

    LyftFlinkStreamingPortableTranslations portableTranslations = new LyftFlinkStreamingPortableTranslations();
    String id = "1";
    String topicName = "kinesis_to_kafka";

    Properties kafkaConsumerProps = new Properties();
    kafkaConsumerProps.put("group.id", "testing");
    kafkaConsumerProps.put("bootstrap.servers", "test-hdd.lyft.com");

    ImmutableMap<String, Object> payload =
            ImmutableMap.<String, Object>builder()
                    .put("topic", topicName)
                    .put("properties", kafkaConsumerProps)
                    .put("username", "read_kinesis_to_kafka")
                    .put("password", "abcde1234")
                    .build();

    byte[] payloadBytes = new ObjectMapper().writeValueAsBytes(payload);
    RunnerApi.Pipeline pipeline = createPipeline(id, payloadBytes);

    portableTranslations.translateKafkaInput(id, pipeline, streamingContext);

    ArgumentCaptor<FlinkKafkaConsumer011> kafkaSourceCaptor = ArgumentCaptor.forClass(FlinkKafkaConsumer011.class);
    ArgumentCaptor<String> kafkaSourceNameCaptor = ArgumentCaptor.forClass(String.class);
    verify(streamingEnvironment).addSource(kafkaSourceCaptor.capture(), kafkaSourceNameCaptor.capture());
    Assert.assertEquals(WindowedValue.class, kafkaSourceCaptor.getValue().getProducedType().getTypeClass());
    Assert.assertTrue(kafkaSourceNameCaptor.getValue().contains(topicName));

  }

  @Test(expected = NullPointerException.class)
  public void testMissingRequiredCredentialsForKafkaInput() throws JsonProcessingException {

    LyftFlinkStreamingPortableTranslations portableTranslations = new LyftFlinkStreamingPortableTranslations();
    String id = "1";
    String topicName = "kinesis_to_kafka";

    Properties kafkaConsumerProps = new Properties();
    kafkaConsumerProps.put("group.id", "testing");
    kafkaConsumerProps.put("bootstrap.servers", "test-hdd.lyft.com");

    ImmutableMap<String, Object> payload =
            ImmutableMap.<String, Object>builder()
                    .put("topic", topicName)
                    .put("properties", kafkaConsumerProps)
                    .build();

    byte[] payloadBytes = new ObjectMapper().writeValueAsBytes(payload);
    RunnerApi.Pipeline pipeline = createPipeline(id, payloadBytes);

    portableTranslations.translateKafkaInput(id, pipeline, streamingContext);
    Assert.fail("Should fail due to empty username and password! ");

  }

  @Test
  public void testKafkaInputForLocalEnvironment() throws JsonProcessingException {

    LyftFlinkStreamingPortableTranslations portableTranslations = new LyftFlinkStreamingPortableTranslations();
    String id = "1";
    String topicName = "kinesis_to_kafka";

    Properties kafkaConsumerProps = new Properties();
    kafkaConsumerProps.put("group.id", "testing");
    kafkaConsumerProps.put("bootstrap.servers", "kafka-server.devbox.lyft.net");

    ImmutableMap<String, Object> payload =
            ImmutableMap.<String, Object>builder()
                    .put("topic", topicName)
                    .put("properties", kafkaConsumerProps)
                    .build();
    byte[] payloadBytes = new ObjectMapper().writeValueAsBytes(payload);
    RunnerApi.Pipeline pipeline = createPipeline(id, payloadBytes);

    // test
    portableTranslations.translateKafkaInput(id, pipeline, streamingContext);

    // verify
    ArgumentCaptor<FlinkKafkaConsumer011> kafkaSourceCaptor = ArgumentCaptor.forClass(FlinkKafkaConsumer011.class);
    ArgumentCaptor<String> kafkaSourceNameCaptor = ArgumentCaptor.forClass(String.class);
    verify(streamingEnvironment).addSource(kafkaSourceCaptor.capture(), kafkaSourceNameCaptor.capture());
    Assert.assertEquals(WindowedValue.class, kafkaSourceCaptor.getValue().getProducedType().getTypeClass());
    Assert.assertTrue(kafkaSourceNameCaptor.getValue().contains(topicName));

  }

  @Test
  public void testRequiredPropertiesForKafkaSink() throws JsonProcessingException {

    LyftFlinkStreamingPortableTranslations portableTranslations = new LyftFlinkStreamingPortableTranslations();
    String id = "1";
    String topicName = "kinesis_to_kafka";

    Properties kakfaProducerProps = new Properties();
    kakfaProducerProps.put("bootstrap.servers", "test-hdd.lyft.com");

    ImmutableMap<String, Object> payload =
            ImmutableMap.<String, Object>builder()
                    .put("topic", topicName)
                    .put("properties", kakfaProducerProps)
                    .put("username", "read_kinesis_to_kafka")
                    .put("password", "abcde1234")
                    .build();

    byte[] payloadBytes = new ObjectMapper().writeValueAsBytes(payload);
    RunnerApi.Pipeline pipeline = createPipeline(id, payloadBytes);

    DataStream mockDataStream = mock(DataStream.class);
    SingleOutputStreamOperator mockStreamOperator = mock(SingleOutputStreamOperator.class);
    DataStreamSink mockStreamSink = mock(DataStreamSink.class);

    when(mockStreamOperator.addSink(any())).thenReturn(mockStreamSink);
    when(mockDataStream.transform(anyString(), any(), any())).thenReturn(mockStreamOperator);
    when(streamingContext.getDataStreamOrThrow("fake_pcollection_id")).thenReturn(mockDataStream);

    portableTranslations.translateKafkaSink(id, pipeline, streamingContext);

    verify(streamingContext).getDataStreamOrThrow("fake_pcollection_id");
    verify(mockDataStream).transform(anyString(), any(), any());
    ArgumentCaptor<FlinkKafkaProducer011> kafkaSinkCaptor = ArgumentCaptor.forClass(FlinkKafkaProducer011.class);
    ArgumentCaptor<String> kafkaSinkNameCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockStreamSink).name(kafkaSinkNameCaptor.capture());
    verify(mockStreamOperator).addSink(kafkaSinkCaptor.capture());

    Assert.assertTrue(kafkaSinkNameCaptor.getValue().contains(topicName));
    Assert.assertEquals(FlinkKafkaProducer011.class, kafkaSinkCaptor.getValue().getClass());

  }

  @Test
  public void testKafkaSinkForLocalEnvironment() throws JsonProcessingException {

    LyftFlinkStreamingPortableTranslations portableTranslations = new LyftFlinkStreamingPortableTranslations();
    String id = "1";
    String topicName = "kinesis_to_kafka";

    Properties kakfaProducerProps = new Properties();
    kakfaProducerProps.put("bootstrap.servers", "kafka-server.devbox.lyft.net");

    ImmutableMap<String, Object> payload =
            ImmutableMap.<String, Object>builder()
                    .put("topic", topicName)
                    .put("properties", kakfaProducerProps)
                    .build();

    byte[] payloadBytes = new ObjectMapper().writeValueAsBytes(payload);
    RunnerApi.Pipeline pipeline = createPipeline(id, payloadBytes);

    DataStream mockDataStream = mock(DataStream.class);
    SingleOutputStreamOperator mockStreamOperator = mock(SingleOutputStreamOperator.class);
    DataStreamSink mockStreamSink = mock(DataStreamSink.class);

    when(mockStreamOperator.addSink(any())).thenReturn(mockStreamSink);
    when(mockDataStream.transform(anyString(), any(), any())).thenReturn(mockStreamOperator);
    when(streamingContext.getDataStreamOrThrow("fake_pcollection_id")).thenReturn(mockDataStream);

    portableTranslations.translateKafkaSink(id, pipeline, streamingContext);

    ArgumentCaptor<FlinkKafkaProducer011> kafkaSinkCaptor = ArgumentCaptor.forClass(FlinkKafkaProducer011.class);
    ArgumentCaptor<String> kafkaSinkNameCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockStreamSink).name(kafkaSinkNameCaptor.capture());
    verify(mockStreamOperator).addSink(kafkaSinkCaptor.capture());

    Assert.assertTrue(kafkaSinkNameCaptor.getValue().contains(topicName));
    Assert.assertEquals(FlinkKafkaProducer011.class, kafkaSinkCaptor.getValue().getClass());

  }

  /**
   * Creates a new {@link RunnerApi.Pipeline} with payload.
   * @param id
   * @param payload
   * @return
   */
  private RunnerApi.Pipeline createPipeline(String id, byte[] payload) {

    RunnerApi.PTransform pTransform = RunnerApi.PTransform.newBuilder()
            .putOutputs("fake_output_name", "fake_pcollection_id")
            .putInputs("fake_input_name", "fake_pcollection_id")
            .setSpec(
                    RunnerApi.FunctionSpec.newBuilder()
                            .setPayload(ByteString.copyFrom(payload))).build();

    RunnerApi.Pipeline pipeline = RunnerApi.Pipeline.newBuilder()
            .setComponents(RunnerApi.Components.newBuilder()
                    .putTransforms(id, pTransform)
                    .build())
            .build();

    return pipeline;
  }

  private static byte[] encode(String data) throws IOException {
    Deflater deflater = new Deflater();
    deflater.setInput(data.getBytes(Charset.defaultCharset()));
    deflater.finish();
    byte[] buf = new byte[4096];
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length())) {
      while (!deflater.finished()) {
        int count = deflater.deflate(buf);
        bos.write(buf, 0, count);
      }
      return bos.toByteArray();
    }
  }
}
