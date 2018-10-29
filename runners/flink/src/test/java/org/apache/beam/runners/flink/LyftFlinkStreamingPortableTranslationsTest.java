package org.apache.beam.runners.flink;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.Base64;
import org.apache.beam.runners.flink.LyftFlinkStreamingPortableTranslations.LyftBase64ZlibJsonSchema;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Assert;
import org.junit.Test;

public class LyftFlinkStreamingPortableTranslationsTest {

  @Test
  public void testBeamKinesisSchema() throws IOException {
    // [{"event_id": 1, "occurred_at": "2018-10-27 00:20:02.900"}]"
    byte[] message = Base64.getDecoder()
        .decode("eJyLrlZKLUvNK4nPTFGyUjDUUVDKT04uLSpKTYlPLAGKKBkZ"
            + "GFroGhroGpkrGBhYGRlYGRjpWRoYKNXGAgARiA/1");

    LyftBase64ZlibJsonSchema schema = new LyftBase64ZlibJsonSchema();
    WindowedValue<byte[]> value = schema.deserialize(message, "", "", 0, "", "");

    Assert.assertArrayEquals(message, value.getValue());
    Assert.assertEquals(1540599602000L, value.getTimestamp().getMillis());
  }

  @Test
  public void testBeamKinesisSchemaNoTimestamp() throws IOException {
    // [{"event_id": 1}]
    byte[] message = Base64.getDecoder()
        .decode("eJyLrlZKLUvNK4nPTFGyUjCsjQUANv8Fzg==");

    LyftBase64ZlibJsonSchema schema = new LyftBase64ZlibJsonSchema();
    WindowedValue<byte[]> value = schema.deserialize(message, "", "", 0, "", "");

    Assert.assertArrayEquals(message, value.getValue());
    Assert.assertEquals(Long.MIN_VALUE, value.getTimestamp().getMillis());
  }


  @Test
  public void testBeamKinesisSchemaMultipleRecords() throws IOException {
    // [{"event_id": 1, "occurred_at": "2018-10-27 00:20:02.900"},
    //  {"event_id": 2, "occurred_at": "2018-10-27 00:38:13.005"}]
    byte[] message = Base64.getDecoder()
        .decode("eJyLrlZKLUvNK4nPTFGyUjDUUVDKT04uLSpKTYlPLAGKKBkZGFroGhroGpkr"
            + "GBhYGRlYGRjpWRoYKNXqKKBoNSKk1djCytBYz8DAVKk2FgC35B+F");

    LyftBase64ZlibJsonSchema schema = new LyftBase64ZlibJsonSchema();
    WindowedValue<byte[]> value = schema.deserialize(message, "", "", 0, "", "");

    Assert.assertArrayEquals(message, value.getValue());
    // we should output the oldest timestamp in the bundle
    Assert.assertEquals(1540599602000L, value.getTimestamp().getMillis());
  }

}
