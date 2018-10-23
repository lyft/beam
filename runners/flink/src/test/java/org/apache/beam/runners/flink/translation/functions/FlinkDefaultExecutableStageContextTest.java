package org.apache.beam.runners.flink.translation.functions;

import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.flink.translation.functions.FlinkDefaultExecutableStageContext.MultiInstanceFactory;
import org.apache.beam.runners.flink.translation.functions.ReferenceCountingFlinkExecutableStageContextFactory.WrappedContext;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.vendor.protobuf.v3.com.google.protobuf.Struct;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FlinkDefaultExecutableStageContext}. */
@RunWith(JUnit4.class)
public class FlinkDefaultExecutableStageContextTest {
  private static JobInfo constructJobInfo(long parallellism) {
    PortablePipelineOptions portableOptions =
        PipelineOptionsFactory.as(PortablePipelineOptions.class);
    portableOptions.setSdkWorkerParallelism(parallellism);

    Struct pipelineOptions = PipelineOptionsTranslation.toProto(portableOptions);
    return JobInfo.create("job-id", "job-name", "retrieval-token", pipelineOptions);
  }

  @Test
  public void testMultiInstanceFactory() {
    JobInfo jobInfo = constructJobInfo(2);

    WrappedContext f1 = (WrappedContext) MultiInstanceFactory.MULTI_INSTANCE.get(jobInfo);
    WrappedContext f2 = (WrappedContext) MultiInstanceFactory.MULTI_INSTANCE.get(jobInfo);
    WrappedContext f3 = (WrappedContext) MultiInstanceFactory.MULTI_INSTANCE.get(jobInfo);

    Assert.assertNotEquals("We should create two different factories", f1.context, f2.context);
    Assert.assertEquals(
        "Future calls should be round-robbined to those two factories", f1.context, f3.context);
  }

  @Test
  public void testDefault() {
    JobInfo jobInfo = constructJobInfo(0);

    int expectedParallelism = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);

    WrappedContext f1 = (WrappedContext) MultiInstanceFactory.MULTI_INSTANCE.get(jobInfo);
    for (int i = 1; i < expectedParallelism; i++) {
      Assert.assertNotEquals(
          "We should create " + expectedParallelism + " different factories",
          f1.context,
          ((WrappedContext) MultiInstanceFactory.MULTI_INSTANCE.get(jobInfo)).context);
    }

    Assert.assertEquals(
        "Future calls should be round-robbined to those",
        f1.context,
        ((WrappedContext) MultiInstanceFactory.MULTI_INSTANCE.get(jobInfo)).context);
  }
}
