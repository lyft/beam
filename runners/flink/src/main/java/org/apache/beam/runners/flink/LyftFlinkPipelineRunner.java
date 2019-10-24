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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.environment.ProcessManager;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvocation;
import org.apache.beam.runners.fnexecution.jobsubmission.JobInvoker;
import org.apache.beam.runners.fnexecution.jobsubmission.PortablePipelineResult;
import org.apache.beam.runners.fnexecution.jobsubmission.PortablePipelineRunner;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.OptimizerPlanEnvironment;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launch a Beam pipeline via external SDK driver program in the Flink {@link
 * OptimizerPlanEnvironment}.
 *
 * <p>This entry point can be used to execute an external program to launch a Beam pipeline within
 * the Flink {@link OptimizerPlanEnvironment}, which is present when running a job via the REST API.
 *
 * <p>Designed for non-interactive Flink REST client and container with the Beam job server jar and
 * SDK client (for example when using the FlinkK8sOperator).
 *
 * <p>Eliminates the need to build jar files with materialized pipeline protos offline. Allows the
 * driver program to access actual execution environment and services, on par with code executed by
 * SDK workers.
 *
 * <p>The entry point starts the job server and provides the endpoint to the the driver program.
 *
 * <p>The driver program constructs the Beam pipeline and submits it to the job service.
 *
 * <p>The job service defers execution of the pipeline to the plan environment and returns the
 * "detached" status to the driver program.
 *
 * <p>Upon arrival of the job invocation, the entry point executes the runner, which prepares
 * ("executes") the Flink job through the plan environment.
 *
 * <p>Finally Flink launches the job.
 */
public class LyftFlinkPipelineRunner {
  private static final Logger LOG = LoggerFactory.getLogger(LyftFlinkPipelineRunner.class);
  private static String DRIVER_CMD_FLAGS = "--job_endpoint=%s";

  private final String driverCmd;
  private FlinkJobServerDriver driver;
  private Thread driverThread;
  private DetachedJobInvokerFactory jobInvokerFactory;
  private int jobPort = 55555;

  public LyftFlinkPipelineRunner(String driverCmd) {
    this.driverCmd = driverCmd;
  }

  /** Main method to be called within the Flink OptimizerPlanEnvironment. */
  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(
        ExecutionEnvironment.getExecutionEnvironment() instanceof OptimizerPlanEnvironment,
        "Can only execute in OptimizerPlanEnvironment");
    LOG.info("entry points args: {}", Arrays.asList(args));
    EntryPointConfiguration configuration = parseArgs(args);
    LyftFlinkPipelineRunner runner = new LyftFlinkPipelineRunner(configuration.driverCmd);
    try {
      runner.startJobService();
      runner.runDriverProgram();
    } catch (Exception e) {
      throw new RuntimeException(String.format("Job %s failed.", configuration.driverCmd), e);
    } finally {
      LOG.info("Stopping job service");
      runner.stopJobService();
    }
    LOG.info("Job submitted successfully.");
  }

  private static class EntryPointConfiguration {
    @Option(
        name = "--driver-cmd",
        required = true,
        usage =
            "Command that launches the Python driver program. "
                + "(The job service endpoint will be appended as --job_endpoint=localhost:<port>.)")
    private String driverCmd = null;
  }

  private static EntryPointConfiguration parseArgs(String[] args) {
    EntryPointConfiguration configuration = new EntryPointConfiguration();
    CmdLineParser parser = new CmdLineParser(configuration);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      LOG.error("Unable to parse command line arguments.", e);
      parser.printUsage(System.err);
      throw new IllegalArgumentException("Unable to parse command line arguments.", e);
    }
    return configuration;
  }

  private void startJobService() throws Exception {
    jobInvokerFactory = new DetachedJobInvokerFactory();
    driver =
        FlinkJobServerDriver.fromConfig(
            FlinkJobServerDriver.fromParams(
                new String[] {"--job-port=" + jobPort, "--artifact-port=0", "--expansion-port=0"}),
            jobInvokerFactory);
    driverThread = new Thread(driver);
    driverThread.start();

    Duration timeout = Duration.ofSeconds(30);
    Deadline deadline = Deadline.fromNow(timeout);
    while (driver.getJobServerUrl() == null && deadline.hasTimeLeft()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException interruptEx) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(interruptEx);
      }
    }

    if (!driverThread.isAlive()) {
      throw new IllegalStateException("Job service thread is not alive");
    }

    if (driver.getJobServerUrl() == null) {
      String msg = String.format("Timeout of %s waiting for job service to start.", deadline);
      throw new TimeoutException(msg);
    }
  }

  private void runDriverProgram() throws Exception {
    ProcessManager processManager = ProcessManager.create();
    String executable = "bash";
    List<String> args =
        ImmutableList.of(
            "-c",
            String.format("exec %s " + DRIVER_CMD_FLAGS, driverCmd, driver.getJobServerUrl()));
    String processId = "client1";

    try {
      final ProcessManager.RunningProcess driverProcess =
          processManager.startProcess(processId, executable, args, System.getenv());
      driverProcess.isAliveOrThrow();
      LOG.info("Started driver program");

      // await effect of the driver program submitting the job
      jobInvokerFactory.executeDetachedJob();
    } catch (Exception e) {
      try {
        processManager.stopProcess(processId);
      } catch (Exception processKillException) {
        e.addSuppressed(processKillException);
      }
      throw e;
    }
  }

  private void stopJobService() throws InterruptedException {
    if (driver != null) {
      driver.stop();
    }
    if (driverThread != null) {
      driverThread.interrupt();
      driverThread.join();
    }
  }

  private class DetachedJobInvokerFactory implements FlinkJobServerDriver.JobInvokerFactory {

    private CountDownLatch latch = new CountDownLatch(1);
    private PortablePipelineRunner actualPipelineRunner;
    private RunnerApi.Pipeline pipeline;
    private JobInfo jobInfo;

    private PortablePipelineRunner handoverPipelineRunner =
        new PortablePipelineRunner() {
          @Override
          public PortablePipelineResult run(RunnerApi.Pipeline pipeline, JobInfo jobInfo) {
            DetachedJobInvokerFactory.this.pipeline = pipeline;
            DetachedJobInvokerFactory.this.jobInfo = jobInfo;
            LOG.info("Pipeline execution handover for {}", jobInfo.jobId());
            latch.countDown();
            return new FlinkPortableRunnerResult.Detached();
          }
        };

    @Override
    public JobInvoker create() {
      return new FlinkJobInvoker(
          (FlinkJobServerDriver.FlinkServerConfiguration) driver.configuration) {
        @Override
        protected JobInvocation createJobInvocation(
            String invocationId,
            String retrievalToken,
            ListeningExecutorService executorService,
            RunnerApi.Pipeline pipeline,
            FlinkPipelineOptions flinkOptions,
            PortablePipelineRunner pipelineRunner) {
          // replace pipeline runner to handover execution
          actualPipelineRunner = pipelineRunner;
          return super.createJobInvocation(
              invocationId,
              retrievalToken,
              executorService,
              pipeline,
              flinkOptions,
              handoverPipelineRunner);
        }
      };
    }

    private void executeDetachedJob() throws Exception {
      Duration timeout = Duration.ofSeconds(30);
      if (latch.await(timeout.getSeconds(), TimeUnit.SECONDS)) {
        actualPipelineRunner.run(pipeline, jobInfo);
      } else {
        throw new TimeoutException(
            String.format("Timeout of %s seconds waiting for job submission.", timeout));
      }
    }
  }
}
