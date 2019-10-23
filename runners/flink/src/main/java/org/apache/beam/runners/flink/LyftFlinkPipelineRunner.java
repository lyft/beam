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
import java.io.PrintStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.beam.runners.fnexecution.environment.ProcessManager;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.program.OptimizerPlanEnvironment;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Launch a Beam pipeline via external SDK driver program in the Flink {@link OptimizerPlanEnvironment}.
 *
 * <p>This entry point can be used to execute an external program to launch a Beam pipeline within
 * the Flink {@link OptimizerPlanEnvironment}, which is present when running a job via the REST
 * API.
 *
 * <p>Designed for non-interactive Flink REST client and container with the Beam job server jar
 * and SDK client (for example when using the FlinkK8sOperator).
 *
 * <p>Eliminates the need to build jar files with materialized pipeline protos offline. Allows
 * the driver program to access actual execution environment and services, on par with code
 * executed by SDK workers.
 *
 * <p>The entry point starts the job server and provides the endpoint to the the driver program.
 *
 * <p>The driver program constructs the Beam pipeline and submits it to the job service.
 *
 * <p>The job service submits the pipeline into the plan environment and returns the "detached"
 * status to the driver program.
 *
 * <p>Upon completion of the driver program, the entry points signals completion of job construction
 * to the environment and terminates.
 *
 * <p>Finally Flink launches the job.
 */
public class LyftFlinkPipelineRunner {
  private static final Logger LOG = LoggerFactory.getLogger(LyftFlinkPipelineRunner.class);
  private static String DRIVER_CMD_FLAGS = "--job_endpoint=localhost:%s";

  private final String driverCmd;
  private FlinkJobServerDriver driver = null;
  private Thread driverThread = null;
  private int jobPort = 55555;

  public LyftFlinkPipelineRunner(String driverCmd) {
    this.driverCmd = driverCmd;
  }

  /** Main method to be called within the Flink OptimizerPlanEnvironment. */
  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(
        ExecutionEnvironment.getExecutionEnvironment() instanceof OptimizerPlanEnvironment,
        "Can only execute in OptimizerPlanEnvironment");
    LOG.info("entry points args: %s", Arrays.asList(args));
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
    final PrintStream oldErr = System.err;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream newErr = new PrintStream(baos);
    try {
      System.setErr(newErr);
      driver =
          FlinkJobServerDriver.fromParams(
              new String[] {"--job-port=" + jobPort, "--artifact-port=0", "--expansion-port=0"});
      driverThread = new Thread(driver);
      driverThread.start();
      boolean success = false;

      // TODO: check for job service ready
      Thread.sleep(5000);
      success = true;

      while (!success) {
        newErr.flush();
        String output = baos.toString(Charsets.UTF_8.name());
        if (output.contains("JobService started on localhost:")
            && output.contains("ArtifactStagingService started on localhost:")
            && output.contains("ExpansionService started on localhost:")) {
          success = true;
        } else {
          Thread.sleep(100);
        }
      }

      if (!driverThread.isAlive()) {
        throw new IllegalStateException("Job service thread is not alive");
      }
    } finally {
      System.setErr(oldErr);
    }
  }

  private void runDriverProgram() throws Exception {
    ProcessManager processManager = ProcessManager.create();
    String executable = "bash";
    List<String> args =
        ImmutableList.of("-c", String.format("exec %s " + DRIVER_CMD_FLAGS, driverCmd, jobPort));
    String processId = "client1";

    Duration timeout = Duration.ofSeconds(30);
    Deadline deadline = Deadline.fromNow(timeout);
    try {
      final Process driverProcess =
          processManager
              .startProcess(processId, executable, args, System.getenv())
              .getUnderlyingProcess();
      LOG.info("Started driver program");
      while (driverProcess.isAlive() && deadline.hasTimeLeft()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException interruptEx) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(interruptEx);
        }
      }
      if (driverProcess.isAlive()) {
        String msg =
            String.format(
                "Timeout of %s waiting for command '%s' to submit the job.", deadline, args);
        throw new TimeoutException(msg);
      } else {
        if (driverProcess.exitValue() == 0) {
          // convey to the environment that the job was successfully constructed
          throw new OptimizerPlanEnvironment.ProgramAbortException();
        }
        throw new RuntimeException("Driver program failed.");
      }
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
}
