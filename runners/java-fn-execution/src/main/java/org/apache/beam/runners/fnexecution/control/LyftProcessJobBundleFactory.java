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
package org.apache.beam.runners.fnexecution.control;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.ProcessEnvironment;
import org.apache.beam.runners.fnexecution.environment.ProcessManager;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.vendor.protobuf.v3.com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link JobBundleFactory} that uses direct Python SDK harness process environments.
 *
 * <p>Assumes that all dependencies are present on the host machine and doesn't need to support
 * artifact retrieval. In the Python development environment, simply run the JVM within the Python
 * virtualenv.
 */
public class LyftProcessJobBundleFactory extends ProcessJobBundleFactory {

  /**
   * Required since super constructor calls getEnvironmentFactory before instance is fully
   * initialized.
   */
  private static ThreadLocal<JobInfo> JOB_INFO = new ThreadLocal<>();

  public static LyftProcessJobBundleFactory create(JobInfo jobInfo) throws Exception {
    try {
      JOB_INFO.set(jobInfo);
      return new LyftProcessJobBundleFactory(jobInfo);
    } finally {
      JOB_INFO.remove();
    }
  }

  private LyftProcessJobBundleFactory(JobInfo jobInfo) throws Exception {
    super(jobInfo);
  }

  @Override
  protected EnvironmentFactory getEnvironmentFactory(
      GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
      GrpcFnServer<GrpcLoggingService> loggingServiceServer,
      GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
      GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
      ControlClientPool.Source clientSource,
      IdGenerator idGenerator) {

    return new PythonEnvironmentFactory(
        Preconditions.checkNotNull(JOB_INFO.get(), "jobInfo is null"),
        controlServiceServer,
        loggingServiceServer,
        retrievalServiceServer,
        provisioningServiceServer,
        idGenerator,
        clientSource);
  }

  private static class PythonEnvironmentFactory implements EnvironmentFactory {
    private static final Logger LOG = LoggerFactory.getLogger(PythonEnvironmentFactory.class);

    // the command that will be run via bash
    // default assumes execution within already activated virtualenv
    // env is added for debugging purposes, output is only visible when debug logging is enabled
    private static final String SDK_HARNESS_BASH_CMD =
        System.getProperty(
            "lyft.pythonWorkerCmd", "env; python -m apache_beam.runners.worker.sdk_worker_main");
    private static final int HARNESS_CONNECT_TIMEOUT_MINS = 5;

    private final JobInfo jobInfo;
    private final ProcessManager processManager;
    private final GrpcFnServer<FnApiControlClientPoolService> controlServiceServer;
    private final GrpcFnServer<GrpcLoggingService> loggingServiceServer;
    private final GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer;
    private final GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer;
    private final IdGenerator idGenerator;
    private final ControlClientPool.Source clientSource;

    private PythonEnvironmentFactory(
        JobInfo jobInfo,
        GrpcFnServer<FnApiControlClientPoolService> controlServiceServer,
        GrpcFnServer<GrpcLoggingService> loggingServiceServer,
        GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer,
        GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer,
        IdGenerator idGenerator,
        ControlClientPool.Source clientSource) {
      this.jobInfo = jobInfo;
      this.processManager = ProcessManager.create();
      this.controlServiceServer = controlServiceServer;
      this.loggingServiceServer = loggingServiceServer;
      this.retrievalServiceServer = retrievalServiceServer;
      this.provisioningServiceServer = provisioningServiceServer;
      this.idGenerator = idGenerator;
      this.clientSource = clientSource;
    }

    /** Creates a new, active {@link RemoteEnvironment} backed by a forked process. */
    @Override
    public RemoteEnvironment createEnvironment(RunnerApi.Environment environment) throws Exception {
      String workerId = idGenerator.getId();

      String pipelineOptionsJson = JsonFormat.printer().print(jobInfo.pipelineOptions());
      HashMap<String, String> env = new HashMap<>();
      env.put("WORKER_ID", workerId);
      env.put("PIPELINE_OPTIONS", pipelineOptionsJson);
      env.put(
          "LOGGING_API_SERVICE_DESCRIPTOR",
          loggingServiceServer.getApiServiceDescriptor().toString());
      env.put(
          "CONTROL_API_SERVICE_DESCRIPTOR",
          controlServiceServer.getApiServiceDescriptor().toString());
      //env.put("SEMI_PERSISTENT_DIRECTORY", "/tmp");

      String executable = "bash";
      List<String> args = ImmutableList.of("-c", SDK_HARNESS_BASH_CMD);

      LOG.info("Creating Process with ID {}", workerId);
      // Wrap the blocking call to clientSource.get in case an exception is thrown.
      InstructionRequestHandler instructionHandler = null;
      try {
        processManager.startProcess(workerId, executable, args, env);
        // Wait for the SDK harness to connect to the gRPC server.
        long timeoutMillis =
            System.currentTimeMillis()
                + Duration.ofMinutes(HARNESS_CONNECT_TIMEOUT_MINS).toMillis();
        while (instructionHandler == null && System.currentTimeMillis() < timeoutMillis) {
          try {
            instructionHandler = clientSource.take(workerId, Duration.ofSeconds(30));
          } catch (TimeoutException timeoutEx) {
            LOG.info(
                "Still waiting for connection from command '{}' for worker id {}",
                SDK_HARNESS_BASH_CMD,
                workerId);
          } catch (InterruptedException interruptEx) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(interruptEx);
          }
        }

        if (instructionHandler == null) {
          String msg =
              String.format(
                  "Timeout of %d minutes waiting for worker '%s' with command '%s' to connect to the service endpoint.",
                  HARNESS_CONNECT_TIMEOUT_MINS, workerId, SDK_HARNESS_BASH_CMD);
          throw new TimeoutException(msg);
        }

      } catch (Exception e) {
        try {
          processManager.stopProcess(workerId);
        } catch (Exception processKillException) {
          e.addSuppressed(processKillException);
        }
        throw e;
      }

      return ProcessEnvironment.create(processManager, environment, workerId, instructionHandler);
    }
  }
}
