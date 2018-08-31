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

public class LyftProcessJobBundleFactory extends ProcessJobBundleFactory {

  /**
   * Required since the super constructor calls getEnvironmentFactory the instance is initialized.
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

    return new LyftPythonEnvironmentFactory(
        Preconditions.checkNotNull(JOB_INFO.get(), "jobInfo is null"),
        controlServiceServer,
        loggingServiceServer,
        retrievalServiceServer,
        provisioningServiceServer,
        idGenerator,
        clientSource);
  }

  private static class LyftPythonEnvironmentFactory implements EnvironmentFactory {
    private static final Logger LOG = LoggerFactory.getLogger(LyftPythonEnvironmentFactory.class);

    private final JobInfo jobInfo;
    private final ProcessManager processManager;
    private final GrpcFnServer<FnApiControlClientPoolService> controlServiceServer;
    private final GrpcFnServer<GrpcLoggingService> loggingServiceServer;
    private final GrpcFnServer<ArtifactRetrievalService> retrievalServiceServer;
    private final GrpcFnServer<StaticGrpcProvisionService> provisioningServiceServer;
    private final IdGenerator idGenerator;
    private final ControlClientPool.Source clientSource;

    private LyftPythonEnvironmentFactory(
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
      env.put("SEMI_PERSISTENT_DIRECTORY", "/tmp");

      String sdkHarnessEntrypoint = "apache_beam.runners.worker.sdk_worker_main";
      String executable = "bash";
      //List<String> args = ImmutableList.of("-c", String.format("'env; python -m %s'", sdkHarnessEntrypoint));
      List<String> args =
          ImmutableList.of("-c", String.format("env; python -m %s", sdkHarnessEntrypoint));

      LOG.info("Creating Process with ID {}", workerId);
      // Wrap the blocking call to clientSource.get in case an exception is thrown.
      InstructionRequestHandler instructionHandler = null;
      try {
        processManager.startProcess(workerId, executable, args, env);
        // Wait on a client from the gRPC server.
        while (instructionHandler == null) {
          try {
            instructionHandler = clientSource.take(workerId, Duration.ofMinutes(2));
          } catch (TimeoutException timeoutEx) {
            LOG.info(
                "Still waiting for startup of environment '{}' for worker id {}",
                executable,
                workerId);
          } catch (InterruptedException interruptEx) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(interruptEx);
          }
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
