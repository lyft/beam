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

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardEnvironments;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.GrpcContextHeaderAccessorProvider;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.artifact.BeamFileSystemArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.artifact.ClassLoaderArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.ExecutableProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.BundleProcessor;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.EmbeddedEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.ExternalEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.LyftPythonEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.ProcessEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.logging.Slf4jLogWriter;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.fnexecution.provisioning.StaticGrpcProvisionService;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions.RetrievalServiceType;
import org.apache.beam.sdk.util.NoopLock;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link JobBundleFactory} for which the implementation can specify a custom {@link
 * EnvironmentFactory} for environment management. Note that returned {@link StageBundleFactory
 * stage bundle factories} are not thread-safe. Instead, a new stage factory should be created for
 * each client. {@link DefaultJobBundleFactory} initializes the Environment lazily when the forStage
 * is called for a stage.
 */
@ThreadSafe
public class DefaultJobBundleFactory implements JobBundleFactory {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultJobBundleFactory.class);
  private static final IdGenerator factoryIdGenerator = IdGenerators.incrementingLongs();

  private final String factoryId = factoryIdGenerator.getId();
  private final ImmutableList<LoadingCache<Environment, WrappedSdkHarnessClient>> environmentCaches;
  private final ImmutableList<Lock> environmentCacheLocks;
  private final AtomicInteger stageBundleFactoryCount = new AtomicInteger();
  private final Map<String, EnvironmentFactory.Provider> environmentFactoryProviderMap;
  private final ExecutorService executor;
  private final MapControlClientPool clientPool;
  private final IdGenerator stageIdGenerator;
  private final int environmentExpirationMillis;
  private final Semaphore availableCachesCount;
  private final LinkedBlockingDeque<LoadingCache<Environment, WrappedSdkHarnessClient>>
      availableCaches;
  private final boolean loadBalanceBundles;
  /** Clients which were evicted due to environment expiration but still had pending references. */
  private final Set<WrappedSdkHarnessClient> evictedActiveClients;

  private boolean closed;

  public static DefaultJobBundleFactory create(JobInfo jobInfo) {
    PipelineOptions pipelineOptions =
        PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions());
    Map<String, EnvironmentFactory.Provider> environmentFactoryProviderMap =
        ImmutableMap.of(
            BeamUrns.getUrn(StandardEnvironments.Environments.DOCKER),
            // new DockerEnvironmentFactory.Provider(pipelineOptions),
            // START LYFT CUSTOM
            new LyftPythonEnvironmentFactory.Provider(jobInfo),
            // END LYFT CUSTOM
            BeamUrns.getUrn(StandardEnvironments.Environments.PROCESS),
            new ProcessEnvironmentFactory.Provider(pipelineOptions),
            BeamUrns.getUrn(StandardEnvironments.Environments.EXTERNAL),
            new ExternalEnvironmentFactory.Provider(),
            Environments.ENVIRONMENT_EMBEDDED, // Non Public urn for testing.
            new EmbeddedEnvironmentFactory.Provider(pipelineOptions));
    return new DefaultJobBundleFactory(jobInfo, environmentFactoryProviderMap);
  }

  public static DefaultJobBundleFactory create(
      JobInfo jobInfo, Map<String, EnvironmentFactory.Provider> environmentFactoryProviderMap) {
    return new DefaultJobBundleFactory(jobInfo, environmentFactoryProviderMap);
  }

  DefaultJobBundleFactory(
      JobInfo jobInfo, Map<String, EnvironmentFactory.Provider> environmentFactoryMap) {
    IdGenerator stageIdSuffixGenerator = IdGenerators.incrementingLongs();
    this.environmentFactoryProviderMap = environmentFactoryMap;
    this.executor = Executors.newCachedThreadPool();
    this.clientPool = MapControlClientPool.create();
    this.stageIdGenerator = () -> factoryId + "-" + stageIdSuffixGenerator.getId();
    this.environmentExpirationMillis = getEnvironmentExpirationMillis(jobInfo);
    this.loadBalanceBundles = shouldLoadBalanceBundles(jobInfo);
    this.environmentCacheLocks = createEnvironmentCacheLocks(getMaxEnvironmentClients(jobInfo));
    this.environmentCaches =
        createEnvironmentCaches(
            serverFactory -> createServerInfo(jobInfo, serverFactory),
            getMaxEnvironmentClients(jobInfo));
    this.availableCachesCount = new Semaphore(environmentCaches.size(), true);
    this.availableCaches = new LinkedBlockingDeque<>(environmentCaches);
    this.evictedActiveClients = Sets.newConcurrentHashSet();
  }

  @VisibleForTesting
  DefaultJobBundleFactory(
      JobInfo jobInfo,
      Map<String, EnvironmentFactory.Provider> environmentFactoryMap,
      IdGenerator stageIdGenerator,
      ServerInfo serverInfo) {
    this.environmentFactoryProviderMap = environmentFactoryMap;
    this.executor = Executors.newCachedThreadPool();
    this.clientPool = MapControlClientPool.create();
    this.stageIdGenerator = stageIdGenerator;
    this.environmentExpirationMillis = getEnvironmentExpirationMillis(jobInfo);
    this.loadBalanceBundles = shouldLoadBalanceBundles(jobInfo);
    this.environmentCacheLocks = createEnvironmentCacheLocks(getMaxEnvironmentClients(jobInfo));
    this.environmentCaches =
        createEnvironmentCaches(serverFactory -> serverInfo, getMaxEnvironmentClients(jobInfo));
    this.availableCachesCount = new Semaphore(environmentCaches.size(), true);
    this.availableCaches = new LinkedBlockingDeque<>(environmentCaches);
    this.evictedActiveClients = Sets.newConcurrentHashSet();
  }

  private ImmutableList<Lock> createEnvironmentCacheLocks(int count) {
    ImmutableList.Builder<Lock> locksForCaches = ImmutableList.builder();
    for (int i = 0; i < count; i++) {
      final Lock refLock;
      if (environmentExpirationMillis > 0) {
        // The lock ensures there is no race condition between expiring an environment and a client
        // still attempting to use it, hence referencing it.
        refLock = new ReentrantLock(true);
      } else {
        refLock = NoopLock.get();
      }
      locksForCaches.add(refLock);
    }
    return locksForCaches.build();
  }

  private ImmutableList<LoadingCache<Environment, WrappedSdkHarnessClient>> createEnvironmentCaches(
      ThrowingFunction<ServerFactory, ServerInfo> serverInfoCreator, int count) {

    ImmutableList.Builder<LoadingCache<Environment, WrappedSdkHarnessClient>> caches =
        ImmutableList.builder();
    for (int i = 0; i < count; i++) {

      final Lock refLock = environmentCacheLocks.get(i);
      CacheBuilder<Environment, WrappedSdkHarnessClient> cacheBuilder =
          CacheBuilder.newBuilder()
              .removalListener(
                  notification -> {
                    WrappedSdkHarnessClient client = notification.getValue();
                    final int refCount;
                    try {
                      // We need to use a lock here to ensure we are not causing the environment to
                      // be removed if beforehand a StageBundleFactory has retrieved it but not yet
                      // issued ref() on it.
                      refLock.lock();
                      refCount = client.unref();
                    } finally {
                      refLock.unlock();
                    }
                    if (refCount > 0) {
                      LOG.warn(
                          "Expiring environment {} with {} remaining bundle references. Taking note to clean it up during shutdown if the references are not removed by then.",
                          notification.getKey(),
                          refCount);
                      evictedActiveClients.add(client);
                    }
                  });

      if (environmentExpirationMillis > 0) {
        cacheBuilder.expireAfterWrite(environmentExpirationMillis, TimeUnit.MILLISECONDS);
      }

      caches.add(
          cacheBuilder.build(
              new CacheLoader<Environment, WrappedSdkHarnessClient>() {
                @Override
                public WrappedSdkHarnessClient load(Environment environment) throws Exception {
                  EnvironmentFactory.Provider environmentFactoryProvider =
                      environmentFactoryProviderMap.get(environment.getUrn());
                  ServerFactory serverFactory = environmentFactoryProvider.getServerFactory();
                  ServerInfo serverInfo = serverInfoCreator.apply(serverFactory);
                  EnvironmentFactory environmentFactory =
                      environmentFactoryProvider.createEnvironmentFactory(
                          serverInfo.getControlServer(),
                          serverInfo.getLoggingServer(),
                          serverInfo.getRetrievalServer(),
                          serverInfo.getProvisioningServer(),
                          clientPool,
                          stageIdGenerator);
                  return WrappedSdkHarnessClient.wrapping(
                      environmentFactory.createEnvironment(environment), serverInfo);
                }
              }));
    }
    return caches.build();
  }

  private static int getEnvironmentExpirationMillis(JobInfo jobInfo) {
    PipelineOptions pipelineOptions =
        PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions());
    return pipelineOptions.as(PortablePipelineOptions.class).getEnvironmentExpirationMillis();
  }

  private static int getMaxEnvironmentClients(JobInfo jobInfo) {
    PortablePipelineOptions pipelineOptions =
        PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions())
            .as(PortablePipelineOptions.class);
    int maxEnvironments = MoreObjects.firstNonNull(pipelineOptions.getSdkWorkerParallelism(), 1);
    Preconditions.checkArgument(maxEnvironments >= 0, "sdk_worker_parallelism must be >= 0");
    if (maxEnvironments == 0) {
      // if this is 0, use the auto behavior of num_cores - 1 so that we leave some resources
      // available for the java process
      maxEnvironments = Math.max(Runtime.getRuntime().availableProcessors() - 1, 1);
    }
    return maxEnvironments;
  }

  private static boolean shouldLoadBalanceBundles(JobInfo jobInfo) {
    PipelineOptions pipelineOptions =
        PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions());
    return pipelineOptions.as(PortablePipelineOptions.class).getLoadBalanceBundles();
  }

  @Override
  public StageBundleFactory forStage(ExecutableStage executableStage) {
    return new SimpleStageBundleFactory(executableStage);
  }

  @Override
  public synchronized void close() throws Exception {
    if (closed) {
      return;
    }
    // The following code is written defensively to guard against any exceptions occurring
    // during shutdown. It is not visually appealing but unless it can be written more
    // defensively, there is no reason to change it.
    Exception exception = null;
    for (LoadingCache<Environment, WrappedSdkHarnessClient> environmentCache : environmentCaches) {
      try {
        // Clear the cache. This closes all active environments.
        // note this may cause open calls to be cancelled by the peer
        environmentCache.invalidateAll();
        environmentCache.cleanUp();
      } catch (Exception e) {
        if (exception != null) {
          exception.addSuppressed(e);
        } else {
          exception = e;
        }
      }
    }
    // Cleanup any left-over environments which were not properly dereferenced by the Runner, e.g.
    // when the bundle was not closed properly. This ensures we do not leak resources.
    for (WrappedSdkHarnessClient client : evictedActiveClients) {
      try {
        //noinspection StatementWithEmptyBody
        while (client.unref() > 0) {
          // Remove any pending references from the client to force closing the environment
        }
      } catch (Exception e) {
        if (exception != null) {
          exception.addSuppressed(e);
        } else {
          exception = e;
        }
      }
    }
    try {
      executor.shutdown();
    } catch (Exception e) {
      if (exception != null) {
        exception.addSuppressed(e);
      } else {
        exception = e;
      }
    }
    closed = true;
    if (exception != null) {
      throw exception;
    }
  }

  private static ImmutableMap.Builder<String, RemoteOutputReceiver<?>> getOutputReceivers(
      ExecutableProcessBundleDescriptor processBundleDescriptor,
      OutputReceiverFactory outputReceiverFactory) {
    ImmutableMap.Builder<String, RemoteOutputReceiver<?>> outputReceivers = ImmutableMap.builder();
    for (Map.Entry<String, Coder> remoteOutputCoder :
        processBundleDescriptor.getRemoteOutputCoders().entrySet()) {
      String outputTransform = remoteOutputCoder.getKey();
      Coder coder = remoteOutputCoder.getValue();
      String bundleOutputPCollection =
          Iterables.getOnlyElement(
              processBundleDescriptor
                  .getProcessBundleDescriptor()
                  .getTransformsOrThrow(outputTransform)
                  .getInputsMap()
                  .values());
      FnDataReceiver outputReceiver = outputReceiverFactory.create(bundleOutputPCollection);
      outputReceivers.put(outputTransform, RemoteOutputReceiver.of(coder, outputReceiver));
    }
    return outputReceivers;
  }

  private static class PreparedClient {
    private BundleProcessor processor;
    private ExecutableProcessBundleDescriptor processBundleDescriptor;
    private WrappedSdkHarnessClient wrappedClient;
  }

  private PreparedClient prepare(
      WrappedSdkHarnessClient wrappedClient, ExecutableStage executableStage) {
    PreparedClient preparedClient = new PreparedClient();
    try {
      preparedClient.wrappedClient = wrappedClient;
      preparedClient.processBundleDescriptor =
          ProcessBundleDescriptors.fromExecutableStage(
              stageIdGenerator.getId(),
              executableStage,
              wrappedClient.getServerInfo().getDataServer().getApiServiceDescriptor(),
              wrappedClient.getServerInfo().getStateServer().getApiServiceDescriptor());
    } catch (IOException e) {
      throw new RuntimeException("Failed to create ProcessBundleDescriptor.", e);
    }

    preparedClient.processor =
        wrappedClient
            .getClient()
            .getProcessor(
                preparedClient.processBundleDescriptor.getProcessBundleDescriptor(),
                preparedClient.processBundleDescriptor.getRemoteInputDestinations(),
                wrappedClient.getServerInfo().getStateServer().getService());
    return preparedClient;
  }

  /**
   * A {@link StageBundleFactory} for remotely processing bundles that supports environment
   * expiration.
   */
  private class SimpleStageBundleFactory implements StageBundleFactory {

    private final ExecutableStage executableStage;
    private final int environmentIndex;
    private final Map<WrappedSdkHarnessClient, PreparedClient> preparedClients =
        new IdentityHashMap<>();
    private volatile PreparedClient currentClient;

    private SimpleStageBundleFactory(ExecutableStage executableStage) {
      this.executableStage = executableStage;
      this.environmentIndex = stageBundleFactoryCount.getAndIncrement() % environmentCaches.size();
      WrappedSdkHarnessClient client =
          environmentCaches.get(environmentIndex).getUnchecked(executableStage.getEnvironment());
      this.currentClient = prepare(client, executableStage);
      this.preparedClients.put(client, currentClient);
    }

    @Override
    public RemoteBundle getBundle(
        OutputReceiverFactory outputReceiverFactory,
        StateRequestHandler stateRequestHandler,
        BundleProgressHandler progressHandler)
        throws Exception {
      // TODO: Consider having BundleProcessor#newBundle take in an OutputReceiverFactory rather
      // than constructing the receiver map here. Every bundle factory will need this.

      final LoadingCache<Environment, WrappedSdkHarnessClient> currentCache;
      final WrappedSdkHarnessClient client;
      if (loadBalanceBundles) {
        availableCachesCount.acquire();
        currentCache = availableCaches.take();
        // Lock because the environment expiration can remove the ref for the client
        // which would close the underlying environment before we can ref it.
        Lock refLock = environmentCacheLocks.get(environmentIndex);
        try {
          refLock.lock();
          client = currentCache.getUnchecked(executableStage.getEnvironment());
          client.ref();
        } finally {
          refLock.unlock();
        }

        currentClient = preparedClients.get(client);
        if (currentClient == null) {
          // we are using this client for the first time
          preparedClients.put(client, currentClient = prepare(client, executableStage));
          // cleanup any expired clients
          preparedClients.keySet().removeIf(c -> c.bundleRefCount.get() == 0);
        }

      } else {
        currentCache = environmentCaches.get(environmentIndex);
        Lock refLock = environmentCacheLocks.get(environmentIndex);
        // Lock because the environment expiration can remove the ref for the client which would
        // close the underlying environment before we can ref it.
        try {
          refLock.lock();
          client = currentCache.getUnchecked(executableStage.getEnvironment());
          client.ref();
        } finally {
          refLock.unlock();
        }

        if (currentClient.wrappedClient != client) {
          // reset after environment expired
          preparedClients.clear();
          currentClient = prepare(client, executableStage);
          preparedClients.put(client, currentClient);
        }
      }

      if (environmentExpirationMillis > 0) {
        // Cleanup list of clients which were active during eviction but now do not hold references
        evictedActiveClients.removeIf(c -> c.bundleRefCount.get() == 0);
      }

      final RemoteBundle bundle =
          currentClient.processor.newBundle(
              getOutputReceivers(currentClient.processBundleDescriptor, outputReceiverFactory)
                  .build(),
              stateRequestHandler,
              progressHandler);
      return new RemoteBundle() {
        @Override
        public String getId() {
          return bundle.getId();
        }

        @Override
        public Map<String, FnDataReceiver> getInputReceivers() {
          return bundle.getInputReceivers();
        }

        @Override
        public void close() throws Exception {
          try {
            bundle.close();
          } finally {
            client.unref();
            if (loadBalanceBundles) {
              availableCaches.offer(currentCache);
              availableCachesCount.release();
            }
          }
        }
      };
    }

    @Override
    public ExecutableProcessBundleDescriptor getProcessBundleDescriptor() {
      return currentClient.processBundleDescriptor;
    }

    @Override
    public void close() throws Exception {
      // Clear reference to encourage cache eviction. Values are weakly referenced.
      preparedClients.clear();
    }
  }

  /**
   * Holder for an {@link SdkHarnessClient} along with its associated state and data servers. As of
   * now, there is a 1:1 relationship between data services and harness clients. The servers are
   * packaged here to tie server lifetimes to harness client lifetimes.
   */
  protected static class WrappedSdkHarnessClient {

    private final RemoteEnvironment environment;
    private final SdkHarnessClient client;
    private final ServerInfo serverInfo;
    private final AtomicInteger bundleRefCount = new AtomicInteger();

    private boolean closed;

    static WrappedSdkHarnessClient wrapping(RemoteEnvironment environment, ServerInfo serverInfo) {
      SdkHarnessClient client =
          SdkHarnessClient.usingFnApiClient(
              environment.getInstructionRequestHandler(), serverInfo.getDataServer().getService());
      return new WrappedSdkHarnessClient(environment, client, serverInfo);
    }

    private WrappedSdkHarnessClient(
        RemoteEnvironment environment, SdkHarnessClient client, ServerInfo serverInfo) {
      this.environment = environment;
      this.client = client;
      this.serverInfo = serverInfo;
      ref();
    }

    SdkHarnessClient getClient() {
      return client;
    }

    ServerInfo getServerInfo() {
      return serverInfo;
    }

    public synchronized void close() {
      if (closed) {
        return;
      }
      // DO NOT ADD ANYTHING HERE WHICH MIGHT CAUSE THE BLOCK BELOW TO NOT BE EXECUTED.
      // If we exit prematurely (e.g. due to an exception), resources won't be cleaned up properly.
      // Please make an AutoCloseable and add it to the try statement below.
      // These will be closed in the reverse creation order:
      try (AutoCloseable envCloser = environment;
          AutoCloseable provisioningServer = serverInfo.getProvisioningServer();
          AutoCloseable retrievalServer = serverInfo.getRetrievalServer();
          AutoCloseable stateServer = serverInfo.getStateServer();
          AutoCloseable dataServer = serverInfo.getDataServer();
          AutoCloseable controlServer = serverInfo.getControlServer();
          // Close the logging server first to prevent spaming the logs with error messages
          AutoCloseable loggingServer = serverInfo.getLoggingServer()) {
        // Wrap resources in try-with-resources to ensure all are cleaned up.
        // This will close _all_ of these even in the presence of exceptions.
        // The first exception encountered will be the base exception,
        // the next one will be added via Throwable#addSuppressed.
        closed = true;
      } catch (Exception e) {
        LOG.warn("Error cleaning up servers {}", environment.getEnvironment(), e);
      }
      // TODO: Wait for executor shutdown?
    }

    private int ref() {
      return bundleRefCount.incrementAndGet();
    }

    private int unref() {
      int refCount = bundleRefCount.decrementAndGet();
      Preconditions.checkState(refCount >= 0, "Reference count must not be negative.");
      if (refCount == 0) {
        // Close environment after it was removed from cache and all bundles finished.
        LOG.info("Closing environment {}", environment.getEnvironment());
        close();
      }
      return refCount;
    }
  }

  private ServerInfo createServerInfo(JobInfo jobInfo, ServerFactory serverFactory)
      throws IOException {
    Preconditions.checkNotNull(serverFactory, "serverFactory can not be null");

    PortablePipelineOptions portableOptions =
        PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions())
            .as(PortablePipelineOptions.class);
    ArtifactRetrievalService artifactRetrievalService;

    if (portableOptions.getRetrievalServiceType() == RetrievalServiceType.CLASSLOADER) {
      artifactRetrievalService = new ClassLoaderArtifactRetrievalService();
    } else {
      artifactRetrievalService = BeamFileSystemArtifactRetrievalService.create();
    }

    GrpcFnServer<FnApiControlClientPoolService> controlServer =
        GrpcFnServer.allocatePortAndCreateFor(
            FnApiControlClientPoolService.offeringClientsToPool(
                clientPool.getSink(), GrpcContextHeaderAccessorProvider.getHeaderAccessor()),
            serverFactory);
    GrpcFnServer<GrpcLoggingService> loggingServer =
        GrpcFnServer.allocatePortAndCreateFor(
            GrpcLoggingService.forWriter(Slf4jLogWriter.getDefault()), serverFactory);
    GrpcFnServer<ArtifactRetrievalService> retrievalServer =
        GrpcFnServer.allocatePortAndCreateFor(artifactRetrievalService, serverFactory);
    GrpcFnServer<StaticGrpcProvisionService> provisioningServer =
        GrpcFnServer.allocatePortAndCreateFor(
            StaticGrpcProvisionService.create(jobInfo.toProvisionInfo()), serverFactory);
    GrpcFnServer<GrpcDataService> dataServer =
        GrpcFnServer.allocatePortAndCreateFor(
            GrpcDataService.create(
                portableOptions, executor, OutboundObserverFactory.serverDirect()),
            serverFactory);
    GrpcFnServer<GrpcStateService> stateServer =
        GrpcFnServer.allocatePortAndCreateFor(GrpcStateService.create(), serverFactory);

    ServerInfo serverInfo =
        new AutoValue_DefaultJobBundleFactory_ServerInfo.Builder()
            .setControlServer(controlServer)
            .setLoggingServer(loggingServer)
            .setRetrievalServer(retrievalServer)
            .setProvisioningServer(provisioningServer)
            .setDataServer(dataServer)
            .setStateServer(stateServer)
            .build();
    return serverInfo;
  }

  /** A container for EnvironmentFactory and its corresponding Grpc servers. */
  @AutoValue
  public abstract static class ServerInfo {
    abstract GrpcFnServer<FnApiControlClientPoolService> getControlServer();

    abstract GrpcFnServer<GrpcLoggingService> getLoggingServer();

    abstract GrpcFnServer<ArtifactRetrievalService> getRetrievalServer();

    abstract GrpcFnServer<StaticGrpcProvisionService> getProvisioningServer();

    abstract GrpcFnServer<GrpcDataService> getDataServer();

    abstract GrpcFnServer<GrpcStateService> getStateServer();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setControlServer(GrpcFnServer<FnApiControlClientPoolService> server);

      abstract Builder setLoggingServer(GrpcFnServer<GrpcLoggingService> server);

      abstract Builder setRetrievalServer(GrpcFnServer<ArtifactRetrievalService> server);

      abstract Builder setProvisioningServer(GrpcFnServer<StaticGrpcProvisionService> server);

      abstract Builder setDataServer(GrpcFnServer<GrpcDataService> server);

      abstract Builder setStateServer(GrpcFnServer<GrpcStateService> server);

      abstract ServerInfo build();
    }
  }
}
