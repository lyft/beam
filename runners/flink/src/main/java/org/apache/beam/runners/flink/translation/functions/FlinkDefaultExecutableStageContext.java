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
package org.apache.beam.runners.flink.translation.functions;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardEnvironments;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.control.DefaultJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.environment.EmbeddedEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.LyftPythonEnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.ProcessEnvironmentFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.options.PortablePipelineOptions;

/** Implementation of a {@link FlinkExecutableStageContext}. */
class FlinkDefaultExecutableStageContext implements FlinkExecutableStageContext, AutoCloseable {
  private final JobBundleFactory jobBundleFactory;

  private static FlinkDefaultExecutableStageContext create(JobInfo jobInfo) {
    JobBundleFactory jobBundleFactory =
        DefaultJobBundleFactory.create(
            jobInfo,
            ImmutableMap.of(
                BeamUrns.getUrn(StandardEnvironments.Environments.DOCKER),
                //new DockerEnvironmentFactory.Provider(
                //    PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions())),
                // START LYFT CUSTOM
                new LyftPythonEnvironmentFactory.Provider(jobInfo),
                // END LYFT CUSTOM
                BeamUrns.getUrn(StandardEnvironments.Environments.PROCESS),
                new ProcessEnvironmentFactory.Provider(),
                Environments.ENVIRONMENT_EMBEDDED, // Non Public urn for testing.
                new EmbeddedEnvironmentFactory.Provider()));
    return new FlinkDefaultExecutableStageContext(jobBundleFactory);
  }

  private FlinkDefaultExecutableStageContext(JobBundleFactory jobBundleFactory) {
    this.jobBundleFactory = jobBundleFactory;
  }

  @Override
  public StageBundleFactory getStageBundleFactory(ExecutableStage executableStage) {
    return jobBundleFactory.forStage(executableStage);
  }

  @Override
  public void close() throws Exception {
    jobBundleFactory.close();
  }

  private static class JobFactoryState {
    private final AtomicInteger counter = new AtomicInteger(0);
    private final List<ReferenceCountingFlinkExecutableStageContextFactory> factories =
        new ArrayList<>();
    private final int maxFactories;

    private JobFactoryState(int maxFactories) {
      this.maxFactories = maxFactories;
    }

    private synchronized FlinkExecutableStageContext.Factory getFactory() {
      int count = counter.getAndIncrement();

      if (count < maxFactories) {
        factories.add(ReferenceCountingFlinkExecutableStageContextFactory
            .create(FlinkDefaultExecutableStageContext::create));
      }

      return factories.get(count % maxFactories);
    }
  }

  enum MultiInstanceFactory implements Factory {
    MULTI_INSTANCE;

    private static final ConcurrentMap<String, JobFactoryState> jobFactories =
        new ConcurrentHashMap<>();

    @Override
    public FlinkExecutableStageContext get(JobInfo jobInfo) {
      JobFactoryState state = jobFactories
          .computeIfAbsent(jobInfo.jobId(), k -> {
            PortablePipelineOptions portableOptions = PipelineOptionsTranslation
                .fromProto(jobInfo.pipelineOptions())
                .as(PortablePipelineOptions.class);

            return new JobFactoryState(
                MoreObjects.firstNonNull(portableOptions.getSdkWorkerParallelism(), 1L).intValue());
          });

      return state.getFactory().get(jobInfo);
    }
  }
}
