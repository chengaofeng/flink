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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.DetachedJobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.execution.Executor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link Executor} to be used when executing a job in isolation.
 * This executor will start a cluster specifically for the job at hand and
 * tear it down when the job is finished either successfully or due to an error.
 */
public class PerJobClusterExecutor<ClusterID> implements Executor {

	private static final Logger LOG = LoggerFactory.getLogger(PerJobClusterExecutor.class);

	private final ClusterClientServiceLoader clusterClientServiceLoader;

	public PerJobClusterExecutor() {
		this(new DefaultClusterClientServiceLoader());
	}

	public PerJobClusterExecutor(final ClusterClientServiceLoader clusterClientServiceLoader) {
		this.clusterClientServiceLoader = checkNotNull(clusterClientServiceLoader);
	}

	@Override
	public JobExecutionResult execute(Pipeline pipeline, Configuration executionConfig) throws Exception {
		final ClusterClientFactory<ClusterID> clusterClientFactory = clusterClientServiceLoader.getClusterClientFactory(executionConfig);
		final boolean attached = executionConfig.get(ExecutionOptions.ATTACHED);

		try (final ClusterDescriptor<ClusterID> clusterDescriptor = clusterClientFactory.createClusterDescriptor(executionConfig)) {
			return attached
					? executeAttached(clusterClientFactory, clusterDescriptor, pipeline, executionConfig)
					: executeDetached(clusterClientFactory, clusterDescriptor, pipeline, executionConfig);
		}
	}

	private JobExecutionResult executeDetached(
			final ClusterClientFactory<ClusterID> clusterClientFactory,
			final ClusterDescriptor<ClusterID> clusterDescriptor,
			final Pipeline pipeline,
			final Configuration executionConfig) throws Exception {

		final ExecutionConfigAccessor configAccessor = ExecutionConfigAccessor.fromConfiguration(executionConfig);
		final List<URL> classpaths = configAccessor.getClasspaths();
		final List<URL> jarFileUrls = configAccessor.getJarFilePaths();

		final JobGraph jobGraph = getJobGraph(pipeline, executionConfig, classpaths, jarFileUrls);

		final ClusterSpecification clusterSpecification = clusterClientFactory.getClusterSpecification(executionConfig);

		try (final ClusterClient<ClusterID> ignored = clusterDescriptor.deployJobCluster(clusterSpecification, jobGraph, true)) {
			LOG.info("Job has been submitted with JobID " + jobGraph.getJobID());
		}
		return new DetachedJobExecutionResult(jobGraph.getJobID());
	}

	private JobExecutionResult executeAttached(
			final ClusterClientFactory<ClusterID> clusterClientFactory,
			final ClusterDescriptor<ClusterID> clusterDescriptor,
			final Pipeline pipeline,
			final Configuration executionConfig) throws Exception {

		ClusterClient<ClusterID> clusterClient = null;
		Thread shutdownHook = null;

		try {
			final ExecutionConfigAccessor configAccessor = ExecutionConfigAccessor.fromConfiguration(executionConfig);
			final List<URL> classpaths = configAccessor.getClasspaths();
			final List<URL> jarFileUrls = configAccessor.getJarFilePaths();

			final JobGraph jobGraph = getJobGraph(pipeline, executionConfig, classpaths, jarFileUrls);

			final ClassLoader userClassLoader = ClientUtils.buildUserCodeClassLoader(jarFileUrls, classpaths, getClass().getClassLoader());

			final ClusterSpecification clusterSpecification = clusterClientFactory.getClusterSpecification(executionConfig);

			clusterClient = clusterDescriptor.deploySessionCluster(clusterSpecification);
			shutdownHook = configAccessor.isShutdownOnAttachedExit()
					? ShutdownHookUtil.addShutdownHook(clusterClient::shutDownCluster, clusterClient.getClass().getSimpleName(), LOG)
					: null;

			checkState(!configAccessor.getDetachedMode());
			return ClientUtils.submitJobAndWaitForResult(clusterClient, jobGraph, userClassLoader).getJobExecutionResult();
		} finally {
			if (clusterClient != null) {
				clusterClient.shutDownCluster();

				if (shutdownHook != null) {
					ShutdownHookUtil.removeShutdownHook(shutdownHook, clusterClient.getClass().getSimpleName(), LOG);
				}
				clusterClient.close();
			}
		}
	}

	private JobGraph getJobGraph(
			final Pipeline pipeline,
			final Configuration configuration,
			final List<URL> classpaths,
			final List<URL> libraries) {

		checkNotNull(pipeline);
		checkNotNull(configuration);
		checkNotNull(classpaths);
		checkNotNull(libraries);

		final ExecutionConfigAccessor executionConfigAccessor = ExecutionConfigAccessor.fromConfiguration(configuration);
		final JobGraph jobGraph = FlinkPipelineTranslationUtil
				.getJobGraph(pipeline, configuration, executionConfigAccessor.getParallelism());

		jobGraph.addJars(libraries);
		jobGraph.setClasspaths(classpaths);
		jobGraph.setSavepointRestoreSettings(executionConfigAccessor.getSavepointRestoreSettings());

		return jobGraph;
	}
}
