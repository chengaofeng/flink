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

package org.apache.flink.client;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ContextEnvironment;
import org.apache.flink.client.program.ContextEnvironmentFactory;
import org.apache.flink.client.program.DetachedJobExecutionResult;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.program.ProgramMissingJobException;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.core.execution.ExecutorServiceLoader;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarFile;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility functions for Flink client.
 */
public enum ClientUtils {
	;

	private static final Logger LOG = LoggerFactory.getLogger(ClientUtils.class);

	public static void checkJarFile(URL jar) throws IOException {
		File jarFile;
		try {
			jarFile = new File(jar.toURI());
		} catch (URISyntaxException e) {
			throw new IOException("JAR file path is invalid '" + jar + '\'');
		}
		if (!jarFile.exists()) {
			throw new IOException("JAR file does not exist '" + jarFile.getAbsolutePath() + '\'');
		}
		if (!jarFile.canRead()) {
			throw new IOException("JAR file can't be read '" + jarFile.getAbsolutePath() + '\'');
		}

		try (JarFile ignored = new JarFile(jarFile)) {
			// verify that we can open the Jar file
		} catch (IOException e) {
			throw new IOException("Error while opening jar file '" + jarFile.getAbsolutePath() + '\'', e);
		}
	}

	public static ClassLoader buildUserCodeClassLoader(List<URL> jars, List<URL> classpaths, ClassLoader parent) {
		URL[] urls = new URL[jars.size() + classpaths.size()];
		for (int i = 0; i < jars.size(); i++) {
			urls[i] = jars.get(i);
		}
		for (int i = 0; i < classpaths.size(); i++) {
			urls[i + jars.size()] = classpaths.get(i);
		}
		return FlinkUserCodeClassLoaders.parentFirst(urls, parent);
	}

	public static JobExecutionResult submitJob(
			ClusterClient<?> client,
			JobGraph jobGraph) throws ProgramInvocationException {
		checkNotNull(client);
		checkNotNull(jobGraph);
		try {
			return client
				.submitJob(jobGraph)
				.thenApply(JobSubmissionResult::getJobID)
				.thenApply(DetachedJobExecutionResult::new)
				.get();
		} catch (InterruptedException | ExecutionException e) {
			ExceptionUtils.checkInterrupted(e);
			throw new ProgramInvocationException("Could not run job in detached mode.", jobGraph.getJobID(), e);
		}
	}

	public static JobExecutionResult submitJobAndWaitForResult(
			ClusterClient<?> client,
			JobGraph jobGraph,
			ClassLoader classLoader) throws ProgramInvocationException {
		checkNotNull(client);
		checkNotNull(jobGraph);
		checkNotNull(classLoader);

		JobResult jobResult;

		try {
			jobResult = client
				.submitJob(jobGraph)
				.thenApply(JobSubmissionResult::getJobID)
				.thenCompose(client::requestJobResult)
				.get();
		} catch (InterruptedException | ExecutionException e) {
			ExceptionUtils.checkInterrupted(e);
			throw new ProgramInvocationException("Could not run job", jobGraph.getJobID(), e);
		}

		try {
			return jobResult.toJobExecutionResult(classLoader);
		} catch (JobExecutionException | IOException | ClassNotFoundException e) {
			throw new ProgramInvocationException("Job failed", jobGraph.getJobID(), e);
		}
	}

	public static void runProgram(
			final ClusterClientServiceLoader clusterClientServiceLoader,
			final Configuration configuration,
			final PackagedProgram program) throws ProgramInvocationException, FlinkException {

		checkNotNull(clusterClientServiceLoader);
		checkNotNull(configuration);
		checkNotNull(program);

		// TODO: 05.11.19 now the  ClusterClientServiceLoader is not used.
		final ExecutorServiceLoader executorServiceLoader = new DefaultExecutorServiceLoader();

		logAndSysout("Starting execution of program");

		JobSubmissionResult result = executeProgram(executorServiceLoader, configuration, program);

		if (result.isJobExecutionResult()) {
			logAndSysout("Program execution finished");
			JobExecutionResult execResult = result.getJobExecutionResult();
			System.out.println("Job with JobID " + execResult.getJobID() + " has finished.");
			System.out.println("Job Runtime: " + execResult.getNetRuntime() + " ms");
			Map<String, Object> accumulatorsResult = execResult.getAllAccumulatorResults();
			if (accumulatorsResult.size() > 0) {
				System.out.println("Accumulator Results: ");
				System.out.println(AccumulatorHelper.getResultsFormatted(accumulatorsResult));
			}
		} else {
			logAndSysout("Job has been submitted with JobID " + result.getJobID());
		}
	}

	public static JobSubmissionResult executeProgram(
			final ExecutorServiceLoader executorServiceLoader,
			final Configuration configuration,
			final PackagedProgram program) throws ProgramMissingJobException, ProgramInvocationException {

		final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(program.getUserCodeClassLoader());
			LOG.info("Starting program (attached: {})", configuration.getBoolean(ExecutionOptions.ATTACHED));

			final AtomicReference<JobExecutionResult> jobExecutionResult = new AtomicReference<>();

			// TODO: 11.11.19 this should move to the appropriate place
//			ConfigUtils.encodeStreamToConfig(configuration, PipelineOptions.JARS, program.getAllLibraries().stream(), URL::toString);

			final ContextEnvironmentFactory factory = new ContextEnvironmentFactory(
					executorServiceLoader,
					configuration,
					jobExecutionResult);
			ContextEnvironment.setAsContext(factory);

			try {
				program.invokeInteractiveModeForExecution();

				JobExecutionResult result = jobExecutionResult.get();
				if (result == null) {
					throw new ProgramMissingJobException("The program didn't contain a Flink job.");
				}
				return result;
			} finally {
				ContextEnvironment.unsetContext();
			}
		} finally {
			Thread.currentThread().setContextClassLoader(contextClassLoader);
		}
	}

	private static void logAndSysout(String message) {
		LOG.info(message);
		System.out.println(message);
	}
}
