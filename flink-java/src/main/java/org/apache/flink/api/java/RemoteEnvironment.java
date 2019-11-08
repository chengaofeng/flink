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

package org.apache.flink.api.java;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An {@link ExecutionEnvironment} that sends programs to a cluster for execution. The environment
 * needs to be created with the address and port of the JobManager of the Flink cluster that
 * should execute the programs.
 *
 * <p>Many programs executed via the remote environment depend on additional classes. Such classes
 * may be the classes of functions (transformation, aggregation, ...) or libraries. Those classes
 * must be attached to the remote environment as JAR files, to allow the environment to ship the
 * classes into the cluster for the distributed execution.
 */
@Public
public class RemoteEnvironment extends ExecutionEnvironment {

	/**
	 * Creates a new RemoteEnvironment that points to the master (JobManager) described by the
	 * given host name and port.
	 *
	 * <p>Each program execution will have all the given JAR files in its classpath.
	 *
	 * @param host The host name or address of the master (JobManager), where the program should be executed.
	 * @param port The port of the master (JobManager), where the program should be executed.
	 * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
	 *                 user-defined functions, user-defined input formats, or any libraries, those must be
	 *                 provided in the JAR files.
	 */
	public RemoteEnvironment(String host, int port, String... jarFiles) {
		this(host, port, null, jarFiles, null);
	}

	/**
	 * Creates a new RemoteEnvironment that points to the master (JobManager) described by the
	 * given host name and port.
	 *
	 * <p>Each program execution will have all the given JAR files in its classpath.
	 *
	 * @param host The host name or address of the master (JobManager), where the program should be executed.
	 * @param port The port of the master (JobManager), where the program should be executed.
	 * @param clientConfig The configuration used by the client that connects to the cluster.
	 * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
	 *                 user-defined functions, user-defined input formats, or any libraries, those must be
	 *                 provided in the JAR files.
	 */
	public RemoteEnvironment(String host, int port, Configuration clientConfig, String[] jarFiles) {
		this(host, port, clientConfig, jarFiles, null);
	}

	/**
	 * Creates a new RemoteEnvironment that points to the master (JobManager) described by the
	 * given host name and port.
	 *
	 * <p>Each program execution will have all the given JAR files in its classpath.
	 *
	 * @param host The host name or address of the master (JobManager), where the program should be executed.
	 * @param port The port of the master (JobManager), where the program should be executed.
	 * @param clientConfig The configuration used by the client that connects to the cluster.
	 * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
	 *                 user-defined functions, user-defined input formats, or any libraries, those must be
	 *                 provided in the JAR files.
	 * @param globalClasspaths The paths of directories and JAR files that are added to each user code
	 *                 classloader on all nodes in the cluster. Note that the paths must specify a
	 *                 protocol (e.g. file://) and be accessible on all nodes (e.g. by means of a NFS share).
	 *                 The protocol must be supported by the {@link java.net.URLClassLoader}.
	 */
	public RemoteEnvironment(String host, int port, Configuration clientConfig, String[] jarFiles, URL[] globalClasspaths) {
		super(validateAndParseConfiguratin(clientConfig, host, port, jarFiles, globalClasspaths));
	}

	private static Configuration validateAndParseConfiguratin(
			final Configuration clientConfiguration,
			final String host,
			final int port,
			final String[] jarFiles,
			final URL[] globalClasspaths) {
		if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
			throw new InvalidProgramException(
					"The RemoteEnvironment cannot be instantiated when running in a pre-defined context " +
							"(such as Command Line Client, Scala Shell, or TestEnvironment)");
		}

		final Configuration effectiveConfiguration = clientConfiguration == null ? new Configuration() : new Configuration(clientConfiguration);
		final InetSocketAddress jmAddress = getJobManagerAddress(host, port);
		final List<URL> jarFileUrls = parseJarFileUrls(jarFiles);
		return createEffectiveConfiguration(effectiveConfiguration, jmAddress, jarFileUrls, globalClasspaths);
	}

	private static Configuration createEffectiveConfiguration(
			final Configuration effectiveConfiguration,
			final InetSocketAddress jmAddress,
			final List<URL> jarFileUrls,
			final URL[] globalClasspaths) {

		checkNotNull(effectiveConfiguration);
		checkNotNull(jarFileUrls);

		effectiveConfiguration.set(ExecutionOptions.CLUSTER_MODE, ExecutionMode.SESSION);
		effectiveConfiguration.set(ExecutionOptions.ATTACHED, true);
		effectiveConfiguration.set(ExecutionOptions.TARGET, "default");

		setJobManagerAddressInConfig(effectiveConfiguration, jmAddress);

		ConfigUtils.encodeStreamToConfig(effectiveConfiguration, PipelineOptions.JARS, jarFileUrls.stream(), URL::toString);
		ConfigUtils.encodeArrayToConfig(effectiveConfiguration, PipelineOptions.CLASSPATHS, globalClasspaths, URL::toString);

		return effectiveConfiguration;
	}

	private static InetSocketAddress getJobManagerAddress(final String host, final int port) {
		checkNotNull(host);
		checkArgument(port >= 1 && port < 0xffff);

		return new InetSocketAddress(host, port);
	}

	// TODO: 25.10.19 these lines are the same as the CliFrontend.setJobManagerAddressInConfig()
	private static void setJobManagerAddressInConfig(final Configuration config, final InetSocketAddress jmAddress) {
		checkNotNull(jmAddress);

		config.setString(JobManagerOptions.ADDRESS, jmAddress.getHostString());
		config.setInteger(JobManagerOptions.PORT, jmAddress.getPort());
		config.setString(RestOptions.ADDRESS, jmAddress.getHostString());
		config.setInteger(RestOptions.PORT, jmAddress.getPort());
	}

	protected static List<URL> parseJarFileUrls(final String[] jarFiles) {
		if (jarFiles == null) {
			return Collections.emptyList();
		}

		final List<URL> jarFileList = new ArrayList<>(jarFiles.length);
		for (String jarFile : jarFiles) {
			try {
				jarFileList.add(new File(jarFile).getAbsoluteFile().toURI().toURL());
			} catch (MalformedURLException e) {
				throw new IllegalArgumentException("JAR file path invalid", e);
			}
		}
		return jarFileList;
	}

	@Override
	public String toString() {
		final Configuration executionConfiguration = getExecutorConfiguration();
		return "Remote Environment (" +
				executionConfiguration.get(JobManagerOptions.ADDRESS) + ":" + executionConfiguration.get(JobManagerOptions.PORT) +
				" - parallelism = " + (getParallelism() == -1 ? "default" : getParallelism()) + ").";
	}
}
