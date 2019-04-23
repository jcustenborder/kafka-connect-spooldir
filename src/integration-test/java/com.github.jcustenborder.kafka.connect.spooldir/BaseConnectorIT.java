/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package com.github.jcustenborder.kafka.connect.spooldir;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;

public abstract class BaseConnectorIT {
	private static final Logger log = LoggerFactory.getLogger(BaseConnectorIT.class);

	protected EmbeddedConnectCluster connect;

	// base props
	protected static final String CONNECTOR_NAME = "csv-source-connector";
	protected static final String KAFKA_TOPIC = "spooldir-test-topic";
	protected static final int TASKS_MAX = 3;
	protected static final long CONSUME_MAX_DURATION_MS = TimeUnit.SECONDS.toMillis(5);
	protected static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(15);

	// SpoolDir specific props
	protected File tempRoot;
	protected File inputPath;
	protected File finishedPath;
	protected File errorPath;
	protected Map<String, String> settings;

	protected void startConnect() throws IOException {
		connect = new EmbeddedConnectCluster.Builder()
				.name("spooldir-connect-cluster")
				.build();

		// start the clusters
		connect.start();

		// setup a test topic
		connect.kafka().createTopic(KAFKA_TOPIC);
		// todo: admin client fails to auto create this
		connect.kafka().createTopic("connect-storage-topic-spooldir-connect-cluster");

		// setup temp dir
		this.tempRoot = Files.createTempDir();
		this.inputPath = new File(this.tempRoot, "input");
		this.inputPath.mkdirs();
		this.finishedPath = new File(this.tempRoot, "finished");
		this.finishedPath.mkdirs();
		this.errorPath = new File(this.tempRoot, "error");
		this.errorPath.mkdirs();

		// setup base connector props
		this.settings = new LinkedHashMap<>();
		this.settings.put(AbstractSourceConnectorConfig.INPUT_PATH_CONFIG, this.inputPath.getAbsolutePath());
		this.settings.put(AbstractSourceConnectorConfig.FINISHED_PATH_CONFIG, this.finishedPath.getAbsolutePath());
		this.settings.put(AbstractSourceConnectorConfig.ERROR_PATH_CONFIG, this.errorPath.getAbsolutePath());
		this.settings.put(AbstractSourceConnectorConfig.TOPIC_CONF, KAFKA_TOPIC);
		this.settings.put(TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));
	}

	protected void stopConnect() throws IOException {
		// stop all Connect, Kafka and Zk threads.
		connect.stop();

		// cleanup temp dir
		java.nio.file.Files.walkFileTree(this.tempRoot.toPath(), new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				log.trace("cleanupTempDir() - Removing {}", file);
				java.nio.file.Files.delete(file);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
				log.trace("cleanupTempDir() - Removing {}", file);
				java.nio.file.Files.delete(file);
				return FileVisitResult.CONTINUE;
			}
		});
	}

	/**
	 * Given a single file, this will load it into the temp input directory.
	 * @param filePrefix a prefix value to give to all files
	 * @param fileType the file type (csv, json, etc)
	 * @param inputFile file name (loaded from classpath resources)
	 * @throws IOException
	 */
	protected void loadFile(String filePrefix, String fileType, String inputFile) throws IOException {
		loadFiles(filePrefix, fileType, new String[]{ inputFile });
	}

	/**
	 * Given a set of data files, this will load them into the temp input directory.
	 * @param filePrefix a prefix value to give to all files
	 * @param fileType the file type (csv, json, etc)
	 * @param inputFiles array of file names (loaded from classpath resources)
	 * @throws IOException
	 */
	protected void loadFiles(String filePrefix, String fileType, String[] inputFiles) throws IOException {
		int index = 0;
		for (String inputFile : inputFiles) {
			try (InputStream inputStream = this.getClass().getResourceAsStream(inputFile)) {
				File outputFile = new File(this.inputPath, filePrefix + index + "." + fileType);
				try (OutputStream outputStream = new FileOutputStream(outputFile)) {
					ByteStreams.copy(inputStream, outputStream);
				}
			}
			index++;
		}
	}

	/**
	 * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the given
	 * name to start the specified number of tasks.
	 *
	 * @param name     the name of the connector
	 * @param numTasks the minimum number of tasks that are expected
	 * @return the time this method discovered the connector has started, in milliseconds past epoch
	 * @throws InterruptedException if this was interrupted
	 */
	protected long waitForConnectorToStart(String name, int numTasks) throws InterruptedException {
		TestUtils.waitForCondition(
				() -> assertConnectorAndTasksRunning(name, numTasks).orElse(false),
				CONNECTOR_STARTUP_DURATION_MS,
				"Connector tasks did not start in time."
		);
		return System.currentTimeMillis();
	}

	/**
	 * Wait up to {@link #CONNECTOR_STARTUP_DURATION_MS maximum time limit} for the connector with the given
	 * name to be available. This method does not wait for a RUNNING status. It will return as soon as a
	 * ConnectorStateInfo object is available.
	 *
	 * @param name     the name of the connector
	 * @return the time this method discovered the connector has started, in milliseconds past epoch
	 * @throws InterruptedException if this was interrupted
	 */
	protected Optional<ConnectorStateInfo> waitForConnectorInfo(String name) throws InterruptedException {
		TestUtils.waitForCondition(
				() -> assertConnectInfoAvailable(name).orElse(false),
				CONNECTOR_STARTUP_DURATION_MS,
				"ConnectorStateInfo was not available in time."
		);

		return Optional.of(connect.connectorStatus(name));
	}

	/**
	 * Confirm that a connector with an exact number of tasks is running.
	 *
	 * @param connectorName the connector
	 * @param numTasks      the expected number of tasks
	 * @return true if the connector and tasks are in RUNNING state; false otherwise
	 */
	protected Optional<Boolean> assertConnectorAndTasksRunning(String connectorName, int numTasks) {
		try {
			ConnectorStateInfo info = connect.connectorStatus(connectorName);
			boolean result = info != null
					&& info.tasks().size() == numTasks
					&& info.connector().state().equals(AbstractStatus.State.RUNNING.toString())
					&& info.tasks().stream().allMatch(s -> s.state().equals(AbstractStatus.State.RUNNING.toString()));
			return Optional.of(result);
		} catch (Exception e) {
			log.error("Could not check connector state info.", e);
			return Optional.empty();
		}
	}

	/**
	 * Confirm that the connector info is available (a more lenient version of assertConnectorAndTasksRunning).
	 *
	 * This method is helpful in asserting on connector may have failed to start up.
	 * @param connectorName the connector
	 * @return true if the connector info is not null
	 */
	protected Optional<Boolean> assertConnectInfoAvailable(String connectorName) {
		try {
			return Optional.of(connect.connectorStatus(connectorName) != null);
		} catch (Exception e) {
			log.error("Could not check connector state info.", e);
			return Optional.empty();
		}
	}
}
