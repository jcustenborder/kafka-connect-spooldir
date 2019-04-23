package com.github.jcustenborder.kafka.connect.spooldir;

import com.google.common.io.PatternFilenameFilter;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.junit.Assert.*;

public class SpoolDirJsonSourceIT extends BaseConnectorIT {
	private static final Logger log = LoggerFactory.getLogger(SpoolDirJsonSourceIT.class);
	private static final String CONNECTOR_CLASS = "SpoolDirJsonSourceConnector";
	private static final String FILE_PATTERN = "^users\\d+\\.json";

	@BeforeEach
	public void setup() throws IOException {
		startConnect();
	}

	@AfterEach
	public void close() throws IOException {
		stopConnect();
	}

	@Test // that the connect loads multiple json files and produces to a topic
	public void jsonSourceWithoutSchema() throws Throwable {
		// setup connect source (csv files)
		String[] inputFiles = new String[]{
				"json/FieldsMatch.data",
				"json/FieldsMatch.data"
		};
		loadFiles("users", "json", inputFiles);

		// setup up props for the sink connector
		this.settings.put(CONNECTOR_CLASS_CONFIG, CONNECTOR_CLASS);
		this.settings.put(SpoolDirSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, FILE_PATTERN);
		this.settings.put(SpoolDirSourceConnectorConfig.SCHEMA_GENERATION_ENABLED_CONF, "true");

		// start the connector
		connect.configureConnector(CONNECTOR_NAME, this.settings);

		// wait for tasks to spin up
		waitForConnectorToStart(CONNECTOR_NAME, 1);

		// Verify the records were written to Kafka
		connect.kafka().consume(40, CONSUME_MAX_DURATION_MS, KAFKA_TOPIC);

		// assert that the correct number of files were moved to "finished"
		assertEquals(inputPath.listFiles(new PatternFilenameFilter(FILE_PATTERN)).length, 0);
		assertEquals(errorPath.listFiles(new PatternFilenameFilter(FILE_PATTERN)).length, 0);
		assertEquals(finishedPath.listFiles(new PatternFilenameFilter(FILE_PATTERN)).length, inputFiles.length);

		// todo: any assertions on the ConsumerRecords other than count?
	}

	@Test // that the connector fails to startup when multiple schemas are found in files
	public void jsonSourceWithMismatchData() throws Throwable {
		// setup connect source (csv files)
		String[] inputFiles = new String[]{
				"json/FieldsMatch.data",
				"json/DataHasMoreFields.data"
		};
		loadFiles("users", "json", inputFiles);

		// setup up props for the sink connector
		this.settings.put(CONNECTOR_CLASS_CONFIG, CONNECTOR_CLASS);
		this.settings.put(SpoolDirSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, FILE_PATTERN);
		this.settings.put(SpoolDirSourceConnectorConfig.SCHEMA_GENERATION_ENABLED_CONF, "true");

		// start the connector
		connect.configureConnector(CONNECTOR_NAME, this.settings);

		// assert on connector state
		ConnectorStateInfo info = waitForConnectorInfo(CONNECTOR_NAME).orElse(null);

		// connector state should be FAILED with 0 tasks
		assertNotNull(info);
		assertEquals(info.tasks().size(), 0);
		assertEquals(info.connector().state(), AbstractStatus.State.FAILED.toString());
		assertTrue(info.connector().trace().contains("DataException"));

		// files should be untouched
		assertEquals(inputPath.listFiles(new PatternFilenameFilter(FILE_PATTERN)).length, inputFiles.length);
		assertEquals(errorPath.listFiles(new PatternFilenameFilter(FILE_PATTERN)).length, 0);
		assertEquals(finishedPath.listFiles(new PatternFilenameFilter(FILE_PATTERN)).length, 0);
	}
}
