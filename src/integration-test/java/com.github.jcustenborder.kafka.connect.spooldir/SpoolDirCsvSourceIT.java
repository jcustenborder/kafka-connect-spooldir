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

public class SpoolDirCsvSourceIT extends BaseConnectorIT {
	private static final Logger log = LoggerFactory.getLogger(SpoolDirCsvSourceIT.class);
	private static final String CONNECTOR_CLASS = "SpoolDirCsvSourceConnector";
	private static final String FILE_PATTERN = "^users\\d+\\.csv";
	private static final String KEY_SCHEMA = "{\n  \"name\" : \"com.example.users.UserKey\",\n  \"type\" : \"STRUCT\",\n  \"isOptional\" : false,\n  \"fieldSchemas\" : {\n    \"id\" : {\n      \"type\" : \"INT64\",\n      \"isOptional\" : false\n    }\n  }\n}\n";
	private static final String VALUE_SCHEMA = "{\n  \"name\" : \"com.example.users.User\",\n  \"type\" : \"STRUCT\",\n  \"isOptional\" : false,\n  \"fieldSchemas\" : {\n    \"id\" : {\n      \"type\" : \"INT64\",\n      \"isOptional\" : false\n    },\n    \"first_name\" : {\n      \"type\" : \"STRING\",\n      \"isOptional\" : true\n    },\n    \"last_name\" : {\n      \"type\" : \"STRING\",\n      \"isOptional\" : true\n    },\n    \"email\" : {\n      \"type\" : \"STRING\",\n      \"isOptional\" : true\n    },\n    \"gender\" : {\n      \"type\" : \"STRING\",\n      \"isOptional\" : true\n    },\n    \"ip_address\" : {\n      \"type\" : \"STRING\",\n      \"isOptional\" : true\n    },\n    \"last_login\" : {\n      \"name\" : \"org.apache.kafka.connect.data.Timestamp\",\n      \"type\" : \"INT64\",\n      \"version\" : 1,\n      \"isOptional\" : true\n    },\n    \"account_balance\" : {\n      \"name\" : \"org.apache.kafka.connect.data.Decimal\",\n      \"type\" : \"BYTES\",\n      \"version\" : 1,\n      \"parameters\" : {\n        \"scale\" : \"2\"\n      },\n      \"isOptional\" : true\n    },\n    \"country\" : {\n      \"type\" : \"STRING\",\n      \"isOptional\" : true\n    },\n    \"favorite_color\" : {\n      \"type\" : \"STRING\",\n      \"isOptional\" : true\n    }\n  }\n}\n";

	@BeforeEach
	public void setup() throws IOException {
		startConnect();
	}

	@AfterEach
	public void close() throws IOException {
		stopConnect();
	}

	@Test // that the connect loads multiple csv files and produces to a topic
	public void csvSourceWithHeaderAndWithoutSchema() throws Throwable {
		// setup connect source (csv files)
		String[] inputFiles = new String[]{
				"csv/FieldsMatch.data",
				"csv/FieldsMatch.data"
		};
		loadFiles("users", "csv", inputFiles);

		// setup up props for the sink connector
		this.settings.put(CONNECTOR_CLASS_CONFIG, CONNECTOR_CLASS);
		this.settings.put(SpoolDirSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, FILE_PATTERN);
		this.settings.put(SpoolDirSourceConnectorConfig.SCHEMA_GENERATION_ENABLED_CONF, "true");
		this.settings.put(SpoolDirCsvSourceConnectorConfig.CSV_FIRST_ROW_AS_HEADER_CONF, "true");

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

	@Test // that the connect loads multiple csv files and produces to a topic
	public void csvSourceWithoutHeaderAndWithoutSchema() throws Throwable {
		// setup connect source (csv files)
		String[] inputFiles = new String[]{
				"csv/WithoutHeader.data",
				"csv/WithoutHeader.data",
				"csv/WithoutHeader.data"
		};
		loadFiles("users", "csv", inputFiles);

		// setup up props for the sink connector
		this.settings.put(CONNECTOR_CLASS_CONFIG, CONNECTOR_CLASS);
		this.settings.put(SpoolDirSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, FILE_PATTERN);
		this.settings.put(SpoolDirSourceConnectorConfig.SCHEMA_GENERATION_ENABLED_CONF, "true");
		this.settings.put(SpoolDirCsvSourceConnectorConfig.CSV_FIRST_ROW_AS_HEADER_CONF, "false");

		// start the connector
		connect.configureConnector(CONNECTOR_NAME, this.settings);

		// wait for tasks to spin up
		waitForConnectorToStart(CONNECTOR_NAME, 1);

		// Verify the records were written to Kafka
		connect.kafka().consume(60, CONSUME_MAX_DURATION_MS, KAFKA_TOPIC);

		// assert that the correct number of files were moved to "finished"
		assertEquals(inputPath.listFiles(new PatternFilenameFilter(FILE_PATTERN)).length, 0);
		assertEquals(errorPath.listFiles(new PatternFilenameFilter(FILE_PATTERN)).length, 0);
		assertEquals(finishedPath.listFiles(new PatternFilenameFilter(FILE_PATTERN)).length, inputFiles.length);

		// todo: any assertions on the ConsumerRecords other than count?
	}

	@Test // that the connector fails to startup when multiple schemas are found in files
	public void csvSourceWithMismatchData() throws Throwable {
		// setup connect source (csv files)
		String[] inputFiles = new String[]{
				"csv/WithoutHeader.data",
				"csv/DataHasMoreFields.data"
		};
		loadFiles("users", "csv", inputFiles);

		// setup up props for the sink connector
		this.settings.put(CONNECTOR_CLASS_CONFIG, CONNECTOR_CLASS);
		this.settings.put(SpoolDirSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, FILE_PATTERN);
		this.settings.put(SpoolDirSourceConnectorConfig.SCHEMA_GENERATION_ENABLED_CONF, "true");
		this.settings.put(SpoolDirCsvSourceConnectorConfig.CSV_FIRST_ROW_AS_HEADER_CONF, "true");

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

	@Test // that the connect loads multiple csv files and produces to a topic
	public void csvSourceWithHeaderAndWithSchema() throws Throwable {
		// setup connect source (csv files)
		String[] inputFiles = new String[]{ "csv/FieldsMatch.data" };
		loadFiles("users", "csv", inputFiles);

		// setup up props for the sink connector
		this.settings.put(CONNECTOR_CLASS_CONFIG, CONNECTOR_CLASS);
		this.settings.put(SpoolDirSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, FILE_PATTERN);
		this.settings.put(SpoolDirSourceConnectorConfig.SCHEMA_GENERATION_ENABLED_CONF, "false");
		this.settings.put(SpoolDirSourceConnectorConfig.KEY_SCHEMA_CONF, KEY_SCHEMA);
		this.settings.put(SpoolDirSourceConnectorConfig.VALUE_SCHEMA_CONF, VALUE_SCHEMA);
		this.settings.put(SpoolDirCsvSourceConnectorConfig.CSV_FIRST_ROW_AS_HEADER_CONF, "true");
		this.settings.put(SpoolDirCsvSourceConnectorConfig.CSV_NULL_FIELD_INDICATOR_CONF, "BOTH");

		// start the connector
		connect.configureConnector(CONNECTOR_NAME, this.settings);

		// wait for tasks to spin up
		waitForConnectorToStart(CONNECTOR_NAME, 1);

		// Verify the records were written to Kafka
		connect.kafka().consume(20, CONSUME_MAX_DURATION_MS, KAFKA_TOPIC);

		// assert that the correct number of files were moved to "finished"
		assertEquals(inputPath.listFiles(new PatternFilenameFilter(FILE_PATTERN)).length, 0);
		assertEquals(errorPath.listFiles(new PatternFilenameFilter(FILE_PATTERN)).length, 0);
		assertEquals(finishedPath.listFiles(new PatternFilenameFilter(FILE_PATTERN)).length, inputFiles.length);

		// todo: any assertions on the ConsumerRecords other than count?
	}

}
