package com.github.jcustenborder.kafka.connect.spooldir;


import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.ICSVParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class SpoolDirCsvSourceConnectorConfigTest {
  private static final String KAFKA_TOPIC_VALUE = "kafka.topic.config.value";
  private static final String TOPIC_VALUE = "topic.config.value";
  private Map<String, String> settings;

  @BeforeEach
  public void setup() {
    settings = new HashMap<>();
    settings.put(SpoolDirCsvSourceConnectorConfig.CSV_SEPARATOR_CHAR_CONF, "0");
    settings.put(SpoolDirCsvSourceConnectorConfig.KAFKA_TOPIC_CONF, KAFKA_TOPIC_VALUE);
    settings.put(SpoolDirCsvSourceConnectorConfig.INPUT_PATH_CONFIG, "/tmp");
    settings.put(SpoolDirCsvSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, "^.+$");
    settings.put(SpoolDirCsvSourceConnectorConfig.ERROR_PATH_CONFIG, "/tmp");
    settings.put(SpoolDirCsvSourceConnectorConfig.FINISHED_PATH_CONFIG, "/tmp");
    settings.put(SpoolDirCsvSourceConnectorConfig.KEY_SCHEMA_CONF, "{\n" +
        "    \"name\" : \"com.example.users.UserKey\",\n" +
        "    \"type\" : \"STRUCT\",\n" +
        "    \"isOptional\" : false,\n" +
        "    \"fieldSchemas\" : {\n" +
        "      \"id\" : {\n" +
        "        \"type\" : \"INT64\",\n" +
        "        \"isOptional\" : false\n" +
        "      }\n" +
        "    }\n" +
        "  }");
    settings.put(SpoolDirCsvSourceConnectorConfig.VALUE_SCHEMA_CONF, "{\n" +
        "    \"name\" : \"com.example.users.UserKey\",\n" +
        "    \"type\" : \"STRUCT\",\n" +
        "    \"isOptional\" : false,\n" +
        "    \"fieldSchemas\" : {\n" +
        "      \"id\" : {\n" +
        "        \"type\" : \"INT64\",\n" +
        "        \"isOptional\" : false\n" +
        "      }\n" +
        "    }\n" +
        "  }");
  }


  @Test
  public void nullFieldSeparator() throws IOException {
    SpoolDirCsvSourceConnectorConfig config = new SpoolDirCsvSourceConnectorConfig(
        true,
        settings
    );

    ICSVParser parser = config.createCSVParserBuilder();
    try (StringReader reader = new StringReader("id\u0000test\n123\u0000foo")) {
      CSVReaderBuilder readerBuilder = config.createCSVReaderBuilder(reader, parser);
      try (CSVReader csvReader = readerBuilder.build()) {
        String[] line = csvReader.readNext();
        assertArrayEquals(new String[]{"id", "test"}, line);
        line = csvReader.readNext();
        assertArrayEquals(new String[]{"123", "foo"}, line);
      }
    }
  }

  @Test
  public void nullTopicConfigThrowsIllegalStateException() {
    settings.put(SpoolDirCsvSourceConnectorConfig.KAFKA_TOPIC_CONF, null);
    settings.put(SpoolDirCsvSourceConnectorConfig.TOPIC_CONF, null);

    assertThrows(IllegalStateException.class, () -> {
      new SpoolDirCsvSourceConnectorConfig(true, settings);
    });
  }

  @ParameterizedTest(name = "topic ConfigDef compatibility - 'kafka.topic' config: {0}, 'topic' config: {1}, expected: {2}")
  @MethodSource("createTopicTestArgs")
  public void kafkaTopicBackwardsCompatible(String kafkaTopicConf, String topicConf, String expected) {
    settings.put(SpoolDirCsvSourceConnectorConfig.KAFKA_TOPIC_CONF, kafkaTopicConf);
    settings.put(SpoolDirCsvSourceConnectorConfig.TOPIC_CONF, topicConf);

    SpoolDirCsvSourceConnectorConfig config = new SpoolDirCsvSourceConnectorConfig(true, settings);

    assertEquals(expected, config.topic, "topic was not the expected value");
  }

  /**
   * Generates a stream of Arguments for the kafkaTopicBackwardsCompatible @ParameterizedTest
   *
   * @return Stream<Arguments> that are used as the test cases
   */
  private static Stream<Arguments> createTopicTestArgs() {
    return Stream.of(
        Arguments.of(null, TOPIC_VALUE, TOPIC_VALUE),
        Arguments.of(KAFKA_TOPIC_VALUE, TOPIC_VALUE, KAFKA_TOPIC_VALUE),
        Arguments.of(KAFKA_TOPIC_VALUE, null, KAFKA_TOPIC_VALUE));
  }
}
