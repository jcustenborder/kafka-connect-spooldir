package com.github.jcustenborder.kafka.connect.spooldir;


import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.ICSVParser;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class SpoolDirCsvSourceConnectorConfigTest {

  @Test
  public void nullFieldSeparator() throws IOException {
    Map<String, String> settings = new HashMap<>();
    settings.put(SpoolDirCsvSourceConnectorConfig.CSV_SEPARATOR_CHAR_CONF, "0");
    settings.put(SpoolDirCsvSourceConnectorConfig.TOPIC_CONF, "test");
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

}
