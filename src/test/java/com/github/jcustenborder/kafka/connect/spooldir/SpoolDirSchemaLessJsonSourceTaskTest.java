package com.github.jcustenborder.kafka.connect.spooldir;

import com.google.common.io.Files;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class SpoolDirSchemaLessJsonSourceTaskTest extends SpoolDirSourceTaskTest<SpoolDirSchemaLessJsonSourceTask> {
  private static final Logger log = LoggerFactory.getLogger(SpoolDirJsonSourceTaskTest.class);

  @Override
  protected SpoolDirSchemaLessJsonSourceTask createTask() {
    return new SpoolDirSchemaLessJsonSourceTask();
  }

  @Override
  protected void settings(Map<String, String> settings) {
    settings.put(SpoolDirCsvSourceConnectorConfig.CSV_FIRST_ROW_AS_HEADER_CONF, "true");
    settings.put(SpoolDirCsvSourceConnectorConfig.PARSER_TIMESTAMP_DATE_FORMATS_CONF, "yyyy-MM-dd'T'HH:mm:ss'Z'");
    settings.put(SpoolDirCsvSourceConnectorConfig.CSV_NULL_FIELD_INDICATOR_CONF, "BOTH");
  }

  @TestFactory
  public Stream<DynamicTest> poll() throws IOException {
    final String packageName = "schemalessjson";
    List<TestCase> testCases = loadTestCases(packageName);

    return testCases.stream().map(testCase -> {
      String name = Files.getNameWithoutExtension(testCase.path.toString());
      return dynamicTest(name, () -> {
        poll(packageName, testCase);
      });
    });
  }
}
