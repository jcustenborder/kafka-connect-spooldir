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

public class SpoolDirBinaryFileSourceTaskTest extends AbstractSpoolDirSourceTaskTest<SpoolDirBinaryFileSourceTask> {
  private static final Logger log = LoggerFactory.getLogger(SpoolDirJsonSourceTaskTest.class);

  @Override
  protected SpoolDirBinaryFileSourceTask createTask() {
    return new SpoolDirBinaryFileSourceTask();
  }

  @Override
  protected Map<String, String> settings() {
    Map<String, String> settings = super.settings();
    return settings;
  }

  @TestFactory
  public Stream<DynamicTest> poll() throws IOException {
    final String packageName = "binary";
    List<TestCase> testCases = loadTestCases(packageName);

    return testCases.stream().map(testCase -> {
      String name = Files.getNameWithoutExtension(testCase.path.toString());
      return dynamicTest(name, () -> {
        poll(packageName, testCase);
      });
    });
  }
}
