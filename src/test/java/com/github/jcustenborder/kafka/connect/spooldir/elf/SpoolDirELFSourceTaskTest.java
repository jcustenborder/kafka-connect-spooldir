/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.spooldir.elf;

import com.github.jcustenborder.kafka.connect.spooldir.AbstractSpoolDirSourceTaskTest;
import com.github.jcustenborder.kafka.connect.spooldir.TestCase;
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

public class SpoolDirELFSourceTaskTest extends AbstractSpoolDirSourceTaskTest<SpoolDirELFSourceTask> {
  private static final Logger log = LoggerFactory.getLogger(SpoolDirELFSourceTaskTest.class);

  @Override
  protected SpoolDirELFSourceTask createTask() {
    return new SpoolDirELFSourceTask();
  }

  @Override
  protected Map<String, String> settings() {
    Map<String, String> settings = super.settings();
//    settings.put(SpoolDirELFSourceConnectorConfig.CSV_FIRST_ROW_AS_HEADER_CONF, "true");
//    settings.put(SpoolDirCsvSourceConnectorConfig.CSV_NULL_FIELD_INDICATOR_CONF, "BOTH");
//    settings.put(SpoolDirCsvSourceConnectorConfig.PARSER_TIMESTAMP_DATE_FORMATS_CONF, "yyyy-MM-dd'T'HH:mm:ss'Z'");
    return settings;
  }

  @TestFactory
  public Stream<DynamicTest> poll() throws IOException {
    final String packageName = "elf";
    List<TestCase> testCases = loadTestCases(packageName);

    return testCases.stream().map(testCase -> {
      String name = Files.getNameWithoutExtension(testCase.path.toString());
      return dynamicTest(name, () -> {
        poll(packageName, testCase);

      });
    });
  }
}