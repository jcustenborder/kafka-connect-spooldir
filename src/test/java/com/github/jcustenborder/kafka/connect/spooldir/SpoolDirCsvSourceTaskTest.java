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
package com.github.jcustenborder.kafka.connect.spooldir;

import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import shaded.com.google.common.io.Files;
import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SpoolDirCsvSourceTaskTest extends AbstractSpoolDirSourceTaskTest<SpoolDirCsvSourceTask> {
  private static final Logger log = LoggerFactory.getLogger(SpoolDirCsvSourceTaskTest.class);

  @Override
  protected SpoolDirCsvSourceTask createTask() {
    return new SpoolDirCsvSourceTask();
  }

  @Override
  protected Map<String, String> settings() {
    Map<String, String> settings = super.settings();
    settings.put(SpoolDirCsvSourceConnectorConfig.CSV_FIRST_ROW_AS_HEADER_CONF, "true");
    settings.put(SpoolDirCsvSourceConnectorConfig.PARSER_TIMESTAMP_DATE_FORMATS_CONF, "yyyy-MM-dd'T'HH:mm:ss'Z'");
    settings.put(SpoolDirCsvSourceConnectorConfig.CSV_NULL_FIELD_INDICATOR_CONF, "BOTH");
    return settings;
  }

  @TestFactory
  public Stream<DynamicTest> poll() throws IOException {
    final String packageName = "csv";
    List<TestCase> testCases = loadTestCases(packageName);

    return testCases.stream().map(testCase -> {
      String name = Files.getNameWithoutExtension(testCase.path.toString());
      return dynamicTest(name, () -> {
        poll(packageName, testCase);
      });
    });
  }

  void writeCSV(File outputFile, Schema schema, List<Struct> structs) throws IOException {
    try (Writer writer = new FileWriter(outputFile)) {
      try (ICSVWriter csvWriter = new CSVWriterBuilder(writer)
          .build()) {
        String[] header = schema.fields().stream().map(Field::name).toArray(String[]::new);
        csvWriter.writeNext(header);

        for (Struct struct : structs) {
          List<String> values = new ArrayList<>();
          for (Field field : schema.fields()) {
            values.add(struct.get(field).toString());
          }
          csvWriter.writeNext(values.toArray(new String[0]));
        }

        csvWriter.flush();
      }
    }
  }

  @Test
  public void rebalance() throws IOException, InterruptedException {
    Schema schema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .build();
    final int count = 100;
    List<Struct> values = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      values.add(
          new Struct(schema)
              .put("id", i)
      );
    }
  
    File inputFile = this.getTargetFilePath(this.inputPath,  "input.csv");
    writeCSV(inputFile, schema, values);
    Map<String, String> settings = settings();
    settings.put(SpoolDirCsvSourceConnectorConfig.KEY_SCHEMA_CONF, ObjectMapperFactory.INSTANCE.writeValueAsString(schema));
    settings.put(SpoolDirCsvSourceConnectorConfig.VALUE_SCHEMA_CONF, ObjectMapperFactory.INSTANCE.writeValueAsString(schema));
    settings.put(SpoolDirCsvSourceConnectorConfig.BATCH_SIZE_CONF, "50");
    settings.put(SpoolDirCsvSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, ".*");
    SpoolDirCsvSourceTask task = new SpoolDirCsvSourceTask();
    SourceTaskContext sourceTaskContext = mock(SourceTaskContext.class);
    OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
    when(offsetStorageReader.offset(anyMap()))
        .thenReturn(null);
    when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);
    task.initialize(sourceTaskContext);
    task.start(settings);

    List<SourceRecord> records = new ArrayList<>();
    records.addAll(task.poll());
    assertEquals(50, records.size());
    SourceRecord lastRecord = records.get(49);
    when(offsetStorageReader.offset(anyMap())).thenReturn((Map<String, Object>) lastRecord.sourceOffset());

    task.stop();
    task.start(settings);
    records.addAll(task.poll());
    assertEquals(count, records.size(), "Expected number of records does not match.");
    assertNull(task.poll(), "Polling should be finished with the file by now.");
    assertNull(task.poll(), "Polling should be finished with the file by now.");


  }


}
