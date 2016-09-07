/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.connect.source.io.processing.line;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;
import io.confluent.kafka.connect.source.Data;
import io.confluent.kafka.connect.source.SpoolDirectoryConfig;
import io.confluent.kafka.connect.source.io.processing.FileMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LineRecordProcessorTest {

  LineRecordProcessor lineRecordProcessor;
  SpoolDirectoryConfig config;

  final String TOPIC = "testing";

  @Before
  public void setup() {
    this.lineRecordProcessor = new LineRecordProcessor();

    Map<String, String> settings = Data.settings(Files.createTempDir());
    settings.put(SpoolDirectoryConfig.RECORD_PROCESSOR_CLASS_CONF, LineRecordProcessor.class.getName());
    settings.put(SpoolDirectoryConfig.TOPIC_CONF, TOPIC);

    this.config = new SpoolDirectoryConfig(settings);
  }

  @Test
  public void poll() throws IOException {
    File tempfile = File.createTempFile(LineRecordProcessorTest.class.getSimpleName(), "test");
    tempfile.deleteOnExit();
    String inputFileName = Files.getNameWithoutExtension(tempfile.getName());


    final List<String> testLines = ImmutableList.of(
        "This is the first line",
        "This is the second line",
        "This is the third line",
        "This is the fourth line",
        "This is the fifth line",
        "This is the sixth line",
        "This is the seventh line",
        "This is the eighth line",
        "This is the ninth line",
        "This is the tenth line"
    );
    Files.asCharSink(tempfile, Charset.defaultCharset(), FileWriteMode.APPEND).writeLines(testLines);

    List<SourceRecord> expectedRecords = new ArrayList<>();
    int lineNumber = 1;
    for (String line : testLines) {
      Struct key = new Struct(this.lineRecordProcessor.defaultKeySchema);
      key.put(LineRecordProcessor.FIELD_FILENAME, tempfile.getName());
      key.put(LineRecordProcessor.FIELD_LINENUMBER, lineNumber);

      Map<String, ?> sourcePartition = ImmutableMap.of();
      Map<String, ?> sourceOffset = ImmutableMap.of(inputFileName, lineNumber);

      SourceRecord sourceRecord = new SourceRecord(
          sourcePartition,
          sourceOffset,
          TOPIC,
          this.lineRecordProcessor.defaultKeySchema,
          key,
          Schema.STRING_SCHEMA,
          line
      );

      expectedRecords.add(sourceRecord);

      lineNumber++;
    }


    List<SourceRecord> actualRecords;
    try (FileInputStream inputStream = new FileInputStream(tempfile)) {
      this.lineRecordProcessor.configure(this.config, inputStream, new FileMetadata(tempfile));
      actualRecords = this.lineRecordProcessor.poll();
    }

    Assert.assertThat(actualRecords, IsEqual.equalTo(expectedRecords));
  }

}
