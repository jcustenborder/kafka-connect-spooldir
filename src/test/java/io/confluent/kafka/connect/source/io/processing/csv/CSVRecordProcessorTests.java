/**
 * Copyright (C) 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.connect.source.io.processing.csv;

import com.google.common.io.Files;
import io.confluent.kafka.connect.source.Data;
import io.confluent.kafka.connect.source.SpoolDirectoryConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class CSVRecordProcessorTests {


  CSVRecordProcessor csvRecordProcessor;
  SpoolDirectoryConfig config;

  @Before
  public void setup() {
    this.csvRecordProcessor = new CSVRecordProcessor();
  }

  @Test
  public void schemaDefined() throws IOException {

    Map<String, String> settings = Data.settings(Files.createTempDir());
    settings.put(SpoolDirectoryConfig.RECORD_PROCESSOR_CLASS_CONF, CSVRecordProcessor.class.getName());
    this.config = new SpoolDirectoryConfig(settings);

    final String fileName = "Testing";

    try (InputStream inputStream = Data.mockData()) {
      this.csvRecordProcessor.configure(config, inputStream, fileName);
      List<SourceRecord> results = this.csvRecordProcessor.poll();
      Assert.assertNotNull(results);
    }

  }


}
