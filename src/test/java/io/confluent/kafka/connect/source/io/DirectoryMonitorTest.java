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
package io.confluent.kafka.connect.source.io;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.confluent.kafka.connect.source.Data;
import io.confluent.kafka.connect.source.SpoolDirectoryConfig;
import io.confluent.kafka.connect.source.io.processing.csv.CSVRecordProcessor;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.hamcrest.core.IsEqual;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DirectoryMonitorTest {

  File tempDirectory;
  SpoolDirectoryConfig config;

  @Test(expected = ConnectException.class)
  public void configure_MissingDirectories() {
    Map<String, String> settings = new HashMap<>();
    settings.put(SpoolDirectoryConfig.INPUT_PATH_CONFIG, "/missing");
    settings.put(SpoolDirectoryConfig.ERROR_PATH_CONFIG, "/missing2");
    settings.put(SpoolDirectoryConfig.FINISHED_PATH_CONFIG, "/missing4");
    settings.put(SpoolDirectoryConfig.RECORD_PROCESSOR_CLASS_CONF, CSVRecordProcessor.class.getName());
    settings.put(SpoolDirectoryConfig.INPUT_FILE_PATTERN_CONF, "^.+\\.csv$");
    settings.put(SpoolDirectoryConfig.TOPIC_CONF, "input");

    PollingDirectoryMonitor monitor = new PollingDirectoryMonitor();
    monitor.configure(new SpoolDirectoryConfig(settings));
  }

  @Before
  public void before() {
    this.tempDirectory = Files.createTempDir();
    Map<String, String> settings = Data.settings(this.tempDirectory);
    this.config = new SpoolDirectoryConfig(settings);
  }

  @Test
  public void configure() {
    PollingDirectoryMonitor monitor = new PollingDirectoryMonitor();
    monitor.configure(config);
  }

  @Test
  public void poll_empty() {
    PollingDirectoryMonitor monitor = new PollingDirectoryMonitor();
    monitor.configure(config);
    List<SourceRecord> results = monitor.poll();
    Assert.assertNotNull(results);
    Assert.assertTrue(results.isEmpty());
  }

  @Test
  public void poll() throws IOException {
    PollingDirectoryMonitor monitor = new PollingDirectoryMonitor();
    monitor.configure(config);

    File inputFile = new File(this.config.inputPath(), "input.csv");
    try (FileOutputStream outputStream = new FileOutputStream(inputFile)) {
      try (InputStream inputStream = Data.mockData()) {
        ByteStreams.copy(inputStream, outputStream);
      }
    }

    List<SourceRecord> results;
    for (int i = 0; i < 10; i++) {
      results = monitor.poll();
      Assert.assertNotNull(results);
      Assert.assertFalse(results.isEmpty());
      Assert.assertThat(results.size(), IsEqual.equalTo(100));
    }

    //
    results = monitor.poll();
    Assert.assertNotNull(results);
    Assert.assertTrue(results.isEmpty());

    results = monitor.poll();
    Assert.assertNotNull(results);
    Assert.assertTrue(results.isEmpty());
  }


  @After
  public void after() {
    tempDirectory.delete();
  }

}
