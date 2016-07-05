package io.confluent.kafka.connect.source.io;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.confluent.kafka.connect.source.Data;
import io.confluent.kafka.connect.source.io.processing.CSVRecordProcessor;
import io.confluent.kafka.connect.source.io.processing.CSVRecordProcessorConfig;
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

public class DirectoryMonitorTests {

  File tempDirectory;
  File inputDirectory;
  File errorDirectory;
  File finishedDirectory;
  Map<String, String> settings;

  @Test(expected = ConnectException.class)
  public void configure_MissingDirectories() {
    Map<?, ?> settings = ImmutableMap.of(
        PollingDirectoryMonitorConfig.INPUT_PATH_CONFIG, "/missing",
        PollingDirectoryMonitorConfig.ERROR_PATH_CONFIG, "/missing2",
        PollingDirectoryMonitorConfig.FINISHED_PATH_CONFIG, "/missing4",
        DirectoryMonitorConfig.RECORD_PROCESSOR_CLASS_CONF, CSVRecordProcessor.class.getName(),
        PollingDirectoryMonitorConfig.INPUT_FILE_PATTERN_CONF, "^.+\\.csv$"
    );

    PollingDirectoryMonitor monitor = new PollingDirectoryMonitor();
    monitor.configure(settings);
  }

  @Before
  public void before() {
    this.tempDirectory = Files.createTempDir();
    this.inputDirectory = new File(tempDirectory, "input");
    this.inputDirectory.mkdirs();
    this.errorDirectory = new File(tempDirectory, "error");
    this.errorDirectory.mkdirs();
    this.finishedDirectory = new File(tempDirectory, "finished");
    this.finishedDirectory.mkdirs();

    this.settings = new HashMap<>();

    this.settings.put(PollingDirectoryMonitorConfig.INPUT_PATH_CONFIG, inputDirectory.getAbsolutePath());
    this.settings.put(PollingDirectoryMonitorConfig.ERROR_PATH_CONFIG, errorDirectory.getAbsolutePath());
    this.settings.put(PollingDirectoryMonitorConfig.FINISHED_PATH_CONFIG, finishedDirectory.getAbsolutePath());
    this.settings.put(DirectoryMonitorConfig.RECORD_PROCESSOR_CLASS_CONF, CSVRecordProcessor.class.getName());
    this.settings.put(PollingDirectoryMonitorConfig.INPUT_FILE_PATTERN_CONF, "^.+\\.csv$");
    this.settings.put(CSVRecordProcessorConfig.TOPIC_CONF, "csv");
    this.settings.put(CSVRecordProcessorConfig.KEY_FIELDS_CONF, "ID");
    this.settings.put(CSVRecordProcessorConfig.FIRST_ROW_AS_HEADER_CONF, "true");
  }

  @Test
  public void configure() {
    PollingDirectoryMonitor monitor = new PollingDirectoryMonitor();
    monitor.configure(settings);
  }

  @Test
  public void poll_empty() {
    PollingDirectoryMonitor monitor = new PollingDirectoryMonitor();
    monitor.configure(settings);
    List<SourceRecord> results = monitor.poll();
    Assert.assertNotNull(results);
    Assert.assertTrue(results.isEmpty());
  }

  @Test
  public void poll() throws IOException {
    PollingDirectoryMonitor monitor = new PollingDirectoryMonitor();
    monitor.configure(settings);

    File inputFile = new File(this.inputDirectory, "input.csv");
    try (FileOutputStream outputStream = new FileOutputStream(inputFile)) {
      try (InputStream inputStream = Data.getMockData()) {
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
