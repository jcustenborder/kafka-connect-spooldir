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

import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.github.jcustenborder.kafka.connect.utils.AssertConnectRecord.assertSourceRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractSpoolDirSourceTaskTest<T extends AbstractSourceTask> {
  private static final Logger log = LoggerFactory.getLogger(AbstractSpoolDirSourceTaskTest.class);

  protected File tempDirectory;
  protected File inputPath;
  protected File errorPath;
  protected File finishedPath;
  protected T task;

  @BeforeEach
  public void setup() {
    this.tempDirectory = Files.createTempDir();
    this.finishedPath = new File(this.tempDirectory, "finished");
    this.inputPath = new File(this.tempDirectory, "input");
    this.errorPath = new File(this.tempDirectory, "error");
    this.finishedPath.mkdirs();
    this.inputPath.mkdirs();
    this.errorPath.mkdirs();
  }

  @BeforeEach
  public void configureIndent() {
    ObjectMapperFactory.INSTANCE.configure(SerializationFeature.INDENT_OUTPUT, true);
  }

  protected abstract T createTask();

  protected Map<String, String> settings() {
    Map<String, String> settings = Maps.newLinkedHashMap();
    settings.put(AbstractSourceConnectorConfig.INPUT_PATH_CONFIG, this.inputPath.getAbsolutePath());
    settings.put(AbstractSourceConnectorConfig.FINISHED_PATH_CONFIG, this.finishedPath.getAbsolutePath());
    settings.put(AbstractSourceConnectorConfig.ERROR_PATH_CONFIG, this.errorPath.getAbsolutePath());
    settings.put(AbstractSourceConnectorConfig.TOPIC_CONF, "testing");
    settings.put(AbstractSourceConnectorConfig.EMPTY_POLL_WAIT_MS_CONF, "10");
    return settings;
  }

  protected String defineInputPathSubDir() {
    return null;
  }

  protected File getTargetFilePath(File containerPath, String inputFileName) {
    String subDir = (this.defineInputPathSubDir() != null ? this.defineInputPathSubDir() : "");
    File targetDir = new File(containerPath, subDir);
    targetDir.mkdirs();
    return new File(targetDir, inputFileName);
  }

  protected void poll(final String packageName, TestCase testCase) throws InterruptedException, IOException {
    String keySchemaConfig = ObjectMapperFactory.INSTANCE.writeValueAsString(testCase.keySchema);
    String valueSchemaConfig = ObjectMapperFactory.INSTANCE.writeValueAsString(testCase.valueSchema);

    Map<String, String> settings = this.settings();
    settings.put(AbstractSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, String.format("^.*\\.%s", packageName));
    settings.put(AbstractSpoolDirSourceConnectorConfig.KEY_SCHEMA_CONF, keySchemaConfig);
    settings.put(AbstractSpoolDirSourceConnectorConfig.VALUE_SCHEMA_CONF, valueSchemaConfig);

    if (null != testCase.settings && !testCase.settings.isEmpty()) {
      settings.putAll(testCase.settings);
    }

    this.task = createTask();

    SourceTaskContext sourceTaskContext = mock(SourceTaskContext.class);
    OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
    when(offsetStorageReader.offset(anyMap())).thenReturn(testCase.offset);
    when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);
    this.task.initialize(sourceTaskContext);

    this.task.start(settings);

    String dataFile = new File(packageName, Files.getNameWithoutExtension(testCase.path.toString())) + ".data";
    log.trace("poll(String, TestCase) - dataFile={}", dataFile);

    String inputFileName = String.format("%s.%s",
        Files.getNameWithoutExtension(testCase.path.toString()),
        packageName
    );

    //Use this config because it's the simplest.
    SpoolDirBinaryFileSourceConnectorConfig config = new SpoolDirBinaryFileSourceConnectorConfig(settings);

    final File p = this.getTargetFilePath(this.inputPath, inputFileName);
    try (InputStream inputStream = this.getClass().getResourceAsStream(dataFile)) {
      assertNotNull(
          inputStream,
          String.format("Resource stream '%s' was not found", dataFile)
      );
      try (OutputStream outputStream = new FileOutputStream(p)) {
        ByteStreams.copy(inputStream, outputStream);
      }
    }

    final InputFile inputFile = new InputFile(config, p);
    log.trace("poll(String, TestCase) - inputFile = {}", inputFile);

    assertFalse(inputFile.processingFlag().exists(), String.format("processingFile %s should not exist before first poll().", inputFile.processingFlag()));
    assertTrue(inputFile.exists(), String.format("inputFile %s should exist.", inputFile));
    List<SourceRecord> records = this.task.poll();
    assertTrue(inputFile.exists(), String.format("inputFile %s should exist after first poll().", inputFile));
    assertTrue(inputFile.processingFlag().exists(), String.format("processingFile %s should exist after first poll().", inputFile.processingFlag()));

    assertNotNull(records, "records should not be null.");
    assertFalse(records.isEmpty(), "records should not be empty");
    assertEquals(testCase.expected.size(), records.size(), "records.size() does not match.");

    /*
    The following headers will change. Lets ensure they are there but we don't care about their
    values since they are driven by things that will change such as lastModified dates and paths.
     */
    List<String> headersToRemove = new ArrayList<String>();
    headersToRemove.add(Metadata.HEADER_LAST_MODIFIED);
    headersToRemove.add(Metadata.HEADER_PATH);
    headersToRemove.add(Metadata.HEADER_LENGTH);
    headersToRemove.add(Metadata.HEADER_NAME_WITHOUT_EXTENSION);
    headersToRemove.add(Metadata.HEADER_PARENT_DIR_NAME);
    headersToRemove.add(Metadata.HEADER_FILE_RELATIVE_PATH);

    for (int i = 0; i < testCase.expected.size(); i++) {
      SourceRecord expectedRecord = testCase.expected.get(i);
      SourceRecord actualRecord = records.get(i);

      for (String headerToRemove : headersToRemove) {
        assertNotNull(
            actualRecord.headers().lastWithName(headerToRemove),
            String.format("index:%s should have the header '%s'", i, headerToRemove)
        );
        actualRecord.headers().remove(headerToRemove);
        expectedRecord.headers().remove(headerToRemove);
      }
      assertSourceRecord(expectedRecord, actualRecord, String.format("index:%s", i));
    }

    records = this.task.poll();
    assertNull(records, "records should be null after first poll.");
    records = this.task.poll();
    assertNull(records, "records should be null after first poll.");
    assertFalse(inputFile.exists(), String.format("inputFile %s should not exist.", inputFile));
    assertFalse(inputFile.processingFlag().exists(), String.format("processingFile %s should not exist.", inputFile.processingFlag()));
    final File finishedFile = this.getTargetFilePath(this.finishedPath, inputFileName);
    assertTrue(finishedFile.exists(), String.format("finishedFile %s should exist.", finishedFile));
  }

  protected List<TestCase> loadTestCases(String packageName) throws IOException {
    String packagePrefix = String.format(
        "%s.%s",
        this.getClass().getPackage().getName(),
        packageName
    );
    log.trace("packagePrefix = {}", packagePrefix);
    List<TestCase> testCases = TestDataUtils.loadJsonResourceFiles(packagePrefix, TestCase.class);
    if (testCases.isEmpty() && log.isWarnEnabled()) {
      log.warn("No test cases were found in the resources. packagePrefix = {}", packagePrefix);
    }
    return testCases;
  }

  @Test
  public void version() {
    this.task = createTask();
    assertNotNull(this.task.version(), "version should not be null.");
  }

  @Test
  public void recordOffsetNPE() {
    this.task = createTask();
    long actual = this.task.recordOffset();
  }

  @AfterEach
  public void after() {
    if (null != this.task) {
      this.task.stop();
    }
  }
}
