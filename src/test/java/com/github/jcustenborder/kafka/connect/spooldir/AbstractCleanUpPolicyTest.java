package com.github.jcustenborder.kafka.connect.spooldir;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;

public abstract class AbstractCleanUpPolicyTest<T extends AbstractCleanUpPolicy> {

  InputFile inputFile;
  File inputPath;
  File finishedPath;
  File errorPath;
  protected T cleanupPolicy;

  protected abstract T create(
      InputFile inputFile, File errorPath, File finishedPath
  );

  @BeforeEach
  public void before() throws IOException {
    this.errorPath = Files.createTempDir();
    this.finishedPath = Files.createTempDir();
    this.inputPath = Files.createTempDir();

    File inputFile = File.createTempFile("input", "file", this.inputPath);

    SpoolDirBinaryFileSourceConnectorConfig config = new SpoolDirBinaryFileSourceConnectorConfig(
        ImmutableMap.of(
            SpoolDirBinaryFileSourceConnectorConfig.TOPIC_CONF, "foo",
            SpoolDirBinaryFileSourceConnectorConfig.INPUT_PATH_CONFIG, this.inputPath.toString(),
            SpoolDirBinaryFileSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, "^.$",
            SpoolDirBinaryFileSourceConnectorConfig.ERROR_PATH_CONFIG, this.errorPath.toString(),
            SpoolDirBinaryFileSourceConnectorConfig.FINISHED_PATH_CONFIG, this.finishedPath.toString()
        )
    );

    this.inputFile = new InputFile(config, inputFile);
    this.inputFile.inputStreamReader = mock(InputStreamReader.class);
    this.inputFile.lineNumberReader = mock(LineNumberReader.class);
    this.cleanupPolicy = create(this.inputFile, this.errorPath, this.finishedPath);
  }

  @Test
  public void error() throws IOException {
    assertTrue(this.inputFile.exists(), "Input file should exist");
    this.cleanupPolicy.error();
    assertFalse(this.inputFile.exists(), "input file should not exist");
    File erroredFile = new File(this.errorPath, this.inputFile.getName());
    assertTrue(erroredFile.exists(), "errored file should exist.");
  }

  void delete(File file) {
    if (file.isDirectory()) {
      for (File child : Objects.requireNonNull(file.listFiles())) {
        delete(child);
      }
    }

    file.delete();
  }

  @AfterEach
  public void after() throws IOException {
    delete(this.finishedPath);
    delete(this.errorPath);
    delete(this.inputPath);
    verify(this.inputFile.inputStreamReader, only()).close();
    verify(this.inputFile.lineNumberReader, only()).close();
  }

}
