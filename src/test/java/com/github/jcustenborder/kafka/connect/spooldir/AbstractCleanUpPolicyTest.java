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
  String inputPathSubDir;
  protected T cleanupPolicy;

  protected abstract T create(
      InputFile inputFile, File errorPath, File finishedPath
  );

  protected String defineInputPathSubDir() {
    return null;
  }

  protected ImmutableMap.Builder<String,String> getConnectorConfigMap() {
    return new ImmutableMap.Builder<String,String>()
        .put(SpoolDirBinaryFileSourceConnectorConfig.TOPIC_CONF, "foo")
        .put(SpoolDirBinaryFileSourceConnectorConfig.INPUT_PATH_CONFIG, this.inputPath.toString())
        .put(SpoolDirBinaryFileSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, "^.$")
        .put(SpoolDirBinaryFileSourceConnectorConfig.ERROR_PATH_CONFIG, this.errorPath.toString())
        .put(SpoolDirBinaryFileSourceConnectorConfig.FINISHED_PATH_CONFIG, this.finishedPath.toString());
  }

  @BeforeEach
  public void before() throws IOException {
    this.errorPath = Files.createTempDir();
    this.finishedPath = Files.createTempDir();
    this.inputPath = Files.createTempDir();
    this.inputPathSubDir = defineInputPathSubDir();

    File tempFileParentPathDir = this.inputPath;
    if (this.inputPathSubDir != null) {
      tempFileParentPathDir = new File(this.inputPath, this.inputPathSubDir);
      tempFileParentPathDir.mkdirs();
    }

    File inputFile = File.createTempFile("input", "file", tempFileParentPathDir);

    SpoolDirBinaryFileSourceConnectorConfig config =
        new SpoolDirBinaryFileSourceConnectorConfig(getConnectorConfigMap().build());

    this.inputFile = new InputFile(config, inputFile);
    this.inputFile.inputStreamReader = mock(InputStreamReader.class);
    this.inputFile.lineNumberReader = mock(LineNumberReader.class);
    this.cleanupPolicy = create(this.inputFile, this.errorPath, this.finishedPath);
  }

  protected File getTargetFilePath(File containerPath, InputFile inputFile) {
    String subDir = (this.defineInputPathSubDir() != null ? this.defineInputPathSubDir() : "");
    return new File(new File(containerPath,subDir), inputFile.getName());
  }

  @Test
  public void error() throws IOException {
    assertTrue(this.inputFile.exists(), "Input file should exist");
    this.cleanupPolicy.error();
    assertFalse(this.inputFile.exists(), "input file should not exist");
    File erroredFile = this.getTargetFilePath(this.errorPath,this.inputFile);
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
