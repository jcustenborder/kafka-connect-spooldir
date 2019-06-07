package com.github.jcustenborder.kafka.connect.spooldir;

import com.google.common.io.Files;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractCleanUpPolicyTest<T extends AbstractCleanUpPolicy> {

  InputFile inputFile;
  File finishedPath;
  File errorPath;
  T cleanupPolicy;

  protected abstract T create(
      InputFile inputFile, File errorPath, File finishedPath
  );

  @BeforeEach
  public void before() throws IOException {
    this.errorPath = Files.createTempDir();
    this.finishedPath = Files.createTempDir();
    File inputFile = File.createTempFile("input", "file");
    File processingFlag = new File(inputFile.getParent(), inputFile.getName() + ".PROCESSING");
    this.inputFile = new InputFile(inputFile, processingFlag, 0);
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
  public void after() {
    this.inputFile.delete();
    delete(this.finishedPath);
    delete(this.errorPath);
  }

}
