package com.github.jcustenborder.kafka.connect.spooldir;

import com.google.common.io.Files;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProcessingFileExistsPredicateTest {
  File inputFile;
  InputFileDequeue.ProcessingFileExistsPredicate predicate;

  static final String EXTENSION = "processing";

  @BeforeEach
  public void before() throws IOException {
    this.inputFile = File.createTempFile("test", "file");
    this.predicate = new InputFileDequeue.ProcessingFileExistsPredicate(EXTENSION);
  }

  @AfterEach
  public void after() throws IOException {
    if (null != this.inputFile && this.inputFile.exists()) {
      this.inputFile.delete();
    }
  }

  @Test
  public void test() throws IOException {
    File processingFlag = InputFileDequeue.processingFile(EXTENSION, this.inputFile);
    Files.touch(processingFlag);
    assertFalse(this.predicate.test(this.inputFile));
    processingFlag.delete();
    assertTrue(this.predicate.test(this.inputFile));
  }
}
