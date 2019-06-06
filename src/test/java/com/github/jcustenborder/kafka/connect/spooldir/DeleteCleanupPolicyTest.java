package com.github.jcustenborder.kafka.connect.spooldir;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeleteCleanupPolicyTest extends AbstractCleanUpPolicyTest<AbstractCleanUpPolicy.Delete> {
  @Override
  protected AbstractCleanUpPolicy.Delete create(InputFile inputFile, File errorPath, File finishedPath) {
    return new AbstractCleanUpPolicy.Delete(inputFile, errorPath, finishedPath);
  }

  @Test
  public void success() throws IOException {
    assertTrue(this.inputFile.exists(), "Input file should exist");
    this.cleanupPolicy.success();
    assertFalse(this.inputFile.exists(), "Input file should not exist");
  }
}
