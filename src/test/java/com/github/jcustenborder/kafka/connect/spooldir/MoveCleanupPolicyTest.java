package com.github.jcustenborder.kafka.connect.spooldir;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MoveCleanupPolicyTest extends AbstractCleanUpPolicyTest<AbstractCleanUpPolicy.Move> {
  @Override
  protected AbstractCleanUpPolicy.Move create(InputFile inputFile, File errorPath, File finishedPath) {
    return new AbstractCleanUpPolicy.Move(inputFile, errorPath, finishedPath);
  }

  @Test
  public void success() throws IOException {
    File finishedFile = this.getTargetFilePath(this.finishedPath, this.inputFile);
    assertTrue(this.inputFile.exists(), "Input file should exist");
    assertFalse(finishedFile.exists(), "Finished file should not exist");
    this.cleanupPolicy.success();
    assertFalse(this.inputFile.exists(), "Input file should not exist");
    assertTrue(finishedFile.exists(), "Finished file should exist");
  }
}
