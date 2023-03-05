package com.github.jcustenborder.kafka.connect.spooldir;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Test;

public class MoveCleanupPolicyNoSubDirsTest extends AbstractCleanUpPolicyTest<AbstractCleanUpPolicy.Move> {
  @Override
  protected AbstractCleanUpPolicy.Move create(InputFile inputFile, File errorPath, File finishedPath) {
    return new AbstractCleanUpPolicy.Move(inputFile, errorPath, finishedPath);
  }

  @Test
  public void inputFileNotTreatedAsSubdir() throws IOException {
    assertNull(defineInputPathSubDir(), "Input path should not be a subdir");
    assertNull(this.inputFile.inputPathSubDir(), "Input file should have no subdir");
    this.inputFile.close();
  }
}
