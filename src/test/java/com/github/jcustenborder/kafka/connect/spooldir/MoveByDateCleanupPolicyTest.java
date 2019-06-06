package com.github.jcustenborder.kafka.connect.spooldir;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MoveByDateCleanupPolicyTest extends AbstractCleanUpPolicyTest<AbstractCleanUpPolicy.MoveByDate> {
  @Override
  protected AbstractCleanUpPolicy.MoveByDate create(File inputFile, File errorPath, File finishedPath) {
    return new AbstractCleanUpPolicy.MoveByDate(inputFile, errorPath, finishedPath);
  }

  @Test
  public void success() throws IOException {
    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
    Path subDirectory = Paths.get(this.finishedPath.getAbsolutePath(), dateFormatter.format(this.inputFile.lastModified()));
    File finishedFile = new File(subDirectory.toFile(), this.inputFile.getName());

    assertTrue(this.inputFile.exists(), "Input file should exist");
    assertFalse(finishedFile.exists(), "Finished file should not exist");

    this.cleanupPolicy.success();

    assertFalse(this.inputFile.exists(), "Input file should not exist");
    assertTrue(finishedFile.exists(), "Finished file should exist");
  }
}
