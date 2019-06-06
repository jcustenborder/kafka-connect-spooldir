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

import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;

abstract class AbstractCleanUpPolicy implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(AbstractCleanUpPolicy.class);
  private static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
  protected final File inputFile;
  protected final File errorPath;
  protected final File finishedPath;


  protected AbstractCleanUpPolicy(File inputFile, File errorPath, File finishedPath) {
    this.inputFile = inputFile;
    this.errorPath = errorPath;
    this.finishedPath = finishedPath;
  }


  public static AbstractCleanUpPolicy create(AbstractSourceConnectorConfig config, InputFile inputFile) throws IOException {
    final AbstractCleanUpPolicy result;
    switch (config.cleanupPolicy) {
      case MOVE:
        result = new Move(inputFile.inputFile, config.errorPath, config.finishedPath);
        break;
      case MOVEBYDATE:
        result = new MoveByDate(inputFile.inputFile, config.errorPath, config.finishedPath);
        break;
      case DELETE:
        result = new Delete(inputFile.inputFile, config.errorPath, config.finishedPath);
        break;
      case NONE:
        result = new None(inputFile.inputFile, config.errorPath, config.finishedPath);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("%s is not supported", config.cleanupPolicy)
        );
    }

    return result;
  }

  protected void removeFile(File file) {
    log.info("Removing {}", file);
    if (!file.delete()) {
      log.warn("Could not delete {}", file);
    }
  }

  protected void moveToDirectory(File outputDirectory) {
    File outputFile = new File(outputDirectory, this.inputFile.getName());
    try {
      if (this.inputFile.exists()) {
        log.info("Moving {} to {}", this.inputFile, outputFile);
        Files.move(this.inputFile, outputFile);
      }
    } catch (IOException e) {
      log.error("Exception thrown while trying to move {} to {}", this.inputFile, outputFile, e);
    }
  }

  protected boolean createDirectory(File directory) {
    if (directory.exists()) {
      return true;
    }
    if (!directory.mkdir()) {
      log.error("Cannot make directory - " + directory.getAbsolutePath());
      return false;
    }
    if (!directory.setWritable(true)) {
      log.error("Cannot make directory writable - " + directory.getAbsolutePath());
      return false;
    }
    return true;
  }

  @Override
  public void close() throws IOException {

  }

  /**
   * Method is used to handle file cleanup when processing the file has errored.
   */
  public void error() {
    log.error(
        "Error during processing, moving {} to {}.",
        this.inputFile,
        this.errorPath
    );
    moveToDirectory(this.errorPath);
  }

  /**
   * Method is used to handle file cleanup when processing the file was successful.
   */
  public abstract void success() throws IOException;

  static class Move extends AbstractCleanUpPolicy {
    protected Move(File inputFile, File errorPath, File finishedPath) {
      super(inputFile, errorPath, finishedPath);
    }

    @Override
    public void success() throws IOException {
      moveToDirectory(this.finishedPath);
    }
  }

  static class MoveByDate extends AbstractCleanUpPolicy {
    protected MoveByDate(File inputFile, File errorPath, File finishedPath) {
      super(inputFile, errorPath, finishedPath);
    }

    @Override
    public void success() throws IOException {
      // Setup directory named as the file created date
      File subDirectory = new File(this.finishedPath, dateFormatter.format(this.inputFile.lastModified()));
      log.trace("Finished path: {}", subDirectory);

      if (createDirectory(subDirectory)) {
        moveToDirectory(subDirectory);
      } else {
        moveToDirectory(this.finishedPath);
      }
    }
  }

  static class Delete extends AbstractCleanUpPolicy {
    protected Delete(File inputFile, File errorPath, File finishedPath) {
      super(inputFile, errorPath, finishedPath);
    }

    @Override
    public void success() throws IOException {
      removeFile(this.inputFile);
    }
  }

  static class None extends AbstractCleanUpPolicy {
    protected None(File inputFile, File errorPath, File finishedPath) {
      super(inputFile, errorPath, finishedPath);
    }

    @Override
    public void success() throws IOException {
      log.trace("Leaving {}", this.inputFile);
    }
  }
}
