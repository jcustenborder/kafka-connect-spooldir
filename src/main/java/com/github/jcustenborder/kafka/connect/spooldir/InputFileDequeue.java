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

import com.google.common.collect.ForwardingDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

public class InputFileDequeue extends ForwardingDeque<File> {
  private static final Logger log = LoggerFactory.getLogger(InputFileDequeue.class);
  private final SpoolDirSourceConnectorConfig config;

  public InputFileDequeue(SpoolDirSourceConnectorConfig config) {
    this.config = config;
  }

  public static File processingFile(String processingFileExtension, File input) {
    String fileName = input.getName() + processingFileExtension;
    return new File(input.getParentFile(), fileName);
  }
  
  public static File errorFile(String errorFileExtension, File errorPath, File input) {
    String fileName = input.getName() + errorFileExtension;
    return new File(errorPath, fileName);
  }

  Deque<File> files;

  @Override
  protected Deque<File> delegate() {
    if (null != files && !files.isEmpty()) {
      return files;
    }

    log.info("Searching for file in {}", this.config.inputPath);
    File[] input = this.config.inputPath.listFiles(this.config.inputFilenameFilter);
    if (null == input || input.length == 0) {
      log.info("No files matching {} were found in {}", SpoolDirSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, this.config.inputPath);
      return new ArrayDeque<>();
    }
    Arrays.sort(input, Comparator.comparing(File::getName));
    List<File> files = new ArrayList<>(input.length);
    for (File f : input) {
      File processingFile = processingFile(this.config.processingFileExtension, f);
      log.trace("Checking for processing file: {}", processingFile);

      if (processingFile.exists()) {
        log.debug("Skipping {} because processing file exists.", f);
        continue;
      }
      files.add(f);
    }

    Deque<File> result = new ArrayDeque<>(files.size());

    for (File file : files) {
      long fileAgeMS = System.currentTimeMillis() - file.lastModified();

      if (fileAgeMS < 0L) {
        log.warn("File {} has a date in the future.", file);
      }

      if (this.config.minimumFileAgeMS > 0L && fileAgeMS < this.config.minimumFileAgeMS) {
        log.debug("Skipping {} because it does not meet the minimum age.", file);
        continue;
      }
      result.add(file);
    }

    log.info("Found {} file(s) to process", result.size());
    return (this.files = result);
  }
}
