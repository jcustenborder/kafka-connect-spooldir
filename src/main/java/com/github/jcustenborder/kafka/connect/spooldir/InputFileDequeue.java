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

import shaded.com.google.common.collect.ForwardingDeque;
import shaded.com.google.common.io.PatternFilenameFilter;

import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InputFileDequeue extends ForwardingDeque<InputFile> {
  private static final Logger log = LoggerFactory.getLogger(InputFileDequeue.class);
  private final AbstractSourceConnectorConfig config;
  private final FileComparator fileComparator;
  private final Predicate<File> processingFileExists;
  private final Predicate<File> fileMinimumAge;
  private final Predicate<File> filePartitionSelector;


  public InputFileDequeue(AbstractSourceConnectorConfig config) {
    this.config = config;
    this.fileComparator = new FileComparator(config.fileSortAttributes);
    this.processingFileExists = new ProcessingFileExistsPredicate(config.processingFileExtension);
    this.fileMinimumAge = new MinimumFileAgePredicate(config.minimumFileAgeMS);
    this.filePartitionSelector = AbstractTaskPartitionerPredicate.create(config);
  }

  Deque<InputFile> files;

  static File processingFile(String processingFileExtension, File input) {
    String fileName = input.getName() + processingFileExtension;
    return new File(input.getParentFile(), fileName);
  }

  @Override
  protected Deque<InputFile> delegate() {
    if (null != files && !files.isEmpty()) {
      return files;
    }

    log.trace("delegate() - Searching for file(s) in {}", this.config.inputPath);

    File[] input = null;

    if (this.config.filesWalkRecursively) {

      final PatternFilenameFilter walkerFilenameFilter = this.config.inputFilenameFilter;
      Predicate<Path> filenameFilterPredicate = new Predicate<Path>() {
        @Override
        public boolean test(Path t) {
          return walkerFilenameFilter.accept(t.getParent().toFile(), 
            t.getFileName().toFile().getName());
        }
      };

      try (Stream<Path> filesWalk = Files.walk(this.config.inputPath.toPath())) {

        List<Path> matchedPaths = filesWalk.filter(Files::isRegularFile).filter(filenameFilterPredicate).collect(Collectors.toList());

        List<File> asFiles = new ArrayList<File>();
        matchedPaths.forEach(path -> {
          asFiles.add(path.toFile());
        });

        input = asFiles.toArray(new File[]{});

      } catch (IOException e) {
        log.error("Unexpected eror walking {}: {}", this.config.inputPath.toPath(), e.getMessage(), e);
        return new ArrayDeque<>();
      }
    
    } else {
      input = this.config.inputPath.listFiles(this.config.inputFilenameFilter);
    }

    if (null == input || input.length == 0) {
      log.info("No files matching {} were found in {}", AbstractSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, this.config.inputPath);
      return new ArrayDeque<>();
    }
    log.trace("delegate() - Found {} potential file(s).", input.length);
    this.files = Arrays.stream(input)
        .filter(this.filePartitionSelector)
        .filter(this.processingFileExists)
        .filter(this.fileMinimumAge)
        .sorted(this.fileComparator)
        .map(f -> new InputFile(this.config, f))
        .collect(Collectors.toCollection(ArrayDeque::new));
    return this.files;
  }


  static class ProcessingFileExistsPredicate implements Predicate<File> {
    final String processingFileExtension;

    ProcessingFileExistsPredicate(String processingFileExtension) {
      this.processingFileExtension = processingFileExtension;
    }

    @Override
    public boolean test(File file) {
      File processingFile = processingFile(this.processingFileExtension, file);
      log.trace("Checking for processing file: {}", processingFile);
      return !processingFile.exists();
    }
  }

  static class MinimumFileAgePredicate implements Predicate<File> {
    final long minimumFileAgeMS;
    final Time time;

    /**
     * @param minimumFileAgeMS Minimum time since last write in milliseconds.
     */
    MinimumFileAgePredicate(long minimumFileAgeMS) {
      this(minimumFileAgeMS, Time.SYSTEM);
    }

    /**
     * Constructor is only used for testing.
     *
     * @param minimumFileAgeMS
     * @param time
     */
    MinimumFileAgePredicate(long minimumFileAgeMS, Time time) {
      this.minimumFileAgeMS = minimumFileAgeMS;
      this.time = time;
    }


    @Override
    public boolean test(File file) {
      long fileAgeMS = this.time.milliseconds() - file.lastModified();

      if (fileAgeMS < 0L) {
        log.warn("File {} has a date in the future.", file);
      }
      if (fileAgeMS >= this.minimumFileAgeMS) {
        return true;
      } else {
        log.debug("Skipping {} because it does not meet the minimum age.", file);
        return false;
      }
    }
  }

}
