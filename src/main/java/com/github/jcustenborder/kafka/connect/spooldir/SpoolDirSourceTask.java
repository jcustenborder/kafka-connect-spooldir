/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.spooldir;

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.data.Parser;
import com.github.jcustenborder.kafka.connect.utils.data.type.DateTypeParser;
import com.github.jcustenborder.kafka.connect.utils.data.type.TimeTypeParser;
import com.github.jcustenborder.kafka.connect.utils.data.type.TimestampTypeParser;
import com.github.jcustenborder.kafka.connect.utils.data.type.TypeParser;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class SpoolDirSourceTask<CONF extends SpoolDirSourceConnectorConfig> extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(SpoolDirSourceTask.class);
  protected Parser parser;
  protected Map<String, ?> sourcePartition;
  CONF config;
  Stopwatch processingTime = Stopwatch.createStarted();
  private File inputFile;
  private long inputFileModifiedTime;
  private InputStream inputStream;
  private boolean hasRecords = false;
  private Map<String, String> metadata;

  private static void checkDirectory(String key, File directoryPath) {
    if (log.isInfoEnabled()) {
      log.info("Checking if directory {} '{}' exists.",
          key,
          directoryPath
      );
    }

    String errorMessage = String.format(
        "Directory for '%s' '%s' does not exist ",
        key,
        directoryPath
    );

    if (!directoryPath.isDirectory()) {
      throw new ConnectException(
          errorMessage,
          new FileNotFoundException(directoryPath.getAbsolutePath())
      );
    }

    if (log.isInfoEnabled()) {
      log.info("Checking to ensure {} '{}' is writable ", key, directoryPath);
    }

    errorMessage = String.format(
        "Directory for '%s' '%s' it not writable.",
        key,
        directoryPath
    );

    File temporaryFile = null;

    try {
      temporaryFile = File.createTempFile(".permission", ".testing", directoryPath);
    } catch (IOException ex) {
      throw new ConnectException(
          errorMessage,
          ex
      );
    } finally {
      try {
        if (null != temporaryFile && temporaryFile.exists()) {
          Preconditions.checkState(temporaryFile.delete(), "Unable to delete temp file in %s", directoryPath);
        }
      } catch (Exception ex) {
        if (log.isWarnEnabled()) {
          log.warn("Exception thrown while deleting {}.", temporaryFile, ex);
        }
      }
    }
  }

  protected abstract CONF config(Map<String, ?> settings);

  protected abstract void configure(InputStream inputStream, Map<String, String> metadata, Long lastOffset) throws IOException;

  protected abstract List<SourceRecord> process() throws IOException;

  protected abstract long recordOffset();

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @Override
  public void start(Map<String, String> settings) {
    this.config = config(settings);

    checkDirectory(SpoolDirSourceConnectorConfig.INPUT_PATH_CONFIG, this.config.inputPath);
    checkDirectory(SpoolDirSourceConnectorConfig.FINISHED_PATH_CONFIG, this.config.finishedPath);
    checkDirectory(SpoolDirSourceConnectorConfig.ERROR_PATH_CONFIG, this.config.errorPath);

    this.parser = new Parser();
    Map<Schema, TypeParser> dateTypeParsers = ImmutableMap.of(
        Timestamp.SCHEMA, new TimestampTypeParser(this.config.parserTimestampTimezone, this.config.parserTimestampDateFormats),
        Date.SCHEMA, new DateTypeParser(this.config.parserTimestampTimezone, this.config.parserTimestampDateFormats),
        Time.SCHEMA, new TimeTypeParser(this.config.parserTimestampTimezone, this.config.parserTimestampDateFormats)
    );

    for (Map.Entry<Schema, TypeParser> kvp : dateTypeParsers.entrySet()) {
      this.parser.registerTypeParser(kvp.getKey(), kvp.getValue());
    }
  }

  @Override
  public void stop() {

  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    log.trace("poll()");
    List<SourceRecord> results = read();

    if (results.isEmpty()) {
      log.trace("read() returned empty list. Sleeping {} ms.", this.config.emptyPollWaitMs);
      Thread.sleep(this.config.emptyPollWaitMs);
    }

    log.trace("read() returning {} result(s)", results.size());

    return results;
  }

  private void closeAndMoveToFinished(File outputDirectory, boolean errored) throws IOException {
    if (null != inputStream) {
      log.info("Closing {}", this.inputFile);

      this.inputStream.close();
      this.inputStream = null;

      File finishedFile = new File(outputDirectory, this.inputFile.getName());

      if (errored) {
        log.error("Error during processing, moving {} to {}.", this.inputFile, outputDirectory);
      }

      // delete files that are successfully ingested
      if (this.config.keepFinishedFiles) {
        Files.move(this.inputFile, finishedFile);
        log.info("Finished processing {} in {} second(s). Moving to {}.", this.inputFile, processingTime.elapsed(TimeUnit.SECONDS), outputDirectory);
      } else {
        this.inputFile.delete();
        log.info("Finished processing {} in {} second(s). Deleting it.", this.inputFile, processingTime.elapsed(TimeUnit.SECONDS));

        String fileName = outputDirectory + "/finished.file.name.list";
        String nowString = LocalDate.now() + " " + LocalTime.now();
        String aLine = nowString + "\t" + this.inputFile.getAbsolutePath() + System.lineSeparator();
        java.nio.file.Files.write(Paths.get(fileName),
                aLine.getBytes(Charset.defaultCharset()),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
        log.info("Appending file name to {}.", fileName);
      }

      File processingFile = processingFile(this.inputFile);
      if (processingFile.exists()) {
        log.info("Removing processing file {}", processingFile);
        processingFile.delete();
      }

    }
  }

  File processingFile(File input) {
    String fileName = input.getName() + this.config.processingFileExtension;
    return new File(input.getParentFile(), fileName);
  }

  File findNextInputFile() {
    // search for files recursively
    Collection<File> toCheckFiles = FileUtils.listFiles(
            this.config.inputPath,
            this.config.inputFilenameFilter,
            DirectoryFileFilter.DIRECTORY
    );

    if (toCheckFiles.isEmpty()) {
      log.debug("No files matching {} were found in {}", SpoolDirSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, this.config.inputPath);
      return null;
    }

    List<File> files = new ArrayList<>();
    for (File f : toCheckFiles) {
      File processingFile = processingFile(f);
      log.trace("Checking for processing file: {}", processingFile);

      if (processingFile.exists()) {
        log.debug("Skipping {} because processing file exists.", f);
        continue;
      }
      files.add(f);
    }

    File result = null;
    for (File file : files) {
      long fileAgeMS = System.currentTimeMillis() - file.lastModified();

      if (fileAgeMS < 0L) {
        log.warn("File {} has a date in the future.", file);
      }

      if (this.config.minimumFileAgeMS > 0L && fileAgeMS < this.config.minimumFileAgeMS) {
        log.debug("Skipping {} because it does not meet the minimum age.", file);
        continue;
      }
      result = file;
      break;
    }

    return result;
  }

  public List<SourceRecord> read() {
    try {
      if (!hasRecords) {
        closeAndMoveToFinished(this.config.finishedPath, false);

        File nextFile = findNextInputFile();
        if (null == nextFile) {
          return new ArrayList<>();
        }

        this.metadata = ImmutableMap.of();
        this.inputFile = nextFile;
        this.inputFileModifiedTime = this.inputFile.lastModified();
        File processingFile = processingFile(this.inputFile);
        Files.touch(processingFile);

        try {
          this.sourcePartition = ImmutableMap.of(
              "fileName", this.inputFile.getName()
          );
          log.info("Opening {}", this.inputFile);
          Long lastOffset = null;
          log.trace("looking up offset for {}", this.sourcePartition);
          Map<String, Object> offset = this.context.offsetStorageReader().offset(this.sourcePartition);
          if (null != offset && !offset.isEmpty()) {
            Number number = (Number) offset.get("offset");
            lastOffset = number.longValue();
          }
          this.inputStream = new FileInputStream(this.inputFile);
          configure(this.inputStream, this.metadata, lastOffset);
        } catch (Exception ex) {
          throw new ConnectException(ex);
        }
        processingTime.reset();
        processingTime.start();
      }
      List<SourceRecord> records = process();
      this.hasRecords = !records.isEmpty();
      return records;
    } catch (Exception ex) {
      log.error("Exception encountered processing line {} of {}.", recordOffset(), this.inputFile, ex);

      try {
        closeAndMoveToFinished(this.config.errorPath, true);
      } catch (IOException ex0) {
        log.error("Exception thrown while moving {} to {}", this.inputFile, this.config.errorPath, ex0);
      }
      if (this.config.haltOnError) {
        throw new ConnectException(ex);
      } else {
        return new ArrayList<>();
      }
    }
  }

  protected void addRecord(List<SourceRecord> records, Struct keyStruct, Struct valueStruct) {
    Map<String, ?> sourceOffset = ImmutableMap.of(
        "offset",
        recordOffset()
    );
    log.trace("addRecord() - {}", sourceOffset);
    if (this.config.hasKeyMetadataField && null != keyStruct) {
      keyStruct.put(this.config.keyMetadataField, this.metadata);
    }

    if (this.config.hasvalueMetadataField && null != valueStruct) {
      valueStruct.put(this.config.valueMetadataField, this.metadata);
    }

    final Long timestamp;

    switch (this.config.timestampMode) {
      case FIELD:
        log.trace("addRecord() - Reading date from timestamp field '{}'", this.config.timestampField);
        java.util.Date date = (java.util.Date) valueStruct.get(this.config.timestampField);
        timestamp = date.getTime();
        break;
      case FILE_TIME:
        timestamp = this.inputFileModifiedTime;
        break;
      case PROCESS_TIME:
        timestamp = null;
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported timestamp mode. %s", this.config.timestampMode)
        );
    }


    //TODO: Comeback and add timestamp support.

    SourceRecord sourceRecord = new SourceRecord(
        this.sourcePartition,
        sourceOffset,
        this.config.topic,
        null,
        this.config.keySchema,
        keyStruct,
        this.config.valueSchema,
        valueStruct,
        timestamp
    );
    records.add(sourceRecord);
  }
}