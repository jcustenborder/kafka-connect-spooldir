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
import org.apache.commons.compress.compressors.CompressorStreamFactory;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class SpoolDirSourceTask<CONF extends SpoolDirSourceConnectorConfig> extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(SpoolDirSourceTask.class);
  protected Parser parser;
  protected Map<String, ?> sourcePartition;
  protected CONF config;
  private Stopwatch processingTime = Stopwatch.createStarted();
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

  InputFileDequeue inputFileDequeue;

  @Override
  public void start(Map<String, String> settings) {
    this.config = config(settings);

    checkDirectory(SpoolDirSourceConnectorConfig.INPUT_PATH_CONFIG, this.config.inputPath);
    checkDirectory(SpoolDirSourceConnectorConfig.ERROR_PATH_CONFIG, this.config.errorPath);

    if (SpoolDirSourceConnectorConfig.CleanupPolicy.MOVE == this.config.cleanupPolicy) {
      checkDirectory(SpoolDirSourceConnectorConfig.FINISHED_PATH_CONFIG, this.config.finishedPath);
    }

    this.parser = new Parser();
    Map<Schema, TypeParser> dateTypeParsers = ImmutableMap.of(
        Timestamp.SCHEMA, new TimestampTypeParser(this.config.parserTimestampTimezone, this.config.parserTimestampDateFormats),
        Date.SCHEMA, new DateTypeParser(this.config.parserTimestampTimezone, this.config.parserTimestampDateFormats),
        Time.SCHEMA, new TimeTypeParser(this.config.parserTimestampTimezone, this.config.parserTimestampDateFormats)
    );

    for (Map.Entry<Schema, TypeParser> kvp : dateTypeParsers.entrySet()) {
      this.parser.registerTypeParser(kvp.getKey(), kvp.getValue());
    }

    this.inputFileDequeue = new InputFileDequeue(this.config);
  }

  @Override
  public void stop() {

  }

  int emptyCount = 0;

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    log.trace("poll()");
    List<SourceRecord> results = read();

    if (results.isEmpty()) {
      emptyCount++;
      if (emptyCount > 1) {
        log.trace("read() returned empty list. Sleeping {} ms.", this.config.emptyPollWaitMs);
        Thread.sleep(this.config.emptyPollWaitMs);
      }
      return results;
    }
    emptyCount = 0;
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
      } else {
        log.info("Finished processing {} in {} second(s). Moving to {}.", this.inputFile, processingTime.elapsed(TimeUnit.SECONDS), outputDirectory);
      }

      Files.move(this.inputFile, finishedFile);

      File processingFile = InputFileDequeue.processingFile(this.config.processingFileExtension, this.inputFile);
      if (processingFile.exists()) {
        log.info("Removing processing file {}", processingFile);
        processingFile.delete();
      }

    }
  }

  static final Map<String, String> SUPPORTED_COMPRESSION_TYPES = ImmutableMap.of(
      "bz2", CompressorStreamFactory.BZIP2,
      "gz", CompressorStreamFactory.GZIP,
      "snappy", CompressorStreamFactory.SNAPPY_RAW,
      "lz4", CompressorStreamFactory.LZ4_BLOCK,
      "z", CompressorStreamFactory.Z
  );

  public List<SourceRecord> read() {
    try {
      if (!hasRecords) {
        switch (this.config.cleanupPolicy) {
          case MOVE:
            closeAndMoveToFinished(this.config.finishedPath, false);
            break;
          case DELETE:
            closeAndDelete();
            break;
        }

        File nextFile = this.inputFileDequeue.poll();
        if (null == nextFile) {
          return new ArrayList<>();
        }

        this.metadata = ImmutableMap.of();
        this.inputFile = nextFile;
        this.inputFileModifiedTime = this.inputFile.lastModified();
        File processingFile = InputFileDequeue.processingFile(this.config.processingFileExtension, this.inputFile);
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

          final String extension = Files.getFileExtension(inputFile.getName());
          log.trace("read() - fileName = '{}' extension = '{}'", inputFile, extension);
          final InputStream inputStream = new FileInputStream(this.inputFile);

          if (SUPPORTED_COMPRESSION_TYPES.containsKey(extension)) {
            final String compressor = SUPPORTED_COMPRESSION_TYPES.get(extension);
            log.info("Decompressing {} as {}", inputFile, compressor);
            final CompressorStreamFactory compressorStreamFactory = new CompressorStreamFactory();
            this.inputStream = compressorStreamFactory.createCompressorInputStream(compressor, inputStream);
          } else {
            this.inputStream = inputStream;
          }
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

  private void closeAndDelete() throws IOException {
    if (null != inputStream) {
      log.info("Closing {}", this.inputFile);
      this.inputStream.close();
      this.inputStream = null;
      log.info("Removing file {}", this.inputFile);
      this.inputFile.delete();
      File processingFile = InputFileDequeue.processingFile(this.config.processingFileExtension, this.inputFile);
      if (processingFile.exists()) {
        log.info("Removing processing file {}", processingFile);
        processingFile.delete();
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
        null != keyStruct ? keyStruct.schema() : null,
        keyStruct,
        valueStruct.schema(),
        valueStruct,
        timestamp
    );
    records.add(sourceRecord);
  }
}