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
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.kafka.connect.data.SchemaAndValue;
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

public abstract class AbstractSourceTask<CONF extends AbstractSourceConnectorConfig> extends SourceTask {
  private static final Logger log = LoggerFactory.getLogger(AbstractSourceTask.class);
  protected Map<String, ?> sourcePartition;
  protected CONF config;
  private Stopwatch processingTime = Stopwatch.createStarted();
  private File inputFile;
  protected long inputFileModifiedTime;
  private InputStream inputStream;
  private boolean hasRecords = false;
  protected Map<String, String> metadata;

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
  public void start(Map<String, String> settings) {
    this.config = config(settings);

    checkDirectory(AbstractSourceConnectorConfig.INPUT_PATH_CONFIG, this.config.inputPath);
    checkDirectory(AbstractSourceConnectorConfig.ERROR_PATH_CONFIG, this.config.errorPath);

    if (AbstractSourceConnectorConfig.CleanupPolicy.MOVE == this.config.cleanupPolicy) {
      checkDirectory(AbstractSourceConnectorConfig.FINISHED_PATH_CONFIG, this.config.finishedPath);
    }

    this.inputFileDequeue = new InputFileDequeue(this.config);
  }

  @Override
  public void stop() {

  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  InputFileDequeue inputFileDequeue;
  int emptyCount = 0;
  long recordCount;

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
      return null;
    }
    emptyCount = 0;
    log.trace("read() returning {} result(s)", results.size());

    return results;
  }

  private void recordProcessingTime() {
    log.info(
        "Finished processing {} record(s) in {} second(s).",
        this.recordCount,
        processingTime.elapsed(TimeUnit.SECONDS)
    );
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
        recordProcessingTime();
        log.info(
            "Moving to {} to {}.",
            this.inputFile,
            outputDirectory
        );
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
          this.recordCount = 0;
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
      recordProcessingTime();
      log.info("Removing file {}", this.inputFile);
      this.inputFile.delete();
      File processingFile = InputFileDequeue.processingFile(this.config.processingFileExtension, this.inputFile);
      if (processingFile.exists()) {
        log.info("Removing processing file {}", processingFile);
        processingFile.delete();
      }

    }
  }

  protected Map<String, ?> offset() {
    return ImmutableMap.of(
        "offset",
        recordOffset()
    );
  }

  protected SourceRecord record(
      SchemaAndValue key,
      SchemaAndValue value,
      Long timestamp) {
    Map<String, ?> sourceOffset = ImmutableMap.of(
        "offset",
        recordOffset()
    );

    return new SourceRecord(
        this.sourcePartition,
        sourceOffset,
        this.config.topic,
        null,
        null != key ? key.schema() : null,
        null != key ? key.value() : null,
        value.schema(),
        value.value(),
        timestamp
    );
  }


}
