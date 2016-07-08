package io.confluent.kafka.connect.source.io;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.io.Files;
import io.confluent.kafka.connect.source.SpoolDirectoryConfig;
import io.confluent.kafka.connect.source.io.processing.RecordProcessor;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PollingDirectoryMonitor implements DirectoryMonitor {
  private static final Logger log = LoggerFactory.getLogger(PollingDirectoryMonitor.class);
  Stopwatch processingTime = Stopwatch.createStarted();
  private File inputDirectory;
  private File finishedDirectory;
  private File errorDirectory;
  private Map<?, ?> configValues;
  private SpoolDirectoryConfig config;
  private RecordProcessor recordProcessor;
  private FilenameFilter inputPatternFilter;
  private File inputFile;
  private String inputFileName;
  private InputStream inputStream;
  private boolean hasRecords = false;

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

  @Override
  public void configure(SpoolDirectoryConfig config) {
    this.config = config;

    this.inputDirectory = this.config.inputPath();
    checkDirectory(SpoolDirectoryConfig.INPUT_PATH_CONFIG, this.inputDirectory);
    this.finishedDirectory = this.config.finishedPath();
    checkDirectory(SpoolDirectoryConfig.FINISHED_PATH_CONFIG, this.finishedDirectory);
    this.errorDirectory = this.config.errorPath();
    checkDirectory(SpoolDirectoryConfig.ERROR_PATH_CONFIG, this.errorDirectory);

    try {
      this.recordProcessor = this.config.recordProcessor().newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ConnectException("Exception thrown while creating record processor", e);
    }

    this.inputPatternFilter = config.inputFilePattern();
  }

  private void closeAndMoveToFinished(File outputDirectory, boolean errored) throws IOException {
    if (null != inputStream) {
      if (log.isInfoEnabled()) {
        log.info("Closing {}", this.inputFile);
      }
      this.inputStream.close();
      this.inputStream = null;

      File finishedFile = new File(outputDirectory, this.inputFile.getName());

      if (errored) {
        if (log.isErrorEnabled()) {
          log.error("Error during processing, moving {} to {}.", this.inputFile, outputDirectory);
        }
      } else {
        if (log.isInfoEnabled()) {
          log.info("Finished processing {} in {} second(s). Moving to {}.", this.inputFile, processingTime.elapsed(TimeUnit.SECONDS), outputDirectory);
        }
      }


      Files.move(this.inputFile, finishedFile);
    }
  }

  File findNextInputFile() {
    File[] files = this.inputDirectory.listFiles(this.inputPatternFilter);
    if (null == files || files.length == 0) {
      if (log.isDebugEnabled()) {
        log.debug("No files matching {} were found in {}", SpoolDirectoryConfig.INPUT_FILE_PATTERN_CONF, this.inputDirectory);
      }
      return null;
    }

    File result = null;

    for (File file : files) {
      long fileAgeMS = System.currentTimeMillis() - file.lastModified();

      if (fileAgeMS < 0L) {
        if (log.isWarnEnabled()) {
          log.warn("File {} has a date in the future.", file);
        }
      }

      if (this.config.minimumFileAgeMS() > 0L && fileAgeMS < this.config.minimumFileAgeMS()) {
        if (log.isDebugEnabled()) {
          log.debug("Skipping {} because it does not meet the minimum age.", file);
        }
        continue;
      }
      result = file;
      break;
    }

    return result;
  }

  @Override
  public List<SourceRecord> poll() {
    try {
      if (!hasRecords) {
        closeAndMoveToFinished(this.finishedDirectory, false);

        File nextFile = findNextInputFile();
        if (null == nextFile) {
          return new ArrayList<>();
        }

        this.inputFile = nextFile;
        this.inputFileName = Files.getNameWithoutExtension(this.inputFile.getName());
        try {
          if (log.isInfoEnabled()) {
            log.info("Opening {}", this.inputFile);
          }
          this.inputStream = new FileInputStream(this.inputFile);
          this.recordProcessor.configure(this.config, this.inputStream, this.inputFileName);
        } catch (Exception ex) {
          throw new ConnectException(ex);
        }
        processingTime.reset();
        processingTime.start();
      }
      List<SourceRecord> records = this.recordProcessor.poll();
      this.hasRecords = !records.isEmpty();
      return records;
    } catch (Exception ex) {
      if (log.isErrorEnabled()) {
        log.error("Exception encountered processing line {} of {}.", this.recordProcessor.lineNumber(), this.inputFile, ex);
      }

      try {
        closeAndMoveToFinished(this.errorDirectory, true);
      } catch (IOException ex0) {

        if (log.isErrorEnabled()) {
          log.error("Exception thrown while moving {} to {}", this.inputFile, this.errorDirectory, ex0);
        }
      }
      if (this.config.haltOnError()) {
        throw new ConnectException(ex);
      } else {

        return new ArrayList<>();
      }
    }
  }
}
