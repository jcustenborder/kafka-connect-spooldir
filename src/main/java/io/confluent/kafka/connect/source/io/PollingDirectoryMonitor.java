package io.confluent.kafka.connect.source.io;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
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

public class PollingDirectoryMonitor implements DirectoryMonitor {
  private static final Logger log = LoggerFactory.getLogger(PollingDirectoryMonitor.class);
  File inputDirectory;
  File finishedDirectory;
  File errorDirectory;

  Map<?, ?> configValues;
  PollingDirectoryMonitorConfig config;
  RecordProcessor recordProcessor;
  FilenameFilter inputPatternFilter;
  File inputFile;
  String inputFileName;
  InputStream inputStream;
  boolean hasRecords = false;

  static void checkDirectory(String key, File directoryPath) {
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
      temporaryFile = File.createTempFile(".permissings", ".testing", directoryPath);
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
  public void configure(Map<?, ?> configValues) {
    this.configValues = configValues;
    this.config = new PollingDirectoryMonitorConfig(this.configValues);

    this.inputDirectory = this.config.inputPath();
    checkDirectory(PollingDirectoryMonitorConfig.INPUT_PATH_CONFIG, this.inputDirectory);
    this.finishedDirectory = this.config.finishedPath();
    checkDirectory(PollingDirectoryMonitorConfig.FINISHED_PATH_CONFIG, this.finishedDirectory);
    this.errorDirectory = this.config.errorPath();
    checkDirectory(PollingDirectoryMonitorConfig.ERROR_PATH_CONFIG, this.errorDirectory);

    try {
      this.recordProcessor = this.config.recordProcessor().newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ConnectException("Exception thrown while creating record processor", e);
    }
  }

  void closeAndMoveToFinished(File outputDirectory) throws IOException {
    if (null != inputStream) {
      if (log.isInfoEnabled()) {
        log.info("Closing {}", this.inputFile);
      }
      this.inputStream.close();
      this.inputStream = null;

      File finishedFile = new File(outputDirectory, this.inputFile.getName());

      if (log.isInfoEnabled()) {
        log.info("Finished processing {} moving to {}.", this.inputFile);
      }

      Files.move(this.inputFile, finishedFile);
    }
  }

  @Override
  public List<SourceRecord> poll() {
    try {
      if (!hasRecords) {
        closeAndMoveToFinished(this.finishedDirectory);

        File[] files = this.inputDirectory.listFiles(this.inputPatternFilter);
        if (null == files || files.length == 0) {
          return new ArrayList<>();
        }
        this.inputFile = files[0];
        this.inputFileName = Files.getNameWithoutExtension(this.inputFile.getName());
        try {
          if (log.isInfoEnabled()) {
            log.info("Opening {}", this.inputFile);
          }
          this.inputStream = new FileInputStream(this.inputFile);
          this.recordProcessor.configure(this.configValues, this.inputStream, this.inputFileName);
        } catch (Exception ex) {
          throw new ConnectException(ex);
        }
      }
      List<SourceRecord> records = this.recordProcessor.poll();
      this.hasRecords = !records.isEmpty();
      return records;
    } catch (IOException ex) {
      try {
        closeAndMoveToFinished(this.errorDirectory);
      } catch (IOException ex0) {
        if (log.isErrorEnabled()) {
          log.error("Exception thrown while moving {} to {}", this.inputFile, this.errorDirectory, ex0);
        }
      }
      throw new ConnectException(ex);
    }
  }
}
