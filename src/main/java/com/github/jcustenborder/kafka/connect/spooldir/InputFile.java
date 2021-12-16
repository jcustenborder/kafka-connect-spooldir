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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InputFile implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(InputFile.class);
  private final File file;
  private final File processingFlag;
  private final String name;
  private final String path;
  private final long length;
  private final long lastModified;
  private final Metadata metadata;
  private final AbstractSourceConnectorConfig config;
  private final String inputPathSubDir;
  InputStreamReader inputStreamReader;
  LineNumberReader lineNumberReader;
  InputStream inputStream;

  InputFile(AbstractSourceConnectorConfig config, File file) {
    this.config = config;
    this.file = file;
    this.name = this.file.getName();
    this.path = this.file.getPath();
    this.lastModified = this.file.lastModified();
    this.length = this.file.length();
    String processingFileName = file.getName() + config.processingFileExtension;
    this.processingFlag = new File(file.getParentFile(), processingFileName);
    this.inputPathSubDir = determineRelativePath(file, config.inputPath);
    this.metadata = new Metadata(file, this.inputPathSubDir);
  }

  static final Map<String, String> SUPPORTED_COMPRESSION_TYPES = ImmutableMap.of(
      "bz2", CompressorStreamFactory.BZIP2,
      "gz", CompressorStreamFactory.GZIP,
      "snappy", CompressorStreamFactory.SNAPPY_RAW,
      "lz4", CompressorStreamFactory.LZ4_BLOCK,
      "z", CompressorStreamFactory.Z
  );


  private static String determineRelativePath(File inputPath, File inputFile) {
    Path relative = inputFile.toPath().relativize(inputPath.toPath()); // inputPath.toPath().relativize(inputFile.getParentFile().toPath());
    String subDir = relative.toString();
    if ("".equals(subDir)) {
      return null;
    }
    return subDir;
  }

  public String inputPathSubDir() {
    return this.inputPathSubDir;
  }

  public File file() {
    return this.file;
  }

  public File processingFlag() {
    return this.processingFlag;
  }

  public Metadata metadata() {
    return this.metadata;
  }


  public InputStream inputStream() {
    return this.inputStream;
  }

  public InputStream openStream() throws IOException {
    if (null != this.inputStream) {
      throw new IOException(
          String.format("File %s is already open", this.file)
      );
    }

    final String extension = Files.getFileExtension(file.getName());
    log.trace("openStream() - fileName = '{}' extension = '{}'", file, extension);
    this.inputStream = new FileInputStream(this.file);

    if (this.config.bufferedInputStream) {
      log.trace(
          "openStream() - Wrapping '{}' in a BufferedInputStream with bufferSize = {}",
          this.file,
          this.config.fileBufferSizeBytes
      );
      this.inputStream = new BufferedInputStream(this.inputStream, this.config.fileBufferSizeBytes);
    }

    if (SUPPORTED_COMPRESSION_TYPES.containsKey(extension)) {
      final String compressor = SUPPORTED_COMPRESSION_TYPES.get(extension);
      log.info("Decompressing {} as {}", file, compressor);
      final CompressorStreamFactory compressorStreamFactory = new CompressorStreamFactory();
      try {
        this.inputStream = compressorStreamFactory.createCompressorInputStream(
            compressor,
            this.inputStream
        );
      } catch (CompressorException e) {
        throw new IOException("Exception thrown while creating compressor stream " + compressor, e);
      }
    }

    startProcessing();

    return inputStream;
  }

  public void startProcessing() throws IOException {
    log.info("Creating processing flag {}", this.processingFlag);
    Files.touch(this.processingFlag);
  }

  public InputStreamReader openInputStreamReader(Charset charset) throws IOException {
    if (null == this.inputStreamReader) {
      InputStream inputStream = null != this.inputStream ? this.inputStream : openStream();
      this.inputStreamReader = new InputStreamReader(inputStream, charset);
    }

    return this.inputStreamReader;
  }

  public InputStreamReader inputStreamReader() {
    return this.inputStreamReader;
  }

  public LineNumberReader openLineNumberReader(Charset charset) throws IOException {
    if (null == this.lineNumberReader) {
      InputStreamReader inputStreamReader = this.inputStreamReader != null ?
          this.inputStreamReader : openInputStreamReader(charset);
      this.lineNumberReader = new LineNumberReader(inputStreamReader);
    }
    return this.lineNumberReader;
  }

  public LineNumberReader lineNumberReader() {
    return this.lineNumberReader;
  }


  @Override
  public String toString() {
    return this.file.toString();
  }

  @Override
  public void close() throws IOException {
    if (null != this.lineNumberReader) {
      this.lineNumberReader.close();
    }
    if (null != this.inputStreamReader) {
      this.inputStreamReader.close();
    }
    if (null != this.inputStream) {
      log.info("Closing {}", this.file);
      this.inputStream.close();
    }
    if (this.processingFlag.exists()) {
      log.info("Removing processing flag {}", this.processingFlag);
      if (!this.processingFlag.delete()) {
        log.warn("Could not remove processing flag {}", this.processingFlag);
      }
    }
  }

  public String getName() {
    return name;
  }

  public String getPath() {
    return path;
  }

  public long length() {
    return this.length;
  }

  public long lastModified() {
    return this.lastModified;
  }

  private List<File> getInputPathSubDirsToCleanup() {
    List<File> inputPathSubDirsToCleanup = null;
    if (this.inputPathSubDir != null && !config.inputPathWalkRecursivelyRetainSubDirs) {
      inputPathSubDirsToCleanup = new ArrayList<File>();
      File lastSubDir = this.config.inputPath;
      for (String subDirName : this.inputPathSubDir.split(File.separator)) {
        lastSubDir = new File(lastSubDir, subDirName);
        inputPathSubDirsToCleanup.add(lastSubDir);
      }
      Collections.reverse(inputPathSubDirsToCleanup);
    }
    return inputPathSubDirsToCleanup;
  }

  private void cleanupInputDirSubDirs() {
    List<File> inputPathSubDirsToCleanup = this.getInputPathSubDirsToCleanup();
    if (inputPathSubDirsToCleanup != null) {
      for (File subDir : inputPathSubDirsToCleanup) {
        try {
          if (subDir.listFiles() == null || subDir.listFiles().length == 0) {
            if (!subDir.delete()) {
              log.error("Failed to delete input.path sub-directory: {}", subDir);
            } else {
              log.info("Cleaned up input.path sub-directory: {}", subDir);
            }
          } else {
            log.info("Cannot clean up input.path sub-directory as it is not empty: {}", subDir);
          }
        } catch (SecurityException e) {
          log.error("SecurityException thrown while trying to delete input.path sub-directory: {}", subDir, e);
        }
      }
    }
  }


  public void moveToDirectory(File outputDirectory) {

    if (this.inputPathSubDir != null) {
      outputDirectory = new File(outputDirectory, this.inputPathSubDir);
      if (!outputDirectory.isDirectory()) {
        outputDirectory.mkdirs();
      }
    }

    File outputFile = new File(outputDirectory, this.file.getName());
    try {
      if (this.file.exists()) {
        log.info("Moving {} to {}", this.file, outputFile);
        Files.move(this.file, outputFile);
      }
    } catch (IOException e) {
      log.error("Exception thrown while trying to move {} to {}", this.file, outputFile, e);
    }

    this.cleanupInputDirSubDirs();

  }

  public void delete() {
    log.info("Deleting {}", this.file);
    if (!this.file.delete()) {
      log.warn("Could not delete {}", this.file);
    }

    this.cleanupInputDirSubDirs();
  }

  public boolean exists() {
    return this.file.exists();
  }
}
