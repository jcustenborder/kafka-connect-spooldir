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
import java.util.Map;

class InputFile implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(InputFile.class);
  private final File inputFile;
  private final File processingFlag;
  private final String name;
  private final String path;
  private final long length;
  private final long lastModified;
  private final int bufferSize;
  private final Metadata metadata;

  InputFile(File inputFile, File processingFlag, int bufferSize) {
    this.inputFile = inputFile;
    this.bufferSize = bufferSize;
    this.name = this.inputFile.getName();
    this.path = this.inputFile.getPath();
    this.lastModified = this.inputFile.lastModified();
    this.length = this.inputFile.length();
    this.processingFlag = processingFlag;
    this.metadata = new Metadata(inputFile);
  }

  static final Map<String, String> SUPPORTED_COMPRESSION_TYPES = ImmutableMap.of(
      "bz2", CompressorStreamFactory.BZIP2,
      "gz", CompressorStreamFactory.GZIP,
      "snappy", CompressorStreamFactory.SNAPPY_RAW,
      "lz4", CompressorStreamFactory.LZ4_BLOCK,
      "z", CompressorStreamFactory.Z
  );

  public Metadata metadata() {
    return this.metadata;
  }

  private InputStream inputStream;

  public InputStream inputStream() {
    return this.inputStream;
  }

  public InputStream openStream(boolean buffered) throws IOException {
    if (null != this.inputStream) {
      throw new IOException(
          String.format("File %s is already open", this.inputFile)
      );
    }

    final String extension = Files.getFileExtension(inputFile.getName());
    log.trace("openStream() - fileName = '{}' extension = '{}'", inputFile, extension); // TODO [mpb] this is logged...
    this.inputStream = new FileInputStream(this.inputFile);

    if (buffered) {
      log.trace(
          "openStream() - Wrapping '{}' in a BufferedInputStream with bufferSize = {}",  // TODO [mpb] this is logged...
          this.inputFile,
          this.bufferSize
      );
      this.inputStream = new BufferedInputStream(this.inputStream, this.bufferSize);
    }

    if (SUPPORTED_COMPRESSION_TYPES.containsKey(extension)) {
      final String compressor = SUPPORTED_COMPRESSION_TYPES.get(extension);
      log.info("Decompressing {} as {}", inputFile, compressor);
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

    log.info("Creating processing flag {}", this.processingFlag); // TODO [mpb] this is logged...
    Files.touch(this.processingFlag);

    return inputStream;
  }

  @Override
  public String toString() {
    return this.inputFile.toString();
  }

  @Override
  public void close() throws IOException {
    if (null != this.inputStream) {
      log.info("Closing {}", this.inputFile);
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

  public void moveToDirectory(File outputDirectory) {
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

  public void delete() {
    log.info("Deleting {}", this.inputFile);
    if (!this.inputFile.delete()) {
      log.warn("Could not delete {}", this.inputFile);
    }
  }

  public boolean exists() {
    return this.inputFile.exists();
  }
}
