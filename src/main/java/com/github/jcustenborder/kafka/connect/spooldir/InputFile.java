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

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

class InputFile implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(InputFile.class);
  public final File inputFile;
  public final File processingFlag;

  InputFile(File inputFile, File processingFlag) {
    this.inputFile = inputFile;
    this.processingFlag = processingFlag;
  }

  static final Map<String, String> SUPPORTED_COMPRESSION_TYPES = ImmutableMap.of(
      "bz2", CompressorStreamFactory.BZIP2,
      "gz", CompressorStreamFactory.GZIP,
      "snappy", CompressorStreamFactory.SNAPPY_RAW,
      "lz4", CompressorStreamFactory.LZ4_BLOCK,
      "z", CompressorStreamFactory.Z
  );

  public InputStream inputStream;

  public InputStream openStream() throws IOException {
    if (null != this.inputStream) {
      throw new IOException(
          String.format("File %s is already open", this.inputFile)
      );
    }

    final String extension = Files.getFileExtension(inputFile.getName());
    log.trace("read() - fileName = '{}' extension = '{}'", inputFile, extension);
    final InputStream fileInputStream = new FileInputStream(this.inputFile);

    if (SUPPORTED_COMPRESSION_TYPES.containsKey(extension)) {
      final String compressor = SUPPORTED_COMPRESSION_TYPES.get(extension);
      log.info("Decompressing {} as {}", inputFile, compressor);
      final CompressorStreamFactory compressorStreamFactory = new CompressorStreamFactory();
      try {
        inputStream = compressorStreamFactory.createCompressorInputStream(compressor, fileInputStream);
      } catch (CompressorException e) {
        throw new IOException("Exception thrown while creating compressor stream " + compressor, e);
      }
    } else {
      inputStream = fileInputStream;
    }

    log.info("Creating processing flag {}", this.processingFlag);
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
}
