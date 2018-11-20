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

import com.google.common.io.Files;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public abstract class SpoolDirSourceConnectorTest<T extends SpoolDirSourceConnector> {
  private static final Logger log = LoggerFactory.getLogger(SpoolDirSourceConnectorTest.class);
  protected T connector;
  protected Map<String, String> settings;
  File tempRoot;
  File inputPath;
  File finishedPath;
  File errorPath;

  protected abstract T createConnector();

  @BeforeEach
  public void before() {
    this.connector = createConnector();
  }

  @Test
  public void taskClass() {
    assertNotNull(this.connector.taskClass());
  }

  @BeforeEach
  public void createTempDir() {
    this.tempRoot = Files.createTempDir();
    this.inputPath = new File(this.tempRoot, "input");
    this.inputPath.mkdirs();
    this.finishedPath = new File(this.tempRoot, "finished");
    this.finishedPath.mkdirs();
    this.errorPath = new File(this.tempRoot, "error");
    this.errorPath.mkdirs();

    this.settings = new LinkedHashMap<>();
    this.settings.put(AbstractSourceConnectorConfig.INPUT_PATH_CONFIG, this.inputPath.getAbsolutePath());
    this.settings.put(AbstractSourceConnectorConfig.FINISHED_PATH_CONFIG, this.finishedPath.getAbsolutePath());
    this.settings.put(AbstractSourceConnectorConfig.ERROR_PATH_CONFIG, this.errorPath.getAbsolutePath());
    this.settings.put(AbstractSourceConnectorConfig.TOPIC_CONF, "dummy");
    this.settings.put(SpoolDirSourceConnectorConfig.SCHEMA_GENERATION_ENABLED_CONF, "true");
  }

  @AfterEach
  public void cleanupTempDir() throws IOException {
    java.nio.file.Files.walkFileTree(this.tempRoot.toPath(), new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        log.trace("cleanupTempDir() - Removing {}", file);
        java.nio.file.Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
        log.trace("cleanupTempDir() - Removing {}", file);
        java.nio.file.Files.delete(file);
        return FileVisitResult.CONTINUE;
      }
    });
  }


}
