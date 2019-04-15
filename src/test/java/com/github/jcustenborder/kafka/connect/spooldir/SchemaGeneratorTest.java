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
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

public class SchemaGeneratorTest {
  protected Map<String, String> settings;
  File tempRoot;
  File inputPath;
  File finishedPath;
  File errorPath;

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
    this.settings.put(AbstractSourceConnectorConfig.KAFKA_TOPIC_CONF, "dummy");
  }

}
