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

import com.google.common.io.ByteStreams;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class SpoolDirJsonSourceConnectorTest extends AbstractSpoolDirSourceConnectorTest<SpoolDirJsonSourceConnector> {
  @Override
  protected SpoolDirJsonSourceConnector createConnector() {
    return new SpoolDirJsonSourceConnector();
  }

  @Test
  public void startWithoutSchema() throws IOException {
    settings.put(AbstractSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, "^.*\\.json$");

    String[] inputFiles = new String[]{
        "json/FieldsMatch.data",
        "json/FieldsMatch.data",
    };

    int index = 0;
    for (String inputFile : inputFiles) {
      try (InputStream inputStream = this.getClass().getResourceAsStream(inputFile)) {
        File outputFile = new File(this.inputPath, "input" + index + ".json");
        try (OutputStream outputStream = new FileOutputStream(outputFile)) {
          ByteStreams.copy(inputStream, outputStream);
        }
      }
      index++;
    }

    this.connector.start(settings);
  }

  @Test()
  public void startWithoutSchemaMismatch() throws IOException {
    this.settings.put(AbstractSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, "^.*\\.json$");


    String[] inputFiles = new String[]{
        "json/FieldsMatch.data",
        "json/DataHasMoreFields.data",
    };

    int index = 0;
    for (String inputFile : inputFiles) {
      try (InputStream inputStream = this.getClass().getResourceAsStream(inputFile)) {
        File outputFile = new File(this.inputPath, "input" + index + ".json");
        try (OutputStream outputStream = new FileOutputStream(outputFile)) {
          ByteStreams.copy(inputStream, outputStream);
        }
      }
      index++;
    }

    assertThrows(DataException.class, () -> {
      this.connector.start(settings);
    });

  }
}
