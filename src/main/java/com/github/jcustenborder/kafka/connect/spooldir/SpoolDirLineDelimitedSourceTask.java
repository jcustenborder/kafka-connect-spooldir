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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SpoolDirLineDelimitedSourceTask extends AbstractSourceTask<SpoolDirLineDelimitedSourceConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(SpoolDirLineDelimitedSourceTask.class);

  @Override
  protected SpoolDirLineDelimitedSourceConnectorConfig config(Map<String, ?> settings) {
    return new SpoolDirLineDelimitedSourceConnectorConfig(settings);
  }

  @Override
  protected void configure(InputFile inputFile, Long lastOffset) throws IOException {
    this.inputFile.openLineNumberReader(this.config.charset);
  }

  @Override
  protected List<SourceRecord> process() throws IOException {
    int recordCount = 0;
    List<SourceRecord> records = new ArrayList<>(this.config.batchSize);
    String line = null;
    while (recordCount < this.config.batchSize && null != (line = this.inputFile.lineNumberReader().readLine())) {
      SourceRecord record = record(
          null,
          new SchemaAndValue(Schema.STRING_SCHEMA, line),
          null
      );
      records.add(record);
      recordCount++;
    }
    return records;
  }

  @Override
  protected long recordOffset() {
    long result = -1L;

    if (null != this.inputFile && null != this.inputFile.lineNumberReader()) {
      result = this.inputFile.lineNumberReader().getLineNumber();
    }

    return result;
  }
}
