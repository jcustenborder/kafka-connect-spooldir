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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SpoolDirBinaryFileSourceTask extends AbstractSourceTask<SpoolDirBinaryFileSourceConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(SpoolDirBinaryFileSourceTask.class);

  @Override
  protected SpoolDirBinaryFileSourceConnectorConfig config(Map<String, ?> settings) {
    return new SpoolDirBinaryFileSourceConnectorConfig(settings);
  }

  @Override
  protected void configure(InputFile inputFile, Long lastOffset) throws IOException {
    inputFile.openStream();
  }

  @Override
  protected List<SourceRecord> process() throws IOException {
    List<SourceRecord> records = new ArrayList<>(1);

    if (this.inputFile.inputStream().available() > 0) {
      byte[] buffer = ByteStreams.toByteArray(this.inputFile.inputStream());
      records.add(
          record(
              null,
              new SchemaAndValue(Schema.BYTES_SCHEMA, buffer),
              null
          )
      );
    }
    return records;
  }

  @Override
  protected long recordOffset() {
    return 0;
  }
}
