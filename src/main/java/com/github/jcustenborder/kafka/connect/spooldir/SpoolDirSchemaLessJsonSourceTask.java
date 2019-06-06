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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SpoolDirSchemaLessJsonSourceTask extends AbstractSourceTask<SpoolDirSchemaLessJsonSourceConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(SpoolDirSchemaLessJsonSourceTask.class);

  @Override
  protected SpoolDirSchemaLessJsonSourceConnectorConfig config(Map<String, ?> settings) {
    return new SpoolDirSchemaLessJsonSourceConnectorConfig(settings);
  }

  JsonParser parser;
  MappingIterator<JsonNode> nodeIterator;

  long recordOffset;

  @Override
  protected void configure(InputStream inputStream, Long lastOffset) throws IOException {
    if (null != this.parser) {
      this.parser.close();
    }
    this.recordOffset = 0;
    this.parser = ObjectMapperFactory.INSTANCE.getJsonFactory().createParser(inputStream);
    this.nodeIterator = ObjectMapperFactory.INSTANCE.readValues(this.parser, JsonNode.class);

  }

  @Override
  protected List<SourceRecord> process() throws IOException {
    int recordCount = 0;
    List<SourceRecord> records = new ArrayList<>(this.config.batchSize);
    while (recordCount < this.config.batchSize && this.nodeIterator.hasNext()) {
      JsonNode node = this.nodeIterator.next();
      String value = ObjectMapperFactory.INSTANCE.writeValueAsString(node);
      SourceRecord record = record(
          null,
          new SchemaAndValue(Schema.STRING_SCHEMA, value),
          null
      );
      records.add(record);
      recordCount++;
      recordOffset++;
    }
    return records;
  }

  @Override
  protected long recordOffset() {
    return this.recordOffset;
  }
}
