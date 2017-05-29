/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.spooldir;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SpoolDirJsonSourceTask extends SpoolDirSourceTask<SpoolDirJsonSourceConnectorConfig> {
  JsonFactory jsonFactory;
  JsonParser jsonParser;
  Iterator<JsonNode> iterator;
  long offset;

  @Override
  protected SpoolDirJsonSourceConnectorConfig config(Map<String, ?> settings) {
    return new SpoolDirJsonSourceConnectorConfig(true, settings);
  }

  @Override
  public void start(Map<String, String> settings) {
    super.start(settings);
    this.jsonFactory = new JsonFactory();
  }

  @Override
  protected void configure(InputStream inputStream, Map<String, String> metadata, Long lastOffset) throws IOException {
    if (null != jsonParser) {
      log.trace("configure() - Closing existing json parser.");
      jsonParser.close();
    }

    this.jsonParser = this.jsonFactory.createParser(inputStream);
    this.iterator = ObjectMapperFactory.INSTANCE.readValues(this.jsonParser, JsonNode.class);
    this.offset = -1;

    if (null != lastOffset) {
      int skippedRecords = 0;
      while (this.iterator.hasNext() && skippedRecords < lastOffset) {
        next();
        skippedRecords++;
      }
      log.trace("configure() - Skipped {} record(s).", skippedRecords);
      log.info("configure() - Starting on offset {}", this.offset);
    }

  }

  JsonNode next() {
    this.offset++;
    return this.iterator.next();
  }

  @Override
  protected List<SourceRecord> process() throws IOException {
    List<SourceRecord> records = new ArrayList<>(this.config.batchSize);

    while (this.iterator.hasNext() && records.size() < this.config.batchSize) {
      JsonNode node = next();

      Struct valueStruct = new Struct(this.config.valueSchema);
      Struct keyStruct = new Struct(this.config.keySchema);
      log.trace("process() - input = {}", node);
      for (Field field : this.config.valueSchema.fields()) {
        JsonNode fieldNode = node.get(field.name());
        log.trace("process() - field: {} input = '{}'", field.name(), fieldNode);
        Object fieldValue;
        try {
          fieldValue = this.parser.parseJsonNode(field.schema(), fieldNode);
          log.trace("process() - field: {} output = '{}'", field.name(), fieldValue);
          valueStruct.put(field, fieldValue);

          Field keyField = this.config.keySchema.field(field.name());
          if (null != keyField) {
            log.trace("process() - Setting key field '{}' to '{}'", keyField.name(), fieldValue);
            keyStruct.put(keyField, fieldValue);
          }
        } catch (Exception ex) {
          String message = String.format("Exception thrown while parsing data for '%s'. linenumber=%s", field.name(), this.recordOffset());
          throw new DataException(message, ex);
        }
      }

      addRecord(records, keyStruct, valueStruct);
    }

    return records;
  }

  @Override
  protected long recordOffset() {
    return this.offset;
  }
}
