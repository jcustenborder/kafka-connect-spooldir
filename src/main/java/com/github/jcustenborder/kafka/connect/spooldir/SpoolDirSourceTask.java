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

import com.github.jcustenborder.kafka.connect.utils.data.Parser;
import com.github.jcustenborder.kafka.connect.utils.data.type.DateTypeParser;
import com.github.jcustenborder.kafka.connect.utils.data.type.TimeTypeParser;
import com.github.jcustenborder.kafka.connect.utils.data.type.TimestampTypeParser;
import com.github.jcustenborder.kafka.connect.utils.data.type.TypeParser;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public abstract class SpoolDirSourceTask<CONF extends SpoolDirSourceConnectorConfig> extends AbstractSourceTask<CONF> {
  static final Logger log = LoggerFactory.getLogger(SpoolDirSourceTask.class);
  protected Parser parser;

  @Override
  public void start(Map<String, String> settings) {
    super.start(settings);

    this.parser = new Parser();
    Map<Schema, TypeParser> dateTypeParsers = ImmutableMap.of(
        Timestamp.SCHEMA, new TimestampTypeParser(this.config.parserTimestampTimezone, this.config.parserTimestampDateFormats),
        Date.SCHEMA, new DateTypeParser(this.config.parserTimestampTimezone, this.config.parserTimestampDateFormats),
        Time.SCHEMA, new TimeTypeParser(this.config.parserTimestampTimezone, this.config.parserTimestampDateFormats)
    );

    for (Map.Entry<Schema, TypeParser> kvp : dateTypeParsers.entrySet()) {
      this.parser.registerTypeParser(kvp.getKey(), kvp.getValue());
    }
  }

  protected void addRecord(List<SourceRecord> records, SchemaAndValue key, SchemaAndValue value) {

    if (this.config.hasKeyMetadataField && !SchemaAndValue.NULL.equals(key)) {
      final Struct keyStruct = (Struct) key.value();
      keyStruct.put(this.config.keyMetadataField, this.metadata);
    }

    final Struct valueStruct;
    if (this.config.hasvalueMetadataField && !SchemaAndValue.NULL.equals(value)) {
      valueStruct = (Struct) value.value();
      valueStruct.put(this.config.valueMetadataField, this.metadata);
    } else {
      valueStruct = null;
    }

    Long timestamp;

    switch (this.config.timestampMode) {
      case FIELD:
        log.trace("addRecord() - Reading date from timestamp field '{}'", this.config.timestampField);
        final java.util.Date date = (java.util.Date) valueStruct.get(this.config.timestampField);
        timestamp = date.getTime();
        break;
      case FILE_TIME:
        timestamp = this.inputFileModifiedTime;
        break;
      case PROCESS_TIME:
        timestamp = null;
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported timestamp mode. %s", this.config.timestampMode)
        );
    }

    //TODO: Comeback and add timestamp support.

    SourceRecord sourceRecord = record(
        key,
        value,
        timestamp
    );
    recordCount++;
    records.add(sourceRecord);
  }

//  @Override
//  protected void addRecord(List<SourceRecord> records, Struct keyStruct, Struct valueStruct) {
//
//    super.addRecord(records, keyStruct, valueStruct);
//  }
}