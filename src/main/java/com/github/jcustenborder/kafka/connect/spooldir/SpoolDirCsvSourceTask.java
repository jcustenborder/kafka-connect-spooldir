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

import com.google.common.base.Joiner;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.ICSVParser;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SpoolDirCsvSourceTask extends AbstractSpoolDirSourceTask<SpoolDirCsvSourceConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(SpoolDirCsvSourceTask.class);
  String[] fieldNames;
  private ICSVParser csvParser;
  private CSVReader csvReader;
  private InputStreamReader streamReader;

  @Override
  protected SpoolDirCsvSourceConnectorConfig config(Map<String, ?> settings) {
    return new SpoolDirCsvSourceConnectorConfig(true, settings);
  }

  @Override
  protected void configure(InputStream inputStream, final Long lastOffset) throws IOException {
    log.trace("configure() - creating csvParser");
    this.csvParser = this.config.createCSVParserBuilder();
    this.streamReader = new InputStreamReader(inputStream, this.config.charset);
    CSVReaderBuilder csvReaderBuilder = this.config.createCSVReaderBuilder(this.streamReader, csvParser);
    this.csvReader = csvReaderBuilder.build();

    String[] fieldNames;

    if (this.config.firstRowAsHeader) {
      log.trace("configure() - Reading the header row.");
      fieldNames = this.csvReader.readNext();
      log.info("configure() - field names from header row. fields = {}", Joiner.on(", ").join(fieldNames));
    } else {
      log.trace("configure() - Using fields from schema {}", this.config.valueSchema.name());
      fieldNames = new String[this.config.valueSchema.fields().size()];
      int index = 0;
      for (Field field : this.config.valueSchema.fields()) {
        fieldNames[index++] = field.name();
      }
      log.info("configure() - field names from schema order. fields = {}", Joiner.on(", ").join(fieldNames));
    }

    if (null != lastOffset) {
      log.info("Found previous offset. Skipping {} line(s).", lastOffset.intValue());
      String[] row = null;
      while (null != (row = this.csvReader.readNext()) && this.csvReader.getLinesRead() < lastOffset) {
        log.trace("skipped row");
      }
    }

    this.fieldNames = fieldNames;
  }

  @Override
  public void start(Map<String, String> settings) {
    super.start(settings);
  }

  @Override
  public long recordOffset() {
    final long result;
    if (null == this.csvReader) {
      result = -1L;
    } else {
      result = this.csvReader.getLinesRead();
    }
    return result;
  }

  @Override
  public List<SourceRecord> process() throws IOException {
    List<SourceRecord> records = new ArrayList<>(this.config.batchSize);

    while (records.size() < this.config.batchSize) {
      String[] row = this.csvReader.readNext();

      if (row == null) {
        break;
      }
      log.trace("process() - Row on line {} has {} field(s)", recordOffset(), row.length);

      Struct keyStruct = new Struct(this.config.keySchema);
      Struct valueStruct = new Struct(this.config.valueSchema);

      for (int i = 0; i < this.fieldNames.length; i++) {
        String fieldName = this.fieldNames[i];
        log.trace("process() - Processing field {}", fieldName);
        String input = row[i];
        log.trace("process() - input = '{}'", input);
        Object fieldValue = null;

        try {
          Field field = this.config.valueSchema.field(fieldName);
          if (null != field) {
            fieldValue = this.parser.parseString(field.schema(), input);
            log.trace("process() - output = '{}'", fieldValue);
            valueStruct.put(field, fieldValue);
          } else {
            log.trace("process() - Field {} is not defined in the schema.", fieldName);
          }
        } catch (Exception ex) {
          String message = String.format("Exception thrown while parsing data for '%s'. linenumber=%s", fieldName, this.recordOffset());
          throw new DataException(message, ex);
        }

        Field keyField = this.config.keySchema.field(fieldName);
        if (null != keyField) {
          log.trace("process() - Setting key field '{}' to '{}'", keyField.name(), fieldValue);
          keyStruct.put(keyField, fieldValue);
        }
      }

      if (log.isInfoEnabled() && this.csvReader.getLinesRead() % ((long) this.config.batchSize * 20) == 0) {
        log.info("Processed {} lines of {}", this.csvReader.getLinesRead(), this.inputFile);
      }

      addRecord(
          records,
          new SchemaAndValue(keyStruct.schema(), keyStruct),
          new SchemaAndValue(valueStruct.schema(), valueStruct)
      );


    }
    return records;
  }
}
