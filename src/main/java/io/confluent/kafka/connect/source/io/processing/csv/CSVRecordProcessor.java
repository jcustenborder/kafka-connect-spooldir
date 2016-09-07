/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.connect.source.io.processing.csv;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import io.confluent.kafka.connect.source.SpoolDirectoryConfig;
import io.confluent.kafka.connect.source.io.processing.FileMetadata;
import io.confluent.kafka.connect.source.io.processing.RecordProcessor;
import io.confluent.kafka.connect.utils.data.Parser;
import io.confluent.kafka.connect.utils.data.type.DateTypeParser;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CSVRecordProcessor implements RecordProcessor {
  private static final Logger log = LoggerFactory.getLogger(CSVRecordProcessor.class);
  private SpoolDirectoryConfig config;
  private CSVParser csvParser;
  private CSVReader csvReader;
  private InputStreamReader streamReader;
  private FileMetadata fileMetadata;
  private SchemaConfig schemaConfig;
  private SchemaConfig.ParserConfig valueParserConfig;
  private SchemaConfig.ParserConfig keyParserConfig;
  private Parser parser = new Parser();


  @Override
  public void configure(SpoolDirectoryConfig config, InputStream inputStream, FileMetadata fileMetadata) throws IOException {
    this.config = config;

    if (log.isDebugEnabled()) {
      log.debug("Configuring CSVParser...");
    }

    DateTypeParser timestampDateConverter = new DateTypeParser(this.config.parserTimestampTimezone(), this.config.parserTimestampDateFormats());
    this.parser.registerTypeParser(Timestamp.SCHEMA, timestampDateConverter);

    this.csvParser = this.config.createCSVParserBuilder().build();
    this.streamReader = new InputStreamReader(inputStream, this.config.charset());
    this.csvReader = this.config.createCSVReaderBuilder(this.streamReader, csvParser).build();

    String[] fieldNames;

    if (this.config.firstRowAsHeader()) {
      if (log.isDebugEnabled()) {
        log.debug("Reading the first line ");
      }
      fieldNames = this.csvReader.readNext();
      if (log.isDebugEnabled()) {
        log.debug("FieldMapping names for the file are {}", Joiner.on(", ").join(fieldNames));
      }
    } else {
      fieldNames = null;
    }

    if (this.config.schemaFromHeader()) {
      Preconditions.checkState(
          this.config.firstRowAsHeader(),
          "If the %s is set to true, then %s must be set to true as well.",
          SpoolDirectoryConfig.CSV_SCHEMA_FROM_HEADER_KEYS_CONF,
          SpoolDirectoryConfig.CSV_FIRST_ROW_AS_HEADER_CONF
      );

      SchemaConfig schemaConfig = new SchemaConfig();

      for (int i = 0; i < fieldNames.length; i++) {
        FieldConfig fieldConfig = FieldConfig.create(Schema.OPTIONAL_STRING_SCHEMA);
        fieldConfig.name = fieldNames[i];
        fieldConfig.index = i;
        schemaConfig.fields.add(fieldConfig);
      }
      schemaConfig.keys = this.config.schemaFromHeaderKeys();
      schemaConfig.name = this.config.schemaName();
      Preconditions.checkNotNull(schemaConfig.name, "%s must be configured when generating the schema from the header row.", SpoolDirectoryConfig.CSV_SCHEMA_NAME_CONF);
      Preconditions.checkState(!schemaConfig.name.isEmpty(), "%s must be configured when generating the schema from the header row.", SpoolDirectoryConfig.CSV_SCHEMA_NAME_CONF);
      this.schemaConfig = schemaConfig;
    } else {
      this.schemaConfig = this.config.schemaConfig();

      if (this.config.firstRowAsHeader()) {
        Map<String, FieldConfig> map = new LinkedHashMap<>();
        for (FieldConfig field : this.schemaConfig.fields) {
          String mapKey = this.config.caseSensitiveFieldNames() ? field.name : field.name.toLowerCase();
          Preconditions.checkState(!map.containsKey(mapKey), "Schema already has a field with name '%s' defined.", field.name);
          map.put(mapKey, field);
        }

        int fieldIndex = 0;
        for (String fieldName : fieldNames) {
          String mapKey = this.config.caseSensitiveFieldNames() ? fieldName : fieldName.toLowerCase();
          FieldConfig field = map.get(mapKey);

          if (null == field) {
            if (log.isDebugEnabled()) {
              log.debug("FieldMapping '{}' was not found in schema. Skipping.", fieldName);
            }
            continue;
          }
          field.index = fieldIndex;
          fieldIndex++;
        }

      } else {
        if (log.isDebugEnabled()) {
          log.debug("Laying out fields in the order they are in the schema.");
        }

        for (int i = 0; i < this.schemaConfig.fields.size(); i++) {
          FieldConfig field = this.schemaConfig.fields.get(i);
          field.index = i;
          if (log.isDebugEnabled()) {
            log.debug("FieldMapping {} index {}.", field.name, field.index);
          }
        }
      }

    }

    Pair<SchemaConfig.ParserConfig, SchemaConfig.ParserConfig> parserConfigs = this.schemaConfig.parserConfigs(this.config);

    this.keyParserConfig = parserConfigs.getKey();
    this.valueParserConfig = parserConfigs.getValue();

    this.fileMetadata = fileMetadata;
  }

  @Override
  public long lineNumber() {
    return this.csvReader.getLinesRead();
  }

  @Override
  public List<SourceRecord> poll() throws IOException {
    List<SourceRecord> records = new ArrayList<>(this.config.batchSize());

    while (records.size() < this.config.batchSize()) {
      String[] record = this.csvReader.readNext();

      if (record == null) {
        break;
      }

      Struct valueStruct = new Struct(this.valueParserConfig.structSchema);
      Struct keyStruct;

      if (this.keyParserConfig.mappings.isEmpty()) {
        keyStruct = null;
      } else {
        keyStruct = new Struct(this.keyParserConfig.structSchema);
      }


      Preconditions.checkState(this.valueParserConfig.mappings.size() == record.length, "Record has %s columns but schemaConfig has %s columns.",
          this.valueParserConfig.mappings.size(),
          record.length
      );

      //Keep the objects in an array that way we don't parse them for the key.
      Object[] values = new Object[record.length];

      for (SchemaConfig.FieldMapping mapping : this.valueParserConfig.mappings) {
        String input = record[mapping.index];
        try {
          values[mapping.index] = parser.parseString(mapping.schema, input);
        } catch (Exception ex) {
          String message = String.format("Exception thrown while parsing data for '%s'. linenumber=%s", mapping.fieldName, this.lineNumber());
          throw new DataException(message, ex);
        }
      }

      for (SchemaConfig.FieldMapping mapping : this.valueParserConfig.mappings) {
        valueStruct.put(mapping.fieldName, values[mapping.index]);
      }

      for (SchemaConfig.FieldMapping mapping : this.keyParserConfig.mappings) {
        keyStruct.put(mapping.fieldName, values[mapping.index]);
      }

      if (this.config.includeFileMetadata()) {
        this.fileMetadata.addToStruct(valueStruct, this.csvReader.getLinesRead());
      }

      if (log.isInfoEnabled() && this.csvReader.getLinesRead() % ((long) this.config.batchSize() * 20) == 0) {
        log.info("Processed {} lines of {}", this.csvReader.getLinesRead(), this.fileMetadata);
      }

      Map<String, ?> partitions = ImmutableMap.of();
      Map<String, ?> offset = ImmutableMap.of(this.fileMetadata.fileName(), csvReader.getLinesRead());

      SourceRecord sourceRecord = new SourceRecord(
          partitions,
          offset,
          this.config.topic(),
          null == keyStruct ? null : this.keyParserConfig.structSchema,
          null == keyStruct ? null : keyStruct,
          this.valueParserConfig.structSchema,
          valueStruct);
      records.add(sourceRecord);
    }
    return records;
  }

  @Override
  public void close() throws Exception {

  }
}
