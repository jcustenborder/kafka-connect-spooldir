package io.confluent.kafka.connect.source.io.processing;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import io.confluent.kafka.connect.conversion.Converter;
import io.confluent.kafka.connect.conversion.type.DateTypeConverter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CSVRecordProcessor implements RecordProcessor {
  private static final Logger log = LoggerFactory.getLogger(CSVRecordProcessor.class);
  private CSVRecordProcessorConfig config;
  private CSVParser csvParser;
  private CSVReader csvReader;
  private InputStreamReader streamReader;

  private List<CSVFieldConfig> fields;
  private Schema valueSchema;
  private Schema keySchema;
  private String fileName;
  private Converter converter = new Converter();

  private Schema buildValueSchema() {
    SchemaBuilder builder = SchemaBuilder.struct();

    for (CSVFieldConfig fieldConfig : fields) {
      builder.field(fieldConfig.name(), fieldConfig.schema());
    }

    return builder.build();
  }

  private Schema buildKeySchema() {
    SchemaBuilder builder = SchemaBuilder.struct();

    for (String key : this.config.keyFields()) {
      Field field = this.valueSchema.field(key);
      Preconditions.checkState(null != field, "Could not find key '%s' in available fields.", key);
      builder.field(field.name(), field.schema());
    }

    return builder.build();
  }


  @Override
  public void configure(Map<?, ?> configValues, InputStream inputStream, String fileName) throws IOException {
    this.config = new CSVRecordProcessorConfig(configValues);

    if (log.isDebugEnabled()) {
      log.debug("Configuring CSVParser...");
    }

    DateTypeConverter timestampDateConverter = new DateTypeConverter(this.config.parserTimestampTimezone(), this.config.parserTimestampDateFormats());
    this.converter.registerTypeConverter(Timestamp.SCHEMA, timestampDateConverter);

    this.csvParser = this.config.createCSVParserBuilder().build();
    this.streamReader = new InputStreamReader(inputStream, this.config.charset());
    this.csvReader = this.config.createCSVReaderBuilder(this.streamReader, csvParser).build();

    if (this.config.firstRowAsHeader()) {
      String[] fieldNames = this.csvReader.readNext();

      if (log.isDebugEnabled()) {
        log.debug("Field names for the file are {}", Joiner.on(", ").join(fieldNames));
      }

      List<CSVFieldConfig> fields = new ArrayList<>();

      for (int i = 0; i < fieldNames.length; i++) {
        String fieldName = fieldNames[i];

        Map<?, ?> fieldSettings = ImmutableMap.of(
            CSVFieldConfig.NAME_CONF, fieldName,
            CSVFieldConfig.SCHEMA_TYPE_CONF, Schema.Type.STRING.name(),
            CSVFieldConfig.REQUIRED_CONF, Boolean.FALSE.toString()
        );

        fields.add(new CSVFieldConfig(fieldSettings, i));
      }
      this.fields = fields;
    } else {
      this.fields = this.config.fields();
    }

    this.valueSchema = buildValueSchema();
    this.keySchema = buildKeySchema();

    this.fileName = fileName;
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

      Struct valueStruct = new Struct(this.valueSchema);
      Struct keyStruct = new Struct(this.keySchema);

      Preconditions.checkState(this.valueSchema.fields().size() == record.length, "Record has %s columns but schema has %s columns.",
          this.valueSchema.fields().size(),
          record.length
      );

      for (int i = 0; i < record.length; i++) {
        Field field = this.valueSchema.fields().get(i);
        String input = record[i];
        Object value = converter.convert(field.schema(), input);
        valueStruct.put(field.name(), value);
      }

      //Read the key values from the converted value struct.
      for (Field field : this.keySchema.fields()) {
        Object value = valueStruct.get(field);
        keyStruct.put(field.name(), value);
      }

      Map<String, ?> partitions = ImmutableMap.of();
      Map<String, ?> offset = ImmutableMap.of(this.fileName, csvReader.getLinesRead());

      SourceRecord sourceRecord = new SourceRecord(
          partitions,
          offset,
          this.config.topic(),
          this.keySchema,
          keyStruct,
          this.valueSchema,
          valueStruct);
      records.add(sourceRecord);
    }
    return records;
  }

  @Override
  public void close() throws Exception {

  }
}
