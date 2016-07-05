package io.confluent.kafka.connect.source.io.processing;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import io.confluent.kafka.connect.conversion.Converter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
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
  CSVRecordProcessorConfig config;
  CSVParser csvParser;
  CSVReader csvReader;
  InputStreamReader streamReader;

  String[] fieldNames;

  Schema buildValueSchema() {
    SchemaBuilder builder = SchemaBuilder.struct();

    for(String field:this.fieldNames){
      builder.field(field, Schema.OPTIONAL_STRING_SCHEMA);
    }

    return builder.build();
  }

  Schema buildKeySchema(){
    SchemaBuilder builder = SchemaBuilder.struct();

    for(String key:this.config.keyFields()){
      Field field = this.valueSchema.field(key);
      Preconditions.checkState(null!=field, "Could not find key '%s' in available fields.", key);
      builder.field(field.name(), field.schema());
    }

    return builder.build();
  }


  Schema valueSchema;
  Schema keySchema;
  String fileName;
  Converter converter = new Converter();

  @Override
  public void configure(Map<?, ?> configValues, InputStream inputStream, String fileName) throws IOException {
    this.config = new CSVRecordProcessorConfig(configValues);

    if(log.isDebugEnabled()){
      log.debug("Configuring CSVParser...");
    }

    this.csvParser = this.config.createCSVParserBuilder().build();
    this.streamReader = new InputStreamReader(inputStream);
    this.csvReader = this.config.createCSVReaderBuilder(this.streamReader, csvParser).build();

    if(this.config.firstRowAsHeader()){
      this.fieldNames = this.csvReader.readNext();

      if(log.isDebugEnabled()){
        log.debug("Field names for the file are {}", Joiner.on(", ").join(this.fieldNames));
      }
    } else {

    }

    this.valueSchema = buildValueSchema();
    this.keySchema = buildKeySchema();

    this.fileName = fileName;
  }

  @Override
  public List<SourceRecord> poll() throws IOException {
    List<SourceRecord> records = new ArrayList<>(this.config.batchSize());

    while(records.size() < this.config.batchSize()){
      String[] record = this.csvReader.readNext();

      if(record==null){
        break;
      }

      Struct valueStruct = new Struct(this.valueSchema);
      Struct keyStruct = new Struct(this.keySchema);

      for(int i=0;i<record.length;i++){
        String fieldName=this.fieldNames[i];
        Field field = this.valueSchema.field(fieldName);
        String input = record[i];
        Object value = converter.convert(field.schema(), input);
        valueStruct.put(fieldName, value);
      }

      //Read the key values from the converted value struct.
      for(Field field:this.keySchema.fields()){
        Object value = valueStruct.get(field);
        keyStruct.put(field.name(), value);
      }

      Map<String,?> partitions = ImmutableMap.of();
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
