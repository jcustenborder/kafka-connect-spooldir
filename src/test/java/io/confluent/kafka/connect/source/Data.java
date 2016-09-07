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
package io.confluent.kafka.connect.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.opencsv.enums.CSVReaderNullFieldIndicator;
import io.confluent.kafka.connect.source.io.processing.csv.CSVRecordProcessor;
import io.confluent.kafka.connect.source.io.processing.csv.FieldConfig;
import io.confluent.kafka.connect.source.io.processing.csv.SchemaConfig;
import org.apache.kafka.connect.connector.ConnectRecord;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Data {

  public static InputStream mockData() {
    return Data.class.getResourceAsStream("MOCK_DATA.csv");
  }

  public static InputStream mockDataSmall() {
    return Data.class.getResourceAsStream("MOCK_DATA_SMALL.csv");
  }

  public static List<ConnectRecord> mockDataSmallExpected() {
    List<ConnectRecord> connectRecords = new ArrayList<>();


    return connectRecords;
  }


  static void addField(SchemaConfig schemaConfig, String name, FieldConfig.Type type, boolean required, Integer scale) {
    FieldConfig config = new FieldConfig();
    config.name = name;
    config.type = type;
    config.required = required;
    config.scale = scale;
    schemaConfig.fields.add(config);
  }

  public static SchemaConfig schemaConfig() {
    SchemaConfig schemaConfig = new SchemaConfig();
    schemaConfig.keys.add("id");
    schemaConfig.name = "io.confluent.kafka.connect.source.MockData";
    addField(schemaConfig, "id", FieldConfig.Type.INT32, true, null);
    addField(schemaConfig, "first_name", FieldConfig.Type.STRING, true, null);
    addField(schemaConfig, "last_name", FieldConfig.Type.STRING, true, null);
    addField(schemaConfig, "email", FieldConfig.Type.STRING, true, null);
    addField(schemaConfig, "gender", FieldConfig.Type.STRING, true, null);
    addField(schemaConfig, "ip_address", FieldConfig.Type.STRING, true, null);
    addField(schemaConfig, "last_login", FieldConfig.Type.TIMESTAMP, false, null);
    addField(schemaConfig, "account_balance", FieldConfig.Type.DECIMAL, false, 10);
    addField(schemaConfig, "country", FieldConfig.Type.STRING, true, null);
    addField(schemaConfig, "favorite_color", FieldConfig.Type.STRING, false, null);
    return schemaConfig;
  }

  public static final String TOPIC = "csv";

  public static Map<String, String> settings(File targetDir) {
    SchemaConfig config = schemaConfig();
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.enable(SerializationFeature.WRITE_ENUMS_USING_TO_STRING);
    objectMapper.enable(DeserializationFeature.READ_ENUMS_USING_TO_STRING);

    Map<String, String> result = new LinkedHashMap<>();

    try {
      String content = objectMapper.writeValueAsString(config);
      result.put(SpoolDirectoryConfig.CSV_SCHEMA_CONF, content);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }

    File inputDirectory = new File(targetDir, "input");
    inputDirectory.mkdirs();
    result.put(SpoolDirectoryConfig.INPUT_PATH_CONFIG, inputDirectory.getAbsolutePath());
    File finishDirectory = new File(targetDir, "finished");
    result.put(SpoolDirectoryConfig.FINISHED_PATH_CONFIG, finishDirectory.getAbsolutePath());
    finishDirectory.mkdirs();
    File errorDirectory = new File(targetDir, "error");
    result.put(SpoolDirectoryConfig.ERROR_PATH_CONFIG, errorDirectory.getAbsolutePath());
    errorDirectory.mkdirs();
    result.put(SpoolDirectoryConfig.TOPIC_CONF, TOPIC);
//    result.put(SpoolDirectoryConfig.KEY_FIELDS_CONF, "id");
    result.put(SpoolDirectoryConfig.CSV_FIRST_ROW_AS_HEADER_CONF, "true");
    result.put(SpoolDirectoryConfig.INPUT_FILE_PATTERN_CONF, "^.+\\.csv$");
    result.put(SpoolDirectoryConfig.CSV_NULL_FIELD_INDICATOR_CONF, CSVReaderNullFieldIndicator.BOTH.name());
    result.put(SpoolDirectoryConfig.CSV_PARSER_TIMESTAMP_DATE_FORMATS_CONF, "yyyy-MM-dd'T'HH:mm:ss'Z'");
    result.put(SpoolDirectoryConfig.RECORD_PROCESSOR_CLASS_CONF, CSVRecordProcessor.class.getName());
    result.put(SpoolDirectoryConfig.BATCH_SIZE_CONF, "100");
    result.put(SpoolDirectoryConfig.CSV_SCHEMA_NAME_CONF, Data.class.getName() + "Schema");
    return result;
  }

}
