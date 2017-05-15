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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class SchemaGenerator<CONFIG extends SpoolDirSourceConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(SchemaGenerator.class);
  protected CONFIG config;

  protected abstract CONFIG config(Map<String, ?> settings);

  protected abstract Map<String, Schema.Type> determineFieldTypes(InputStream inputStream) throws IOException;

  static final String DUMMY_SCHEMA;

  static {
    String dummySchema;
    try {
      dummySchema = ObjectMapperFactory.INSTANCE.writeValueAsString(SchemaBuilder.struct().build());
    } catch (JsonProcessingException e) {
      dummySchema = null;
    }

    DUMMY_SCHEMA = dummySchema;
  }

  static final Map<String, Object> DEFAULTS;

  static {
    Map<String, Object> defaultSettings = new LinkedHashMap<>();
    defaultSettings.put(SpoolDirSourceConnectorConfig.INPUT_FILE_PATTERN_CONF, ".*");
    defaultSettings.put(SpoolDirSourceConnectorConfig.INPUT_PATH_CONFIG, "/tmp/input");
    defaultSettings.put(SpoolDirSourceConnectorConfig.FINISHED_PATH_CONFIG, "/tmp/finish");
    defaultSettings.put(SpoolDirSourceConnectorConfig.ERROR_PATH_CONFIG, "/tmp/error");
    defaultSettings.put(SpoolDirSourceConnectorConfig.VALUE_SCHEMA_CONF, DUMMY_SCHEMA);
    defaultSettings.put(SpoolDirSourceConnectorConfig.KEY_SCHEMA_CONF, DUMMY_SCHEMA);
    defaultSettings.put(SpoolDirSourceConnectorConfig.TOPIC_CONF, "dummy");
    DEFAULTS = ImmutableMap.copyOf(defaultSettings);
  }


  public SchemaGenerator(Map<String, ?> settings) {
    Map<String, Object> copySettings = new LinkedHashMap<>(settings);

    for (Map.Entry<String, Object> kvp : DEFAULTS.entrySet()) {
      if (!copySettings.containsKey(kvp.getKey())) {
        copySettings.put(kvp.getKey(), kvp.getValue());
      }
    }

    this.config = config(copySettings);
  }

  void addField(SchemaBuilder builder, String name, Schema.Type schemaType) {
    log.trace("addField() - name = {} schemaType = {}", name, schemaType);
    builder.field(
        name,
        SchemaBuilder.type(schemaType).optional().build()
    );
  }

  public Map.Entry<Schema, Schema> generate(File inputFile, List<String> keyFields) throws IOException {
    log.trace("generate() - inputFile = '{}', keyFields = {}", inputFile, keyFields);

    final Map<String, Schema.Type> fieldTypes;

    log.info("Determining fields from {}", inputFile);
    try (InputStream inputStream = new FileInputStream(inputFile)) {
      fieldTypes = determineFieldTypes(inputStream);
    }

    log.trace("generate() - Building key schema.");
    SchemaBuilder keySchemaBuilder = SchemaBuilder.struct()
        .name("com.github.jcustenborder.kafka.connect.model.Key");

    for (String keyFieldName : keyFields) {
      log.trace("generate() - Adding keyFieldName field '{}'", keyFieldName);
      if (fieldTypes.containsKey(keyFieldName)) {
        Schema.Type schemaType = fieldTypes.get(keyFieldName);
        addField(keySchemaBuilder, keyFieldName, schemaType);
      } else {
        log.warn("Key field '{}' is not in the data.", keyFieldName);
      }
    }

    log.trace("generate() - Building value schema.");
    SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct()
        .name("com.github.jcustenborder.kafka.connect.model.Value");

    for (Map.Entry<String, Schema.Type> kvp : fieldTypes.entrySet()) {
      addField(valueSchemaBuilder, kvp.getKey(), kvp.getValue());
    }

    return new AbstractMap.SimpleEntry<>(keySchemaBuilder.build(), valueSchemaBuilder.build());
  }


  public static void main(String... args) throws Exception {
    ArgumentParser parser = ArgumentParsers.newArgumentParser("CsvSchemaGenerator")
        .defaultHelp(true)
        .description("Generate a schema based on a file.");
    parser.addArgument("-t", "--type")
        .required(true)
        .choices("csv", "json")
        .help("The type of generator to use.");
    parser.addArgument("-c", "--config")
        .type(File.class);
    parser.addArgument("-f", "--file")
        .type(File.class)
        .required(true)
        .help("The data file to generate the schema from.");
    parser.addArgument("-i", "--id")
        .nargs("*")
        .help("Field(s) to use as an identifier.");
    parser.addArgument("-o", "--output")
        .type(File.class)
        .help("Output location to write the configuration to. Stdout is default.");

    Namespace ns = null;

    try {
      ns = parser.parseArgs(args);
    } catch (ArgumentParserException ex) {
      parser.handleError(ex);
      System.exit(1);
    }

    File inputFile = ns.get("file");
    List<String> ids = ns.getList("id");
    if (null == ids) {
      ids = ImmutableList.of();
    }

    Map<String, Object> settings = new LinkedHashMap<>();

    File inputPropertiesFile = ns.get("config");
    if (null != inputPropertiesFile) {
      Properties inputProperties = new Properties();

      try (FileInputStream inputStream = new FileInputStream(inputPropertiesFile)) {
        inputProperties.load(inputStream);
      }
      for (String s : inputProperties.stringPropertyNames()) {
        Object v = inputProperties.getProperty(s);
        settings.put(s, v);
      }
    }

    final SchemaGenerator generator;
    final String type = ns.getString("type");

    if ("csv".equalsIgnoreCase(type)) {
      generator = new CsvSchemaGenerator(settings);
    } else if ("json".equalsIgnoreCase(type)) {
      generator = new JsonSchemaGenerator(settings);
    } else {
      throw new UnsupportedOperationException(
          String.format("'%s' is not a supported schema generator type", type)
      );
    }

    Map.Entry<Schema, Schema> kvp = generator.generate(inputFile, ids);

    Properties properties = new Properties();
    properties.putAll(settings);
    properties.setProperty(SpoolDirSourceConnectorConfig.KEY_SCHEMA_CONF, ObjectMapperFactory.INSTANCE.writeValueAsString(kvp.getKey()));
    properties.setProperty(SpoolDirSourceConnectorConfig.VALUE_SCHEMA_CONF, ObjectMapperFactory.INSTANCE.writeValueAsString(kvp.getValue()));

    String output = ns.getString("output");
    final String comment = "Configuration was dynamically generated. Please verify before submitting.";

    if (Strings.isNullOrEmpty(output)) {
      properties.store(System.out, comment);
    } else {
      try (FileOutputStream outputStream = new FileOutputStream(output)) {
        properties.store(outputStream, comment);
      }
    }
  }

}
