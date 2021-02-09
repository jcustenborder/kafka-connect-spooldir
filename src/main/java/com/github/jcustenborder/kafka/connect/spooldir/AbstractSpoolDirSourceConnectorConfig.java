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

import shaded.com.fasterxml.jackson.core.JsonProcessingException;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.recommenders.Recommenders;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import shaded.com.google.common.base.Preconditions;
import shaded.com.google.common.base.Strings;
import shaded.com.google.common.collect.ImmutableList;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;


@SuppressWarnings("WeakerAccess")
public abstract class AbstractSpoolDirSourceConnectorConfig extends AbstractSourceConnectorConfig {
  public static final String TIMESTAMP_FIELD_CONF = "timestamp.field";
  public static final String KEY_SCHEMA_CONF = "key.schema";
  public static final String VALUE_SCHEMA_CONF = "value.schema";
  public static final String PARSER_TIMESTAMP_DATE_FORMATS_CONF = "parser.timestamp.date.formats";
  public static final String PARSER_TIMESTAMP_TIMEZONE_CONF = "parser.timestamp.timezone";
  public static final String SCHEMA_GENERATION_KEY_FIELDS_CONF = "schema.generation.key.fields";
  public static final String SCHEMA_GENERATION_KEY_NAME_CONF = "schema.generation.key.name";
  public static final String SCHEMA_GENERATION_VALUE_NAME_CONF = "schema.generation.value.name";
  public static final String SCHEMA_GENERATION_ENABLED_CONF = "schema.generation.enabled";
  public static final String GROUP_SCHEMA_GENERATION = "Schema Generation";
  public static final String GROUP_SCHEMA = "Schema";
  static final String KEY_SCHEMA_DOC = "The schema for the key written to Kafka.";
  static final String VALUE_SCHEMA_DOC = "The schema for the value written to Kafka.";
  static final String PARSER_TIMESTAMP_DATE_FORMATS_DOC = "The date formats that are expected in the file. This is a list " +
      "of strings that will be used to parse the date fields in order. The most accurate date format should be the first " +
      "in the list. Take a look at the Java documentation for more info. " +
      "https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html";
  static final List<String> PARSER_TIMESTAMP_DATE_FORMATS_DEFAULT = Arrays.asList("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd' 'HH:mm:ss");
  static final String PARSER_TIMESTAMP_TIMEZONE_DOC = "The timezone that all of the dates will be parsed with.";
  static final String PARSER_TIMESTAMP_TIMEZONE_DEFAULT = "UTC";
  static final String SCHEMA_GENERATION_KEY_FIELDS_DOC = "The field(s) to use to build a key schema. This is only used during schema generation.";
  static final String SCHEMA_GENERATION_KEY_NAME_DOC = "The name of the generated key schema.";
  static final String SCHEMA_GENERATION_VALUE_NAME_DOC = "The name of the generated value schema.";
  static final String SCHEMA_GENERATION_ENABLED_DOC = "Flag to determine if schemas should be dynamically generated. If set " +
      " to true, `" + KEY_SCHEMA_CONF + "` and `" + VALUE_SCHEMA_CONF + "` can be omitted, but `" + SCHEMA_GENERATION_KEY_NAME_CONF + "` " +
      "and `" + SCHEMA_GENERATION_VALUE_NAME_CONF + "` must be set.";

  private static final Logger log = LoggerFactory.getLogger(AbstractSpoolDirSourceConnectorConfig.class);

  public final Schema keySchema;
  public final Schema valueSchema;
  public final SimpleDateFormat[] parserTimestampDateFormats;
  public final TimeZone parserTimestampTimezone;

  public final String timestampField;
  public final List<String> keyFields;

  public final boolean schemaGenerationEnabled;
  public final String schemaGenerationKeyName;
  public final String schemaGenerationValueName;


  public AbstractSpoolDirSourceConnectorConfig(final boolean isTask, boolean bufferedInputStream, ConfigDef configDef, Map<String, ?> settings) {
    super(configDef, settings, bufferedInputStream);


    this.keyFields = this.getList(SCHEMA_GENERATION_KEY_FIELDS_CONF);
    this.schemaGenerationEnabled = this.getBoolean(SCHEMA_GENERATION_ENABLED_CONF);
    this.schemaGenerationKeyName = this.getString(SCHEMA_GENERATION_KEY_NAME_CONF);
    this.schemaGenerationValueName = this.getString(SCHEMA_GENERATION_VALUE_NAME_CONF);


    String timestampTimezone = this.getString(PARSER_TIMESTAMP_TIMEZONE_CONF);
    this.parserTimestampTimezone = TimeZone.getTimeZone(timestampTimezone);

    List<SimpleDateFormat> results = new ArrayList<>();
    List<String> formats = this.getList(PARSER_TIMESTAMP_DATE_FORMATS_CONF);
    for (String s : formats) {
      SimpleDateFormat dateFormat = new SimpleDateFormat(s);
      dateFormat.setTimeZone(this.parserTimestampTimezone);
      results.add(dateFormat);
    }
    this.parserTimestampDateFormats = results.toArray(new SimpleDateFormat[results.size()]);


    this.keySchema = readSchema(KEY_SCHEMA_CONF);
    this.valueSchema = readSchema(VALUE_SCHEMA_CONF);

    if (!this.schemaGenerationEnabled) {
      Preconditions.checkNotNull(
          this.keySchema,
          "'%s' must be set if '%s' = false.",
          KEY_SCHEMA_CONF,
          SCHEMA_GENERATION_ENABLED_CONF
      );
      Preconditions.checkNotNull(
          this.valueSchema,
          "'%s' must be set if '%s' = false.",
          VALUE_SCHEMA_CONF,
          SCHEMA_GENERATION_ENABLED_CONF
      );
    } else {
      Preconditions.checkState(
          !Strings.isNullOrEmpty(this.schemaGenerationKeyName),
          "'%s' must be set if '%s' = true.",
          SCHEMA_GENERATION_KEY_NAME_CONF,
          SCHEMA_GENERATION_ENABLED_CONF
      );
      Preconditions.checkState(
          !Strings.isNullOrEmpty(this.schemaGenerationValueName),
          "'%s' must be set if '%s' = true.",
          SCHEMA_GENERATION_VALUE_NAME_CONF,
          SCHEMA_GENERATION_ENABLED_CONF
      );
    }

    if (TimestampMode.FIELD == this.timestampMode) {
      this.timestampField = this.getString(TIMESTAMP_FIELD_CONF);

      if (Strings.isNullOrEmpty(this.timestampField)) {
        throw new ConnectException(
            String.format(
                "When `%s` is set to `%s`, `%s` must be set to a timestamp field. Cannot be null or empty.",
                TIMESTAMP_MODE_CONF,
                TimestampMode.FIELD,
                TIMESTAMP_FIELD_CONF
            )
        );
      }

      log.trace("ctor() - Looking for timestamp field '{}'", this.timestampField);
      Field timestampField = this.valueSchema.field(this.timestampField);

      if (null == timestampField ||
          timestampField.schema().isOptional() ||
          !Timestamp.LOGICAL_NAME.equals(timestampField.schema().name())) {

        String example;

        try {
          example = ObjectMapperFactory.INSTANCE.writeValueAsString(Timestamp.SCHEMA);
        } catch (JsonProcessingException e) {
          example = null;
        }

        log.trace("ctor() - example: {}", example);

        throw new ConnectException(
            String.format(
                "Field '%s' must be present and set to a timestamp and cannot be optional. Example %s",
                this.timestampField,
                example
            )
        );
      }
    } else {
      this.timestampField = null;
    }

    if (schemasRequired() && (isTask && null == this.valueSchema)) {
      throw new DataException(
          String.format("'%s' must be set to a valid schema.", VALUE_SCHEMA_CONF)
      );
    }
  }

  protected static ConfigDef config(boolean bufferedInputStream) {

    ConfigDef.Recommender schemaRecommender = new ConfigDef.Recommender() {
      @Override
      public List<Object> validValues(String key, Map<String, Object> settings) {
        return ImmutableList.of();
      }

      @Override
      public boolean visible(String key, Map<String, Object> settings) {
        boolean schemaGenerationEnabled = (boolean) settings.get(SCHEMA_GENERATION_ENABLED_CONF);

        if (KEY_SCHEMA_CONF.endsWith(key)) {
          return !schemaGenerationEnabled;
        }
        if (VALUE_SCHEMA_CONF.endsWith(key)) {
          return !schemaGenerationEnabled;
        }
        if (SCHEMA_GENERATION_KEY_NAME_CONF.endsWith(key)) {
          return schemaGenerationEnabled;
        }
        if (SCHEMA_GENERATION_VALUE_NAME_CONF.endsWith(key)) {
          return schemaGenerationEnabled;
        }
        if (SCHEMA_GENERATION_KEY_FIELDS_CONF.endsWith(key)) {
          return schemaGenerationEnabled;
        }

        return true;
      }
    };


    return AbstractSourceConnectorConfig.config(bufferedInputStream)
        .define(
            ConfigKeyBuilder.of(KEY_SCHEMA_CONF, Type.STRING)
                .documentation(KEY_SCHEMA_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .group(GROUP_SCHEMA)
                .defaultValue("")
                .width(ConfigDef.Width.LONG)
                .build()
        ).define(
            ConfigKeyBuilder.of(VALUE_SCHEMA_CONF, Type.STRING)
                .documentation(VALUE_SCHEMA_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .group(GROUP_SCHEMA)
                .defaultValue("")
                .width(ConfigDef.Width.LONG)
                .build()
        ).define(
            ConfigKeyBuilder.of(SCHEMA_GENERATION_ENABLED_CONF, Type.BOOLEAN)
                .documentation(SCHEMA_GENERATION_ENABLED_DOC)
                .importance(ConfigDef.Importance.MEDIUM)
                .group(GROUP_SCHEMA_GENERATION)
                .defaultValue(false)
                .recommender(schemaRecommender)
                .build()
        )
        .define(
            ConfigKeyBuilder.of(SCHEMA_GENERATION_KEY_FIELDS_CONF, Type.LIST)
                .documentation(SCHEMA_GENERATION_KEY_FIELDS_DOC)
                .importance(ConfigDef.Importance.MEDIUM)
                .group(GROUP_SCHEMA_GENERATION)
                .defaultValue(ImmutableList.of())
                .recommender(schemaRecommender)
                .build()
        ).define(
            ConfigKeyBuilder.of(SCHEMA_GENERATION_KEY_NAME_CONF, Type.STRING)
                .documentation(SCHEMA_GENERATION_KEY_NAME_DOC)
                .importance(ConfigDef.Importance.MEDIUM)
                .group(GROUP_SCHEMA_GENERATION)
                .defaultValue("com.github.jcustenborder.kafka.connect.model.Key")
                .recommender(schemaRecommender)
                .build()
        ).define(
            ConfigKeyBuilder.of(SCHEMA_GENERATION_VALUE_NAME_CONF, Type.STRING)
                .documentation(SCHEMA_GENERATION_VALUE_NAME_DOC)
                .importance(ConfigDef.Importance.MEDIUM)
                .group(GROUP_SCHEMA_GENERATION)
                .defaultValue("com.github.jcustenborder.kafka.connect.model.Value")
                .recommender(schemaRecommender)
                .build()
        )

        .define(
            ConfigKeyBuilder.of(PARSER_TIMESTAMP_TIMEZONE_CONF, Type.STRING)
                .documentation(PARSER_TIMESTAMP_TIMEZONE_DOC)
                .importance(ConfigDef.Importance.LOW)
                .group(GROUP_TIMESTAMP)
                .defaultValue(PARSER_TIMESTAMP_TIMEZONE_DEFAULT)
                .build()
        ).define(
            ConfigKeyBuilder.of(PARSER_TIMESTAMP_DATE_FORMATS_CONF, Type.LIST)
                .documentation(PARSER_TIMESTAMP_DATE_FORMATS_DOC)
                .importance(ConfigDef.Importance.LOW)
                .group(GROUP_TIMESTAMP)
                .defaultValue(PARSER_TIMESTAMP_DATE_FORMATS_DEFAULT)
                .build()
        ).define(
            ConfigKeyBuilder.of(TIMESTAMP_FIELD_CONF, Type.STRING)
                .documentation(TIMESTAMP_FIELD_DOC)
                .importance(ConfigDef.Importance.MEDIUM)
                .group(GROUP_TIMESTAMP)
                .defaultValue("")
                .recommender(
                    Recommenders.visibleIf(TIMESTAMP_MODE_CONF, TimestampMode.FIELD.toString())
                )
                .build()
        );
  }

  public abstract boolean schemasRequired();

  Schema readSchema(final String key) {
    String schema = this.getString(key);
    Schema result;

    if (Strings.isNullOrEmpty(schema)) {
      result = null;
    } else {
      try {
        result = ObjectMapperFactory.INSTANCE.readValue(schema, Schema.class);
      } catch (IOException e) {
        throw new DataException("Could not read schema from '" + key + "'", e);
      }
    }

    return result;
  }

}
