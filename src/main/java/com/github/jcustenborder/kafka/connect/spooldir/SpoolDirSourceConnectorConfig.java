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
import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.ValidEnum;
import com.github.jcustenborder.kafka.connect.utils.config.ValidPattern;
import com.github.jcustenborder.kafka.connect.utils.config.validators.filesystem.ValidDirectoryWritable;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.io.PatternFilenameFilter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;


@SuppressWarnings("WeakerAccess")
public abstract class SpoolDirSourceConnectorConfig extends AbstractConfig {
  public static final String TIMESTAMP_FIELD_CONF = "timestamp.field";
  public static final String TIMESTAMP_MODE_CONF = "timestamp.mode";
  //DirectoryMonitorConfig
  //PollingDirectoryMonitorConfig
  public static final String INPUT_PATH_CONFIG = "input.path";
  public static final String FINISHED_PATH_CONFIG = "finished.path";
  public static final String ERROR_PATH_CONFIG = "error.path";
  public static final String INPUT_FILE_PATTERN_CONF = "input.file.pattern";
  public static final String HALT_ON_ERROR_CONF = "halt.on.error";
  public static final String FILE_MINIMUM_AGE_MS_CONF = "file.minimum.age.ms";
  public static final String PROCESSING_FILE_EXTENSION_CONF = "processing.file.extension";
  public static final String ERROR_FILE_EXTENSION_CONF = "error.file.extension";
  //RecordProcessorConfig
  public static final String BATCH_SIZE_CONF = "batch.size";
  public static final String PROCESSING_FILE_EXTENSION_DEFAULT = ".PROCESSING";
  public static final String ERROR_FILE_EXTENSION_DEFAULT = ".ERROR";
  public static final String TOPIC_CONF = "topic";
  public static final String KEY_SCHEMA_CONF = "key.schema";
  public static final String VALUE_SCHEMA_CONF = "value.schema";
  public static final String PARSER_TIMESTAMP_DATE_FORMATS_CONF = "parser.timestamp.date.formats";
  public static final String PARSER_TIMESTAMP_TIMEZONE_CONF = "parser.timestamp.timezone";
  public static final String EMPTY_POLL_WAIT_MS_CONF = "empty.poll.wait.ms";
  public static final String SCHEMA_GENERATION_KEY_FIELDS_CONF = "schema.generation.key.fields";
  public static final String SCHEMA_GENERATION_KEY_NAME_CONF = "schema.generation.key.name";
  public static final String SCHEMA_GENERATION_VALUE_NAME_CONF = "schema.generation.value.name";
  public static final String SCHEMA_GENERATION_ENABLED_CONF = "schema.generation.enabled";
  public static final String METADATA_SCHEMA_NAME = "com.github.jcustenborder.kafka.connect.spooldir.Metadata";
  public static final String CLEANUP_POLICY_CONF = "cleanup.policy";
  public static final String CLEANUP_POLICY_DOC = "Determines how the connector should cleanup the " +
      "files that have been successfully processed. NONE leaves the files in place which could " +
      "cause them to be reprocessed if the connector is restarted. DELETE removes the file from the " +
      "filesystem. MOVE will move the file to a finished directory.";
  public static final String GROUP_FILESYSTEM = "File System";
  public static final String GROUP_SCHEMA_GENERATION = "Schema Generation";
  public static final String GROUP_SCHEMA = "Schema";
  public static final String GROUP_GENERAL = "General";
  public static final String GROUP_TIMESTAMP = "Timestamps";
  static final String TIMESTAMP_FIELD_DOC = "The field in the value schema that will contain the parsed timestamp for the record. " +
      "This field cannot be marked as optional and must be a " +
      "[Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html)";
  static final String TIMESTAMP_MODE_DOC = "Determines how the connector will set the timestamp for the " +
      "[ConnectRecord](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/connector/ConnectRecord.html#timestamp()). " +
      "If set to `Field` then the timestamp will be read from a field in the value. This field cannot be optional and must be " +
      "a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html). Specify the field " +
      " in `" + TIMESTAMP_FIELD_CONF + "`. " +
      "If set to `FILE_TIME` then " +
      "the last modified time of the file will be used. If set to `PROCESS_TIME` the time the record is read will be used.";
  static final String BATCH_SIZE_DOC = "The number of records that should be returned with each batch.";
  static final int BATCH_SIZE_DEFAULT = 1000;
  static final String TOPIC_DOC = "The Kafka topic to write the data to.";
  static final String KEY_SCHEMA_DOC = "The schema for the key written to Kafka.";
  static final String VALUE_SCHEMA_DOC = "The schema for the value written to Kafka.";
  static final String INPUT_PATH_DOC = "The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.";
  static final String FINISHED_PATH_DOC = "The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.";
  static final String ERROR_PATH_DOC = "The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.";
  static final String INPUT_FILE_PATTERN_DOC = "Regular expression to check input file names against. This expression " +
      "must match the entire filename. The equivalent of Matcher.matches().";
  static final String HALT_ON_ERROR_DOC = "Should the task halt when it encounters an error or continue to the next file.";
  static final String FILE_MINIMUM_AGE_MS_DOC = "The amount of time in milliseconds after the file was last written to before the file can be processed.";
  static final String PROCESSING_FILE_EXTENSION_DOC = "Before a file is processed, it is renamed to indicate that it is currently being processed. This setting is appended to the end of the file.";
  static final String ERROR_FILE_EXTENSION_DOC = "Extra extension for the error file.";
  static final String PARSER_TIMESTAMP_DATE_FORMATS_DOC = "The date formats that are expected in the file. This is a list " +
      "of strings that will be used to parse the date fields in order. The most accurate date format should be the first " +
      "in the list. Take a look at the Java documentation for more info. " +
      "https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html";
  static final List<String> PARSER_TIMESTAMP_DATE_FORMATS_DEFAULT = Arrays.asList("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd' 'HH:mm:ss");
  static final String PARSER_TIMESTAMP_TIMEZONE_DOC = "The timezone that all of the dates will be parsed with.";
  static final String PARSER_TIMESTAMP_TIMEZONE_DEFAULT = "UTC";
  static final String EMPTY_POLL_WAIT_MS_DOC = "The amount of time to wait if a poll returns an empty list of records.";
  static final String SCHEMA_GENERATION_KEY_FIELDS_DOC = "The field(s) to use to build a key schema. This is only used during schema generation.";
  static final String SCHEMA_GENERATION_KEY_NAME_DOC = "The name of the generated key schema.";
  static final String SCHEMA_GENERATION_VALUE_NAME_DOC = "The name of the generated value schema.";
  static final String SCHEMA_GENERATION_ENABLED_DOC = "Flag to determine if schemas should be dynamically generated. If set " +
      " to true, `" + KEY_SCHEMA_CONF + "` and `" + VALUE_SCHEMA_CONF + "` can be omitted, but `" + SCHEMA_GENERATION_KEY_NAME_CONF + "` " +
      "and `" + SCHEMA_GENERATION_VALUE_NAME_CONF + "` must be set.";
  private static final Logger log = LoggerFactory.getLogger(SpoolDirSourceConnectorConfig.class);
  public final File inputPath;
  public final File finishedPath;
  public final File errorPath;
  public final boolean haltOnError;
  public final long minimumFileAgeMS;
  public final int batchSize;
  public final String topic;
  public final Schema keySchema;
  public final Schema valueSchema;
  public final Field keyMetadataField;
  public final Field valueMetadataField;
  public final SimpleDateFormat[] parserTimestampDateFormats;
  public final TimeZone parserTimestampTimezone;
  public final long emptyPollWaitMs;
  public final String processingFileExtension;
  public final String errorFileExtension;
  public final TimestampMode timestampMode;
  public final String timestampField;
  public final List<String> keyFields;
  public final PatternFilenameFilter inputFilenameFilter;
  public final boolean schemaGenerationEnabled;
  public final String schemaGenerationKeyName;
  public final String schemaGenerationValueName;
  public boolean hasKeyMetadataField;
  public boolean hasvalueMetadataField;
  public CleanupPolicy cleanupPolicy;
  public SpoolDirSourceConnectorConfig(final boolean isTask, ConfigDef configDef, Map<String, ?> settings) {
    super(configDef, settings);
    this.inputPath = ConfigUtils.getAbsoluteFile(this, INPUT_PATH_CONFIG);
    this.cleanupPolicy = ConfigUtils.getEnum(CleanupPolicy.class, this, CLEANUP_POLICY_CONF);

    if (CleanupPolicy.MOVE == this.cleanupPolicy) {
      this.finishedPath = ConfigUtils.getAbsoluteFile(this, FINISHED_PATH_CONFIG);
    } else {
      this.finishedPath = null;
    }

    this.errorPath = ConfigUtils.getAbsoluteFile(this, ERROR_PATH_CONFIG);
    this.haltOnError = this.getBoolean(HALT_ON_ERROR_CONF);
    this.minimumFileAgeMS = this.getLong(FILE_MINIMUM_AGE_MS_CONF);
    this.batchSize = this.getInt(BATCH_SIZE_CONF);
    this.topic = this.getString(TOPIC_CONF);
    this.emptyPollWaitMs = this.getLong(EMPTY_POLL_WAIT_MS_CONF);
    this.processingFileExtension = this.getString(PROCESSING_FILE_EXTENSION_CONF);
    this.errorFileExtension = this.getString(ERROR_FILE_EXTENSION_CONF);
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

    if (null != this.keySchema) {
      this.keyMetadataField = findMetadataField(this.keySchema);
      this.hasKeyMetadataField = null != this.keyMetadataField;
    } else {
      this.keyMetadataField = null;
      this.hasKeyMetadataField = false;
    }

    if (null != this.valueSchema) {
      this.valueMetadataField = findMetadataField(this.valueSchema);
      this.hasvalueMetadataField = null != this.valueMetadataField;
    } else {
      this.valueMetadataField = null;
      this.hasvalueMetadataField = false;
    }

    this.timestampMode = ConfigUtils.getEnum(TimestampMode.class, this, TIMESTAMP_MODE_CONF);

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

    final String inputPatternText = this.getString(INPUT_FILE_PATTERN_CONF);
    final Pattern inputPattern = Pattern.compile(inputPatternText);
    this.inputFilenameFilter = new PatternFilenameFilter(inputPattern);
  }

  private static final Field findMetadataField(Schema schema) {
    Field result = null;
    for (Field field : schema.fields()) {
      if (METADATA_SCHEMA_NAME.equals(field.schema().name()) &&
          Schema.Type.MAP == field.schema().type() &&
          Schema.Type.STRING == field.schema().valueSchema().type()) {
        result = field;
        break;
      }
    }
    return result;
  }

  public static ConfigDef config() {

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

    ConfigDef.Recommender finishedPath = new ConfigDef.Recommender() {
      @Override
      public List<Object> validValues(String s, Map<String, Object> map) {
        return ImmutableList.of();
      }

      @Override
      public boolean visible(String s, Map<String, Object> map) {
        if (!FINISHED_PATH_CONFIG.equals(s)) {
          return true;
        }

        final String cleanupPolicy = (String) map.get(CLEANUP_POLICY_CONF);
        return CleanupPolicy.MOVE.toString().equals(cleanupPolicy);
      }
    };


    return new ConfigDef()


        .define(
            ConfigKeyBuilder.of(TOPIC_CONF, Type.STRING)
                .documentation(TOPIC_DOC)
                .group(GROUP_GENERAL)
                .importance(ConfigDef.Importance.HIGH)
                .build()
        ).define(
            ConfigKeyBuilder.of(BATCH_SIZE_CONF, Type.INT)
                .documentation(BATCH_SIZE_DOC)
                .importance(ConfigDef.Importance.LOW)
                .defaultValue(BATCH_SIZE_DEFAULT)
                .group(GROUP_GENERAL)
                .build()
        ).define(
            ConfigKeyBuilder.of(EMPTY_POLL_WAIT_MS_CONF, Type.LONG)
                .documentation(EMPTY_POLL_WAIT_MS_DOC)
                .importance(ConfigDef.Importance.LOW)
                .defaultValue(250L)
                .validator(ConfigDef.Range.between(1L, Long.MAX_VALUE))
                .group(GROUP_GENERAL)
                .build()
        )
        .define(
            ConfigKeyBuilder.of(CLEANUP_POLICY_CONF, ConfigDef.Type.STRING)
                .documentation(CLEANUP_POLICY_DOC)
                .importance(ConfigDef.Importance.MEDIUM)
                .validator(ValidEnum.of(CleanupPolicy.class))
                .defaultValue(CleanupPolicy.MOVE.toString())
                .group(GROUP_FILESYSTEM)
                .build()
        )
        // Filesystem
        .define(
            ConfigKeyBuilder.of(INPUT_PATH_CONFIG, ConfigDef.Type.STRING)
                .documentation(INPUT_PATH_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .validator(ValidDirectoryWritable.of())
                .group(GROUP_FILESYSTEM)
                .build()
        ).define(
            ConfigKeyBuilder.of(FINISHED_PATH_CONFIG, ConfigDef.Type.STRING)
                .documentation(FINISHED_PATH_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .defaultValue("")
                .recommender(finishedPath)
                .group(GROUP_FILESYSTEM)
                .build()
        ).define(
            ConfigKeyBuilder.of(ERROR_PATH_CONFIG, ConfigDef.Type.STRING)
                .documentation(ERROR_PATH_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .validator(ValidDirectoryWritable.of())
                .group(GROUP_FILESYSTEM)
                .build()
        ).define(
            ConfigKeyBuilder.of(INPUT_FILE_PATTERN_CONF, ConfigDef.Type.STRING)
                .documentation(INPUT_FILE_PATTERN_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .group(GROUP_FILESYSTEM)
                .build()
        ).define(
            ConfigKeyBuilder.of(HALT_ON_ERROR_CONF, Type.BOOLEAN)
                .documentation(HALT_ON_ERROR_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .defaultValue(true)
                .group(GROUP_FILESYSTEM)
                .build()
        ).define(
            ConfigKeyBuilder.of(FILE_MINIMUM_AGE_MS_CONF, Type.LONG)
                .documentation(FILE_MINIMUM_AGE_MS_DOC)
                .importance(ConfigDef.Importance.LOW)
                .group(GROUP_FILESYSTEM)
                .defaultValue(0L)
                .validator(ConfigDef.Range.atLeast(0L))
                .build()
        ).define(
            ConfigKeyBuilder.of(PROCESSING_FILE_EXTENSION_CONF, Type.STRING)
                .documentation(PROCESSING_FILE_EXTENSION_DOC)
                .importance(ConfigDef.Importance.LOW)
                .validator(ValidDirectoryWritable.of())
                .group(GROUP_FILESYSTEM)
                .defaultValue(PROCESSING_FILE_EXTENSION_DEFAULT)
                .validator(ValidPattern.of("^.*\\..+$"))
                .build()
        ).define(
            ConfigKeyBuilder.of(ERROR_FILE_EXTENSION_CONF, Type.STRING)
                .documentation(ERROR_FILE_EXTENSION_DOC)
                .importance(ConfigDef.Importance.LOW)
                .validator(ValidDirectoryWritable.of())
                .group(GROUP_FILESYSTEM)
                .defaultValue(ERROR_FILE_EXTENSION_DEFAULT)
                .validator(ValidPattern.of("^.*\\..+$"))
                .build()
        )

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
        )

        .define(
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
            ConfigKeyBuilder.of(TIMESTAMP_MODE_CONF, Type.STRING)
                .documentation(TIMESTAMP_MODE_DOC)
                .importance(ConfigDef.Importance.MEDIUM)
                .group(GROUP_TIMESTAMP)
                .defaultValue(TimestampMode.PROCESS_TIME.toString())
                .validator(ValidEnum.of(TimestampMode.class))
                .build()
        ).define(
            ConfigKeyBuilder.of(TIMESTAMP_FIELD_CONF, Type.STRING)
                .documentation(TIMESTAMP_FIELD_DOC)
                .importance(ConfigDef.Importance.MEDIUM)
                .group(GROUP_TIMESTAMP)
                .defaultValue("")
                .recommender(new ConfigDef.Recommender() {
                  @Override
                  public List<Object> validValues(String key, Map<String, Object> settings) {
                    return ImmutableList.of();
                  }

                  @Override
                  public boolean visible(String key, Map<String, Object> settings) {
                    String timestampMode = (String) settings.get(TIMESTAMP_MODE_CONF);
                    return TimestampMode.FIELD.toString().equals(timestampMode);
                  }
                })
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

  public enum TimestampMode {
    FIELD,
    FILE_TIME,
    PROCESS_TIME
  }

  public enum CleanupPolicy {
    NONE,
    DELETE,
    MOVE
  }
}
