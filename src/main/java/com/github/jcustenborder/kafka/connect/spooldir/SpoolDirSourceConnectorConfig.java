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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.ValidEnum;
import com.github.jcustenborder.kafka.connect.utils.config.ValidPattern;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import com.google.common.base.Strings;
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
abstract class SpoolDirSourceConnectorConfig extends AbstractConfig {
  private static final Logger log = LoggerFactory.getLogger(SpoolDirSourceConnectorConfig.class);

  public enum TimestampMode {
    FIELD,
    FILE_TIME,
    PROCESS_TIME
  }

  public static final String TIMESTAMP_FIELD_CONF = "timestamp.field";
  static final String TIMESTAMP_FIELD_DOC = "The field in the value schema that will contain the parsed timestamp for the record. " +
      "This field cannot be marked as optional and must be a " +
      "[Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html)";

  public static final String TIMESTAMP_MODE_CONF = "timestamp.mode";
  static final String TIMESTAMP_MODE_DOC = "Determines how the connector will set the timestamp for the " +
      "[ConnectRecord](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/connector/ConnectRecord.html#timestamp()). " +
      "If set to `Field` then the timestamp will be read from a field in the value. This field cannot be optional and must be " +
      "a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html). Specify the field " +
      " in `" + TIMESTAMP_FIELD_CONF + "`. " +
      "If set to `FILE_TIME` then " +
      "the last modified time of the file will be used. If set to `PROCESS_TIME` the time the record is read will be used.";

  //DirectoryMonitorConfig
  //PollingDirectoryMonitorConfig
  public static final String INPUT_PATH_CONFIG = "input.path";
  public static final String FINISHED_PATH_CONFIG = "finished.path";
  public static final String ERROR_PATH_CONFIG = "error.path";
  public static final String INPUT_FILE_PATTERN_CONF = "input.file.pattern";
  public static final String HALT_ON_ERROR_CONF = "halt.on.error";
  public static final String FILE_MINIMUM_AGE_MS_CONF = "file.minimum.age.ms";
  public static final String PROCESSING_FILE_EXTENSION_CONF = "processing.file.extension";
  //RecordProcessorConfig
  public static final String BATCH_SIZE_CONF = "batch.size";
  static final String BATCH_SIZE_DOC = "The number of records that should be returned with each batch.";
  static final int BATCH_SIZE_DEFAULT = 1000;
  public static final String PROCESSING_FILE_EXTENSION_DEFAULT = ".PROCESSING";
  public static final String TOPIC_CONF = "topic";
  static final String TOPIC_DOC = "The Kafka topic to write the data to.";

  public static final String KEY_SCHEMA_CONF = "key.schema";
  public static final String VALUE_SCHEMA_CONF = "value.schema";

  public static final String PARSER_TIMESTAMP_DATE_FORMATS_CONF = "parser.timestamp.date.formats";
  public static final String PARSER_TIMESTAMP_TIMEZONE_CONF = "parser.timestamp.timezone";

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

  static final String PARSER_TIMESTAMP_DATE_FORMATS_DOC = "The date formats that are expected in the file. This is a list " +
      "of strings that will be used to parse the date fields in order. The most accurate date format should be the first " +
      "in the list. Take a look at the Java documentation for more info. " +
      "https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html";
  static final List<String> PARSER_TIMESTAMP_DATE_FORMATS_DEFAULT = Arrays.asList("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd' 'HH:mm:ss");
  static final String PARSER_TIMESTAMP_TIMEZONE_DOC = "The timezone that all of the dates will be parsed with.";
  static final String PARSER_TIMESTAMP_TIMEZONE_DEFAULT = "UTC";

  public static final String EMPTY_POLL_WAIT_MS_CONF = "empty.poll.wait.ms";
  static final String EMPTY_POLL_WAIT_MS_DOC = "The amount of time to wait if a poll returns an empty list of records.";

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
  public boolean hasKeyMetadataField;
  public final Field valueMetadataField;
  public boolean hasvalueMetadataField;
  public final SimpleDateFormat[] parserTimestampDateFormats;
  public final TimeZone parserTimestampTimezone;
  public final long emptyPollWaitMs;
  public final String processingFileExtension;
  public final TimestampMode timestampMode;
  public final String timestampField;

  public static final String METADATA_SCHEMA_NAME = "com.github.jcustenborder.kafka.connect.spooldir.Metadata";

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

  public SpoolDirSourceConnectorConfig(ConfigDef configDef, Map<String, ?> settings) {
    super(configDef, settings);
    this.inputPath = ConfigUtils.getAbsoluteFile(this, INPUT_PATH_CONFIG);
    this.finishedPath = ConfigUtils.getAbsoluteFile(this, FINISHED_PATH_CONFIG);
    this.errorPath = ConfigUtils.getAbsoluteFile(this, ERROR_PATH_CONFIG);
    this.haltOnError = this.getBoolean(HALT_ON_ERROR_CONF);
    this.minimumFileAgeMS = this.getLong(FILE_MINIMUM_AGE_MS_CONF);
    this.batchSize = this.getInt(BATCH_SIZE_CONF);
    this.topic = this.getString(TOPIC_CONF);
    this.emptyPollWaitMs = this.getLong(EMPTY_POLL_WAIT_MS_CONF);
    this.processingFileExtension = this.getString(PROCESSING_FILE_EXTENSION_CONF);

    this.keySchema = readSchema(KEY_SCHEMA_CONF);
    this.valueSchema = readSchema(VALUE_SCHEMA_CONF);

    String timestampTimezone = this.getString(PARSER_TIMESTAMP_TIMEZONE_CONF);

    List<SimpleDateFormat> results = new ArrayList<>();
    List<String> formats = this.getList(PARSER_TIMESTAMP_DATE_FORMATS_CONF);
    for (String s : formats) {
      results.add(new SimpleDateFormat(s));
    }
    this.parserTimestampDateFormats = results.toArray(new SimpleDateFormat[results.size()]);
    this.parserTimestampTimezone = TimeZone.getTimeZone(timestampTimezone);

    this.keyMetadataField = findMetadataField(this.keySchema);
    this.hasKeyMetadataField = null != this.keyMetadataField;
    this.valueMetadataField = findMetadataField(this.valueSchema);
    this.hasvalueMetadataField = null != this.valueMetadataField;
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
  }

  public static ConfigDef config() {


    return new ConfigDef()
        //PollingDirectoryMonitorConfig
        .define(INPUT_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, INPUT_PATH_DOC)
        .define(FINISHED_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, FINISHED_PATH_DOC)
        .define(ERROR_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, ERROR_PATH_DOC)
        .define(INPUT_FILE_PATTERN_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, INPUT_FILE_PATTERN_DOC)
        .define(HALT_ON_ERROR_CONF, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH, HALT_ON_ERROR_DOC)
        .define(FILE_MINIMUM_AGE_MS_CONF, ConfigDef.Type.LONG, 0L, ConfigDef.Range.between(0L, Long.MAX_VALUE), ConfigDef.Importance.LOW, FILE_MINIMUM_AGE_MS_DOC)
        .define(PROCESSING_FILE_EXTENSION_CONF, ConfigDef.Type.STRING, PROCESSING_FILE_EXTENSION_DEFAULT, ValidPattern.of("^.*\\..+$"), ConfigDef.Importance.LOW, PROCESSING_FILE_EXTENSION_DOC)

        .define(BATCH_SIZE_CONF, ConfigDef.Type.INT, BATCH_SIZE_DEFAULT, ConfigDef.Importance.LOW, BATCH_SIZE_DOC)
        .define(TOPIC_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TOPIC_DOC)

        .define(KEY_SCHEMA_CONF, Type.STRING, ConfigDef.Importance.HIGH, KEY_SCHEMA_DOC)
        .define(VALUE_SCHEMA_CONF, Type.STRING, ConfigDef.Importance.HIGH, VALUE_SCHEMA_DOC)
        .define(PARSER_TIMESTAMP_TIMEZONE_CONF, ConfigDef.Type.STRING, PARSER_TIMESTAMP_TIMEZONE_DEFAULT, ConfigDef.Importance.LOW, PARSER_TIMESTAMP_TIMEZONE_DOC)
        .define(PARSER_TIMESTAMP_DATE_FORMATS_CONF, ConfigDef.Type.LIST, PARSER_TIMESTAMP_DATE_FORMATS_DEFAULT, ConfigDef.Importance.LOW, PARSER_TIMESTAMP_DATE_FORMATS_DOC)

        .define(EMPTY_POLL_WAIT_MS_CONF, ConfigDef.Type.LONG, 1000L, ConfigDef.Range.between(1L, Long.MAX_VALUE), ConfigDef.Importance.LOW, EMPTY_POLL_WAIT_MS_DOC)
        .define(TIMESTAMP_MODE_CONF, Type.STRING, TimestampMode.PROCESS_TIME.toString(), ValidEnum.of(TimestampMode.class), ConfigDef.Importance.MEDIUM, TIMESTAMP_MODE_DOC)
        .define(TIMESTAMP_FIELD_CONF, Type.STRING, "", ConfigDef.Importance.MEDIUM, TIMESTAMP_FIELD_DOC);
  }

  Schema readSchema(final String key) {
    String schema = this.getString(key);
    try {
      return ObjectMapperFactory.INSTANCE.readValue(schema, Schema.class);
    } catch (IOException e) {
      throw new DataException("Could not read schema from '" + key + "'", e);
    }
  }

  public PatternFilenameFilter inputFilePattern() {
    String input = this.getString(INPUT_FILE_PATTERN_CONF);
    Pattern pattern = Pattern.compile(input);
    return new PatternFilenameFilter(pattern);
  }
}
