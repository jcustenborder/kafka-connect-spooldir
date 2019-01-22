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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.ValidEnum;
import com.github.jcustenborder.kafka.connect.utils.config.ValidPattern;
import com.github.jcustenborder.kafka.connect.utils.config.recommenders.Recommenders;
import com.github.jcustenborder.kafka.connect.utils.config.validators.filesystem.ValidDirectoryWritable;
import com.google.common.io.PatternFilenameFilter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.io.File;
import java.util.Map;
import java.util.regex.Pattern;

public abstract class AbstractSourceConnectorConfig extends AbstractConfig {
  public static final String FINISHED_PATH_CONFIG = "finished.path";
  public static final String ERROR_PATH_CONFIG = "error.path";
  public static final String INPUT_FILE_PATTERN_CONF = "input.file.pattern";
  public static final String HALT_ON_ERROR_CONF = "halt.on.error";
  public static final String FILE_MINIMUM_AGE_MS_CONF = "file.minimum.age.ms";
  public static final String PROCESSING_FILE_EXTENSION_CONF = "processing.file.extension";
  //RecordProcessorConfig
  public static final String BATCH_SIZE_CONF = "batch.size";
  public static final String PROCESSING_FILE_EXTENSION_DEFAULT = ".PROCESSING";
  public static final String TOPIC_CONF = "topic";
  public static final String EMPTY_POLL_WAIT_MS_CONF = "empty.poll.wait.ms";
  public static final String METADATA_SCHEMA_NAME = "com.github.jcustenborder.kafka.connect.spooldir.Metadata";
  public static final String CLEANUP_POLICY_CONF = "cleanup.policy";
  public static final String CLEANUP_POLICY_DOC = "Determines how the connector should cleanup the " +
      "files that have been successfully processed. NONE leaves the files in place which could " +
      "cause them to be reprocessed if the connector is restarted. DELETE removes the file from the " +
      "filesystem. MOVE will move the file to a finished directory.";
  public static final String GROUP_FILESYSTEM = "File System";
  public static final String GROUP_GENERAL = "General";
  //DirectoryMonitorConfig
  //PollingDirectoryMonitorConfig
  public static final String INPUT_PATH_CONFIG = "input.path";
  public static final String TIMESTAMP_MODE_CONF = "timestamp.mode";
  public static final String GROUP_TIMESTAMP = "Timestamps";
  static final String BATCH_SIZE_DOC = "The number of records that should be returned with each batch.";
  static final int BATCH_SIZE_DEFAULT = 1000;
  static final String TOPIC_DOC = "The Kafka topic to write the data to.";
  static final String INPUT_PATH_DOC = "The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.";
  static final String FINISHED_PATH_DOC = "The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.";
  static final String ERROR_PATH_DOC = "The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.";
  static final String INPUT_FILE_PATTERN_DOC = "Regular expression to check input file names against. This expression " +
      "must match the entire filename. The equivalent of Matcher.matches().";
  static final String HALT_ON_ERROR_DOC = "Should the task halt when it encounters an error or continue to the next file.";
  static final String FILE_MINIMUM_AGE_MS_DOC = "The amount of time in milliseconds after the file was last written to before the file can be processed.";
  static final String PROCESSING_FILE_EXTENSION_DOC = "Before a file is processed, it is renamed to indicate that it is currently being processed. This setting is appended to the end of the file.";
  static final String EMPTY_POLL_WAIT_MS_DOC = "The amount of time to wait if a poll returns an empty list of records.";
  static final String TIMESTAMP_FIELD_DOC = "The field in the value schema that will contain the parsed timestamp for the record. " +
      "This field cannot be marked as optional and must be a " +
      "[Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html)";
  static final String TIMESTAMP_MODE_DOC = "Determines how the connector will set the timestamp for the " +
      "[ConnectRecord](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/connector/ConnectRecord.html#timestamp()). " +
      "If set to `Field` then the timestamp will be read from a field in the value. This field cannot be optional and must be " +
      "a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html). Specify the field " +
      " in `" + SpoolDirSourceConnectorConfig.TIMESTAMP_FIELD_CONF + "`. " +
      "If set to `FILE_TIME` then " +
      "the last modified time of the file will be used. If set to `PROCESS_TIME` the time the record is read will be used.";


  public final File inputPath;
  public final File finishedPath;
  public final File errorPath;
  public final boolean haltOnError;
  public final long minimumFileAgeMS;
  public final int batchSize;
  public final String topic;
  public final long emptyPollWaitMs;
  public final String processingFileExtension;
  public final TimestampMode timestampMode;
  public final CleanupPolicy cleanupPolicy;
  public final PatternFilenameFilter inputFilenameFilter;

  public AbstractSourceConnectorConfig(ConfigDef definition, Map<?, ?> originals) {
    super(definition, originals);
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
    this.timestampMode = ConfigUtils.getEnum(TimestampMode.class, this, TIMESTAMP_MODE_CONF);
    final String inputPatternText = this.getString(INPUT_FILE_PATTERN_CONF);
    final Pattern inputPattern = Pattern.compile(inputPatternText);
    this.inputFilenameFilter = new PatternFilenameFilter(inputPattern);
  }

  public static ConfigDef config() {

    return new ConfigDef()
        .define(
            ConfigKeyBuilder.of(TOPIC_CONF, ConfigDef.Type.STRING)
                .documentation(TOPIC_DOC)
                .group(GROUP_GENERAL)
                .importance(ConfigDef.Importance.HIGH)
                .build()
        ).define(
            ConfigKeyBuilder.of(BATCH_SIZE_CONF, ConfigDef.Type.INT)
                .documentation(BATCH_SIZE_DOC)
                .importance(ConfigDef.Importance.LOW)
                .defaultValue(BATCH_SIZE_DEFAULT)
                .group(GROUP_GENERAL)
                .build()
        ).define(
            ConfigKeyBuilder.of(EMPTY_POLL_WAIT_MS_CONF, ConfigDef.Type.LONG)
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
                .recommender(
                    Recommenders.visibleIf(CLEANUP_POLICY_CONF, CleanupPolicy.MOVE.toString())
                )
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
            ConfigKeyBuilder.of(HALT_ON_ERROR_CONF, ConfigDef.Type.BOOLEAN)
                .documentation(HALT_ON_ERROR_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .defaultValue(true)
                .group(GROUP_FILESYSTEM)
                .build()
        ).define(
            ConfigKeyBuilder.of(FILE_MINIMUM_AGE_MS_CONF, ConfigDef.Type.LONG)
                .documentation(FILE_MINIMUM_AGE_MS_DOC)
                .importance(ConfigDef.Importance.LOW)
                .group(GROUP_FILESYSTEM)
                .defaultValue(0L)
                .validator(ConfigDef.Range.atLeast(0L))
                .build()
        ).define(
            ConfigKeyBuilder.of(PROCESSING_FILE_EXTENSION_CONF, ConfigDef.Type.STRING)
                .documentation(PROCESSING_FILE_EXTENSION_DOC)
                .importance(ConfigDef.Importance.LOW)
                .validator(ValidDirectoryWritable.of())
                .group(GROUP_FILESYSTEM)
                .defaultValue(PROCESSING_FILE_EXTENSION_DEFAULT)
                .validator(ValidPattern.of("^.*\\..+$"))
                .build()
        ).define(
            ConfigKeyBuilder.of(TIMESTAMP_MODE_CONF, ConfigDef.Type.STRING)
                .documentation(TIMESTAMP_MODE_DOC)
                .importance(ConfigDef.Importance.MEDIUM)
                .group(GROUP_TIMESTAMP)
                .defaultValue(TimestampMode.PROCESS_TIME.toString())
                .validator(ValidEnum.of(TimestampMode.class))
                .build()
        );
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
