package io.confluent.kafka.connect.source.io;

import com.google.common.base.Preconditions;
import com.google.common.io.PatternFilenameFilter;
import org.apache.kafka.common.config.ConfigDef;

import java.io.File;
import java.util.Map;
import java.util.regex.Pattern;

@SuppressWarnings("WeakerAccess")
public class PollingDirectoryMonitorConfig extends DirectoryMonitorConfig {

  public static final String INPUT_PATH_CONFIG = "input.path";
  public static final String FINISHED_PATH_CONFIG = "finished.path";
  public static final String ERROR_PATH_CONFIG = "error.path";
  public static final String INPUT_FILE_PATTERN_CONF = "input.file.pattern";
  public static final String HALT_ON_ERROR_CONF = "halt.on.error";
  static final String INPUT_PATH_DOC = "The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.";
  static final String FINISHED_PATH_DOC = "The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.";
  static final String ERROR_PATH_DOC = "The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.";
  static final String INPUT_FILE_PATTERN_DOC = "Regular expression to check input file names against.";
  static final String HALT_ON_ERROR_DOC = "Should the task halt when it encounters an error or continue to the next file.";


  public PollingDirectoryMonitorConfig(Map<?, ?> originals) {
    super(getConf(), originals);
  }

  public static ConfigDef getConf() {
    return DirectoryMonitorConfig.getConf()
        .define(INPUT_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, INPUT_PATH_DOC)
        .define(FINISHED_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, FINISHED_PATH_DOC)
        .define(ERROR_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, ERROR_PATH_DOC)
        .define(INPUT_FILE_PATTERN_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, INPUT_FILE_PATTERN_DOC)
        .define(HALT_ON_ERROR_CONF, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH, HALT_ON_ERROR_DOC)
        ;
  }

  File getFile(String key) {
    String path = this.getString(key);
    File file = new File(path);
    Preconditions.checkState(file.isAbsolute(), "'%s' must be an absolute path.", key);
    return new File(path);
  }

  public File inputPath() {
    return getFile(INPUT_PATH_CONFIG);
  }

  public File finishedPath() {
    return getFile(FINISHED_PATH_CONFIG);
  }

  public File errorPath() {
    return getFile(ERROR_PATH_CONFIG);
  }

  public PatternFilenameFilter inputFilePattern() {
    String input = this.getString(INPUT_FILE_PATTERN_CONF);
    Pattern pattern = Pattern.compile(input);
    return new PatternFilenameFilter(pattern);
  }

  public boolean haltOnError() {
    return this.getBoolean(HALT_ON_ERROR_CONF);
  }
}

