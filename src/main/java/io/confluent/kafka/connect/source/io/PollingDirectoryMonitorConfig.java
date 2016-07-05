package io.confluent.kafka.connect.source.io;

import com.google.common.base.Preconditions;
import com.google.common.io.PatternFilenameFilter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Map;
import java.util.regex.Pattern;

public class PollingDirectoryMonitorConfig extends DirectoryMonitorConfig {

  public static final String INPUT_PATH_CONFIG = "input.path";
  static final String INPUT_PATH_DOC = "Input path to read files from.";

  public static final String FINISHED_PATH_CONFIG = "finished.path";
  static final String FINISHED_PATH_DOC = "The directory to place finished";

  public static final String ERROR_PATH_CONFIG = "error.path";
  static final String ERROR_PATH_DOC = "The directory to place files that have error(s).";

  public static final String INPUT_FILE_PATTERN_CONF="input.file.pattern";
  static final String INPUT_FILE_PATTERN_DOC="Regular expression to check input file names against.";

  public static ConfigDef getConf() {
    return DirectoryMonitorConfig.getConf()
        .define(INPUT_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, INPUT_PATH_DOC)
        .define(FINISHED_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, FINISHED_PATH_DOC)
        .define(ERROR_PATH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, ERROR_PATH_DOC)
        .define(INPUT_FILE_PATTERN_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, INPUT_FILE_PATTERN_DOC)
        ;
  }

  public PollingDirectoryMonitorConfig(Map<?, ?> originals) {
    super(getConf(), originals);
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

  public File errorPath(){
    return getFile(ERROR_PATH_CONFIG);
  }

  public FilenameFilter inputFilePattern() {
    String input = this.getString(INPUT_FILE_PATTERN_CONF);
    Pattern pattern = Pattern.compile(input);
    return new PatternFilenameFilter(pattern);
  }

}

