package io.confluent.kafka.connect.source;

import com.google.common.base.Preconditions;
import io.confluent.kafka.connect.source.io.DirectoryMonitor;
import io.confluent.kafka.connect.source.io.PollingDirectoryMonitor;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;


@SuppressWarnings("WeakerAccess")
public class SpoolDirectoryConfig extends AbstractConfig {

//  public static final String INPUT_DIRECTORY_PATH_CONFIG = "input.directory.path";
//  public static final String INPUT_DIRECTORY_PATH_DOC = "The input directory the connector should read from.";
//  public static final String INPUT_FILE_MASK_CONFIG = "input.mask";
//  public static final String INPUT_FILE_MASK_DOC = "The file mask of the input file set.";
//  public static final List<String> INPUT_FILE_MASK_DEFAULT = ImmutableList.of("*.txt");
//  public static final String COMPLETE_DIRECTORY_PATH_CONFIG = "complete.directory.path";
//  public static final String COMPLETE_DIRECTORY_PATH_DOC = "The directory completed files should me moved to.";
//  public static final String COMPLETE_FILE_EXTENSION_CONFIG = "complete.extension";
//  public static final String COMPLETE_FILE_EXTENSION_DOC = "The file extension to append to a file that has been processed.";
//  public static final String COMPLETE_FILE_EXTENSION_DEFAULT = ".COMPLETED";
//  public static final String PROCESSING_FILE_EXTENSION_CONFIG = "processing.extension";
//  public static final String PROCESSING_FILE_EXTENSION_DOC = "The file extension to append to a file while it is being processed.";
//  public static final String PROCESSING_FILE_EXTENSION_DEFAULT = ".PROCESSING";
//  private static final String INPUT_GROUP = "input";

  public static final String DIRECTORY_MONITOR_CLASS_CONF = "directory.monitor.class";
  static final String DIRECTORY_MONITOR_CLASS_DOC = "Class that implements the DirectoryMonitor interface. This is used to monitor the directory for new files that have been added to the file system.";

  public SpoolDirectoryConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public SpoolDirectoryConfig(Map<String, String> parsedConfig) {
    this(getConf(), parsedConfig);
  }

  public static ConfigDef getConf() {
    return new ConfigDef()
        .define(DIRECTORY_MONITOR_CLASS_CONF, Type.CLASS, PollingDirectoryMonitor.class.getName(), Importance.HIGH, DIRECTORY_MONITOR_CLASS_DOC)
        ;


  }

  public Class<DirectoryMonitor> directoryMonitor() {
    Class<?> actual = this.getClass(DIRECTORY_MONITOR_CLASS_CONF);
    Preconditions.checkState(DirectoryMonitor.class.isAssignableFrom(actual), "%s must implement %s interface.", actual.getSimpleName(), DirectoryMonitor.class.getSimpleName());
    return (Class<DirectoryMonitor>) this.getClass(DIRECTORY_MONITOR_CLASS_CONF);
  }
}
