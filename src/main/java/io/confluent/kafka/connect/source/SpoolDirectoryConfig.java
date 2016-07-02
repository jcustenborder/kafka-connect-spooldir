package io.confluent.kafka.connect.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class SpoolDirectoryConfig extends AbstractConfig {

  private static final String INPUT_GROUP="input";
  public static final String INPUT_DIRECTORY_PATH_CONFIG="input.directory.path";
  public static final String INPUT_DIRECTORY_PATH_DOC="The input directory the connector should read from.";

  public static final String INPUT_FILE_MASK_CONFIG="input.mask";
  public static final String INPUT_FILE_MASK_DOC="The file mask of the input file set.";
  public static final List<String> INPUT_FILE_MASK_DEFAULT= Arrays.asList("*.txt");


  public static final String COMPLETE_DIRECTORY_PATH_CONFIG="complete.directory.path";
  public static final String COMPLETE_DIRECTORY_PATH_DOC="The directory completed files should me moved to.";

  public static final String COMPLETE_FILE_EXTENSION_CONFIG="complete.extension";
  public static final String COMPLETE_FILE_EXTENSION_DOC="The file extension to append to a file that has been processed.";
  public static final String COMPLETE_FILE_EXTENSION_DEFAULT=".COMPLETED";

  public static final String PROCESSING_FILE_EXTENSION_CONFIG="processing.extension";
  public static final String PROCESSING_FILE_EXTENSION_DOC="The file extension to append to a file while it is being processed.";
  public static final String PROCESSING_FILE_EXTENSION_DEFAULT=".PROCESSING";


  public SpoolDirectoryConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig);
  }

  public SpoolDirectoryConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  public static ConfigDef conf() {
    return new ConfigDef()
        .define(INPUT_DIRECTORY_PATH_CONFIG, Type.STRING, Importance.HIGH, INPUT_DIRECTORY_PATH_DOC, INPUT_GROUP, 0, ConfigDef.Width.LONG, "Input")
        .define(INPUT_FILE_MASK_CONFIG, Type.LIST, INPUT_FILE_MASK_DEFAULT, Importance.HIGH,INPUT_FILE_MASK_DOC, INPUT_GROUP, 1, ConfigDef.Width.LONG, "Input")

        ;


  }

}
