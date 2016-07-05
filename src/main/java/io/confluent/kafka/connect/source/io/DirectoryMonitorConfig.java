package io.confluent.kafka.connect.source.io;

import io.confluent.kafka.connect.source.io.processing.RecordProcessor;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class DirectoryMonitorConfig extends AbstractConfig {

  public static final String RECORD_PROCESSOR_CLASS_CONF="record.processor.class";
  static final String RECORD_PROCESSOR_CLASS_DOC="Record Processor to use.";

  public static ConfigDef getConf() {
    return new ConfigDef()
        .define(RECORD_PROCESSOR_CLASS_CONF, ConfigDef.Type.CLASS, ConfigDef.Importance.HIGH, RECORD_PROCESSOR_CLASS_DOC)
        ;
  }

  public DirectoryMonitorConfig(ConfigDef configDef, Map<?, ?> originals) {
    super(configDef, originals);
  }

  public DirectoryMonitorConfig(Map<?, ?> originals) {
    this(getConf(), originals);
  }

  Class<RecordProcessor> recordProcessor() {
    return (Class<RecordProcessor>)this.getClass(RECORD_PROCESSOR_CLASS_CONF);
  }


}
