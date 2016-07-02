package io.confluent.kafka.connect.source.io;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class LineRecordProcessorConfig extends RecordProcessorConfig {

  public static ConfigDef getConf(){
    return RecordProcessorConfig.getConf();
  }

  public LineRecordProcessorConfig(Map<?, ?> originals) {
    super(getConf(), originals);
  }
}
