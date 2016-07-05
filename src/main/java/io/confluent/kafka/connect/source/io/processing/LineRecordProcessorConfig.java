package io.confluent.kafka.connect.source.io.processing;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class LineRecordProcessorConfig extends RecordProcessorConfig {

  public LineRecordProcessorConfig(Map<?, ?> originals) {
    super(getConf(), originals);
  }

  public static ConfigDef getConf() {
    return RecordProcessorConfig.getConf();
  }
}
