package io.confluent.kafka.connect.source.io;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class RecordProcessorConfig extends AbstractConfig {

  public static final String BATCH_SIZE_CONF="batch.size";
  public static final String BATCH_SIZE_DOC="Number of records to return in a batch.";
  public static final int BATCH_SIZE_DEFAULT = 100;

  public static final String TOPIC_CONF="topic";
  public static final String TOPIC_DOC="Topic to write the data to.";

  
  public static ConfigDef getConf(){
    return new ConfigDef()
        .define(BATCH_SIZE_CONF, ConfigDef.Type.INT, BATCH_SIZE_DEFAULT, ConfigDef.Importance.LOW, BATCH_SIZE_DOC)
        .define(TOPIC_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TOPIC_DOC)

        ;
  }

  public RecordProcessorConfig(ConfigDef definition, Map<?, ?> originals) {
    super(definition, originals);
  }

  public int batchSize(){
    return this.getInt(BATCH_SIZE_CONF);
  }

  public String topic() {
    return this.getString(TOPIC_CONF);
  }
}
