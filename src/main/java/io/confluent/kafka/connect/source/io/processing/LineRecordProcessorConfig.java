package io.confluent.kafka.connect.source.io.processing;

import org.apache.kafka.common.config.ConfigDef;

import java.nio.charset.Charset;
import java.util.Map;

public class LineRecordProcessorConfig extends RecordProcessorConfig {

  public static final String CHARSET_CONF = "charset";
  static final String CHARSET_DOC = "Character set to read wth file with.";
  static final String CHARSET_DEFAULT = Charset.defaultCharset().name();


  public LineRecordProcessorConfig(Map<?, ?> originals) {
    super(getConf(), originals);
  }

  public static ConfigDef getConf() {
    return RecordProcessorConfig.getConf()
        .define(CHARSET_CONF, ConfigDef.Type.STRING, CHARSET_DEFAULT, ConfigDef.Importance.MEDIUM, CHARSET_DOC)
        ;
  }

  public Charset charset() {
    String value = this.getString(CHARSET_CONF);
    Charset charset = Charset.forName(value);
    return charset;
  }
}
