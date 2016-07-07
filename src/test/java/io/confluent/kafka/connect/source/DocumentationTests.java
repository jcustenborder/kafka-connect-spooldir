package io.confluent.kafka.connect.source;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.connect.source.io.PollingDirectoryMonitorConfig;
import io.confluent.kafka.connect.source.io.processing.CSVRecordProcessorConfig;
import io.confluent.kafka.connect.source.io.processing.LineRecordProcessorConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.Test;

import java.util.Map;

public class DocumentationTests {

  @Test
  public void generateRST() {

    Map<Class<? extends AbstractConfig>, ConfigDef> configDefs = ImmutableMap.of(
        SpoolDirectoryConfig.class, SpoolDirectoryConfig.getConf(),
        PollingDirectoryMonitorConfig.class, PollingDirectoryMonitorConfig.getConf(),
        CSVRecordProcessorConfig.class, CSVRecordProcessorConfig.getConf(),
        LineRecordProcessorConfig.class, LineRecordProcessorConfig.getConf()
    );

    for (Map.Entry<Class<? extends AbstractConfig>, ConfigDef> entry : configDefs.entrySet()) {
//      System.out.println();
//      System.out.println();
//      System.out.println();
//      System.out.println(entry.getKey().getSimpleName());
//      System.out.println();
      System.out.println(entry.getValue().toRst());
    }

  }


}
