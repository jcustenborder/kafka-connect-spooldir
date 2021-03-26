package com.github.jcustenborder.kafka.connect.spooldir;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SpoolDirLineDelimitedSourceTaskTest extends AbstractSpoolDirSourceTaskTest<SpoolDirLineDelimitedSourceTask> {
  private static final Logger log = LoggerFactory.getLogger(SpoolDirJsonSourceTaskTest.class);

  @Override
  protected SpoolDirLineDelimitedSourceTask createTask() {
    return new SpoolDirLineDelimitedSourceTask();
  }

  @Override
  protected Map<String, String> settings() {
    Map<String, String> settings = super.settings();
    settings.put(SpoolDirCsvSourceConnectorConfig.CSV_FIRST_ROW_AS_HEADER_CONF, "true");
    settings.put(SpoolDirCsvSourceConnectorConfig.PARSER_TIMESTAMP_DATE_FORMATS_CONF, "yyyy-MM-dd'T'HH:mm:ss'Z'");
    settings.put(SpoolDirCsvSourceConnectorConfig.CSV_NULL_FIELD_INDICATOR_CONF, "BOTH");
    return settings;
  }

}
