package io.confluent.kafka.connect.source.io.processing;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.connect.source.io.processing.CSVRecordProcessor;
import io.confluent.kafka.connect.source.io.processing.CSVRecordProcessorConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class CSVRecordProcessorTests {

  CSVRecordProcessorConfig csvRecordProcessorConfig;
  CSVRecordProcessor csvRecordProcessor;

  @Before
  public void setup(){
    this.csvRecordProcessor = new CSVRecordProcessor();
  }

  @Test
  public void poll() throws IOException {
    Map<String, String> configs = ImmutableMap.of(
        CSVRecordProcessorConfig.TOPIC_CONF, "csv",
        CSVRecordProcessorConfig.KEY_FIELDS_CONF, "ID",
        CSVRecordProcessorConfig.FIRST_ROW_AS_HEADER_CONF, "true"
    );
    this.csvRecordProcessorConfig = new CSVRecordProcessorConfig(configs);

    final String fileName="Testing";

    try(InputStream inputStream = this.getClass().getResourceAsStream("MOCK_DATA.csv")){
      this.csvRecordProcessor.configure(configs, inputStream, fileName);

      List<SourceRecord> results = this.csvRecordProcessor.poll();
      Assert.assertNotNull(results);


    }

  }



}
