package io.confluent.kafka.connect.source.io;

import com.google.common.collect.ImmutableMap;
import com.opencsv.CSVParserBuilder;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class CSVRecordProcessorConfigTests {

  @Test
  public void dumpConfig() {
    System.out.println(CSVRecordProcessorConfig.getConf().toRst());
  }

  @Test
  public void createCSVParserBuilder() {
    final CSVParserBuilder expected = new CSVParserBuilder();

    Map<String, String> configs = ImmutableMap.of(
        CSVRecordProcessorConfig.TOPIC_CONF, "csv",
        CSVRecordProcessorConfig.KEY_FIELDS_CONF, "ID"
    );

    CSVRecordProcessorConfig config = new CSVRecordProcessorConfig(configs);
    final CSVParserBuilder actual = config.createCSVParserBuilder();
    Assert.assertThat(actual.getEscapeChar(), IsEqual.equalTo(expected.getEscapeChar()));
    Assert.assertThat(actual.getQuoteChar(), IsEqual.equalTo(expected.getQuoteChar()));
    Assert.assertThat(actual.getSeparator(), IsEqual.equalTo(expected.getSeparator()));
    Assert.assertThat(actual.isIgnoreLeadingWhiteSpace(), IsEqual.equalTo(expected.isIgnoreLeadingWhiteSpace()));
    Assert.assertThat(actual.isIgnoreQuotations(), IsEqual.equalTo(expected.isIgnoreQuotations()));
    Assert.assertThat(actual.isStrictQuotes(), IsEqual.equalTo(expected.isStrictQuotes()));
  }

}
