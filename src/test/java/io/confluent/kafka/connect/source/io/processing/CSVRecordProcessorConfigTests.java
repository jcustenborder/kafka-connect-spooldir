package io.confluent.kafka.connect.source.io.processing;

import com.google.common.collect.ImmutableMap;
import com.opencsv.CSVParserBuilder;
import io.confluent.kafka.connect.source.Data;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

  void add(Map<String, String> settings, Integer index, String key, String value) {
    String mapKey = String.format("fields.%02d.%s", index, key);
    settings.put(mapKey, value);
  }

  @Test
  public void withFields() throws IOException {
    Map<String, String> settings = Data.getMockDataSettings();
    settings.put(CSVRecordProcessorConfig.TOPIC_CONF, "csv");
    settings.put(CSVRecordProcessorConfig.KEY_FIELDS_CONF, "ID");

    CSVRecordProcessorConfig config = new CSVRecordProcessorConfig(settings);

    List<CSVFieldConfig> fields = config.fields();
    Assert.assertNotNull(fields);
    Assert.assertFalse(fields.isEmpty());

    for (int i = 0; i < fields.size(); i++) {
      Assert.assertThat(fields.get(i).index, IsEqual.equalTo(i));
    }

    Properties properties = new Properties();
    properties.putAll(settings);
    properties.store(System.out, "");

  }

}
