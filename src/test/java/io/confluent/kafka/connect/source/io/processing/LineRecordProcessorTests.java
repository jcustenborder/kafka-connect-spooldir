package io.confluent.kafka.connect.source.io.processing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LineRecordProcessorTests {

  LineRecordProcessor lineRecordProcessor;

  @Before
  public void setup() {
    this.lineRecordProcessor = new LineRecordProcessor();
  }

  @Test
  public void poll() throws IOException {
    final String topic = "testing";
    File tempfile = File.createTempFile(LineRecordProcessorTests.class.getSimpleName(), "test");
    tempfile.deleteOnExit();
    String inputFileName = Files.getNameWithoutExtension(tempfile.getName());


    final List<String> testLines = ImmutableList.of(
        "This is the first line",
        "This is the second line",
        "This is the third line",
        "This is the fourth line",
        "This is the fifth line",
        "This is the sixth line",
        "This is the seventh line",
        "This is the eighth line",
        "This is the ninth line",
        "This is the tenth line"
    );
    Files.asCharSink(tempfile, Charset.defaultCharset(), FileWriteMode.APPEND).writeLines(testLines);

    List<SourceRecord> expectedRecords = new ArrayList<>();
    int lineNumber = 1;
    for (String line : testLines) {
      Struct key = new Struct(this.lineRecordProcessor.defaultKeySchema);
      key.put(LineRecordProcessor.FIELD_FILENAME, tempfile.getName());
      key.put(LineRecordProcessor.FIELD_LINENUMBER, lineNumber);

      Map<String, ?> sourcePartition = ImmutableMap.of();
      Map<String, ?> sourceOffset = ImmutableMap.of(inputFileName, lineNumber);

      SourceRecord sourceRecord = new SourceRecord(
          sourcePartition,
          sourceOffset,
          topic,
          this.lineRecordProcessor.defaultKeySchema,
          key,
          Schema.STRING_SCHEMA,
          line
      );

      expectedRecords.add(sourceRecord);

      lineNumber++;
    }

    Map<?, ?> settings = ImmutableMap.of(
        RecordProcessorConfig.TOPIC_CONF, topic
    );

    List<SourceRecord> actualRecords;
    try (FileInputStream inputStream = new FileInputStream(tempfile)) {
      this.lineRecordProcessor.configure(settings, inputStream, inputFileName);
      actualRecords = this.lineRecordProcessor.poll();
    }

    Assert.assertThat(actualRecords, IsEqual.equalTo(expectedRecords));
  }

}
