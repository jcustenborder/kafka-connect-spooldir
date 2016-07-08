/**
 * Copyright (C) 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.connect.source.io.processing.line;

public class LineRecordProcessorTests {

//  LineRecordProcessor lineRecordProcessor;
//
//  @Before
//  public void setup() {
//    this.lineRecordProcessor = new LineRecordProcessor();
//  }
//
//  @Test
//  public void poll() throws IOException {
//    final String topic = "testing";
//    File tempfile = File.createTempFile(LineRecordProcessorTests.class.getSimpleName(), "test");
//    tempfile.deleteOnExit();
//    String inputFileName = Files.getNameWithoutExtension(tempfile.getName());
//
//
//    final List<String> testLines = ImmutableList.of(
//        "This is the first line",
//        "This is the second line",
//        "This is the third line",
//        "This is the fourth line",
//        "This is the fifth line",
//        "This is the sixth line",
//        "This is the seventh line",
//        "This is the eighth line",
//        "This is the ninth line",
//        "This is the tenth line"
//    );
//    Files.asCharSink(tempfile, Charset.defaultCharset(), FileWriteMode.APPEND).writeLines(testLines);
//
//    List<SourceRecord> expectedRecords = new ArrayList<>();
//    int lineNumber = 1;
//    for (String line : testLines) {
//      Struct key = new Struct(this.lineRecordProcessor.defaultKeySchema);
//      key.put(LineRecordProcessor.FIELD_FILENAME, tempfile.getName());
//      key.put(LineRecordProcessor.FIELD_LINENUMBER, lineNumber);
//
//      Map<String, ?> sourcePartition = ImmutableMap.of();
//      Map<String, ?> sourceOffset = ImmutableMap.of(inputFileName, lineNumber);
//
//      SourceRecord sourceRecord = new SourceRecord(
//          sourcePartition,
//          sourceOffset,
//          topic,
//          this.lineRecordProcessor.defaultKeySchema,
//          key,
//          Schema.STRING_SCHEMA,
//          line
//      );
//
//      expectedRecords.add(sourceRecord);
//
//      lineNumber++;
//    }
//
//    Map<String, String> settings = ImmutableMap.of(
//        SpoolDirectoryConfig.TOPIC_CONF, topic
//    );
//
//    List<SourceRecord> actualRecords;
//    try (FileInputStream inputStream = new FileInputStream(tempfile)) {
//      this.lineRecordProcessor.configure(new SpoolDirectoryConfig(settings), inputStream, inputFileName);
//      actualRecords = this.lineRecordProcessor.poll();
//    }
//
//    Assert.assertThat(actualRecords, IsEqual.equalTo(expectedRecords));
//  }

}
