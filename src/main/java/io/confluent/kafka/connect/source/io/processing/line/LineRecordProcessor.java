/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.connect.source.io.processing.line;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.connect.source.SpoolDirectoryConfig;
import io.confluent.kafka.connect.source.io.processing.FileMetadata;
import io.confluent.kafka.connect.source.io.processing.RecordProcessor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Record processor reads file line by line.
 */
public class LineRecordProcessor implements RecordProcessor {
  final static String FIELD_FILENAME = "filename";
  final static String FIELD_LINENUMBER = "linenumber";
  final Schema defaultKeySchema;
  private InputStream inputStream;
  private InputStreamReader inputStreamReader;
  private LineNumberReader lineNumberReader;
  private SpoolDirectoryConfig config;
  private FileMetadata fileMetadata;

  public LineRecordProcessor() {
    this.defaultKeySchema = SchemaBuilder.struct()
        .field(FIELD_FILENAME, Schema.STRING_SCHEMA)
        .field(FIELD_LINENUMBER, Schema.INT32_SCHEMA)
        .build();
  }

  @Override
  public void configure(SpoolDirectoryConfig config, InputStream inputStream, FileMetadata fileMetadata) {
    this.config = config;
    this.inputStream = inputStream;
    this.fileMetadata = fileMetadata;
    this.inputStreamReader = new InputStreamReader(this.inputStream, this.config.charset());
    this.lineNumberReader = new LineNumberReader(this.inputStreamReader);
  }

  @Override
  public long lineNumber() {
    return this.lineNumberReader.getLineNumber();
  }


  private SourceRecord createSourceRecord(
      int lineNumber,
      Map<String, ?> sourcePartition,
      Map<String, ?> sourceOffset,
      String topic,
      String line) {

    Struct value = new Struct(this.defaultKeySchema);
    value.put(FIELD_FILENAME, this.fileMetadata.fileName());
    value.put(FIELD_LINENUMBER, lineNumber);

    return new SourceRecord(
        sourcePartition,
        sourceOffset,
        topic,
        this.defaultKeySchema,
        value,
        Schema.STRING_SCHEMA,
        line
    );
  }

  private Map<String, ?> getSourceOffset(int lineNumber) {
    return ImmutableMap.of(this.fileMetadata.fileName(), lineNumber);
  }


  @Override
  public List<SourceRecord> poll() throws IOException {
    List<SourceRecord> sourceRecords = new ArrayList<>();

    String line;

    Map<String, ?> sourcePartitions = ImmutableMap.of();
    while ((line = this.lineNumberReader.readLine()) != null) {
      int lineNumber = this.lineNumberReader.getLineNumber();
      Map<String, ?> sourceOffsets = getSourceOffset(lineNumber);
      SourceRecord sourceRecord = createSourceRecord(
          lineNumber,
          sourcePartitions,
          sourceOffsets,
          this.config.topic(),
          line
      );
      sourceRecords.add(sourceRecord);
    }

    return sourceRecords;
  }

  @Override
  public void close() throws Exception {
    this.inputStream.close();
    this.inputStreamReader.close();
    this.lineNumberReader.close();
  }
}
