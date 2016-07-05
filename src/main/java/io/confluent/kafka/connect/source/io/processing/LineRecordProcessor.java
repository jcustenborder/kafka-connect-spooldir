package io.confluent.kafka.connect.source.io.processing;

import com.google.common.collect.ImmutableMap;
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
  String inputFileName;
  InputStream inputStream;
  InputStreamReader inputStreamReader;
  LineNumberReader lineNumberReader;
  LineRecordProcessorConfig config;

  final Schema defaultKeySchema;

  final static String FIELD_FILENAME="filename";
  final static String FIELD_LINENUMBER="linenumber";

  public LineRecordProcessor() {
    this.defaultKeySchema = SchemaBuilder.struct()
        .field(FIELD_FILENAME, Schema.STRING_SCHEMA)
        .field(FIELD_LINENUMBER, Schema.INT32_SCHEMA)
        .build();
  }

  @Override
  public void configure(Map<?, ?> configValues, InputStream inputStream, String fileName) {
    this.inputStream = inputStream;
    this.inputFileName = fileName;
    this.inputStreamReader = new InputStreamReader(this.inputStream);
    this.lineNumberReader = new LineNumberReader(this.inputStreamReader);
    this.config = new LineRecordProcessorConfig(configValues);
  }


  public SourceRecord createSourceRecord(
      int lineNumber,
      Map<String, ?> sourcePartition,
      Map<String, ?> sourceOffset,
      String topic,
      String line) {

    Struct key = new Struct(this.defaultKeySchema);
    key.put(FIELD_FILENAME, this.inputFileName);
    key.put(FIELD_LINENUMBER, lineNumber);

    SourceRecord sourceRecord = new SourceRecord(
        sourcePartition,
        sourceOffset,
        topic,
        this.defaultKeySchema,
        key,
        Schema.STRING_SCHEMA,
        line
    );
    return sourceRecord;
  }

  Map<String, ?> getSourceOffset(int lineNumber){
    return ImmutableMap.of(this.inputFileName, lineNumber);
  }



  @Override
  public List<SourceRecord> poll() throws IOException{
    List<SourceRecord> sourceRecords = new ArrayList<>();

    String line;

    Map<String, ?> sourcePartitions = ImmutableMap.of();
    while((line = this.lineNumberReader.readLine())!=null){
      int lineNumber = this.lineNumberReader.getLineNumber();
      Map<String, ?> sourceOffsets = getSourceOffset(lineNumber);
      SourceRecord sourceRecord=createSourceRecord(
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
