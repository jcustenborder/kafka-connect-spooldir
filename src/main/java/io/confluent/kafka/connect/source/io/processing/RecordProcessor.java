package io.confluent.kafka.connect.source.io.processing;

import io.confluent.kafka.connect.source.SpoolDirectoryConfig;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface RecordProcessor extends AutoCloseable {
  void configure(SpoolDirectoryConfig config,
                 InputStream inputStream,
                 String fileName) throws IOException;

  long lineNumber();

  List<SourceRecord> poll() throws IOException;
}
