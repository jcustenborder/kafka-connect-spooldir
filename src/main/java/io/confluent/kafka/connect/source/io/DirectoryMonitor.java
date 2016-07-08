package io.confluent.kafka.connect.source.io;

import io.confluent.kafka.connect.source.SpoolDirectoryConfig;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

@SuppressWarnings("WeakerAccess")
public interface DirectoryMonitor {
  void configure(SpoolDirectoryConfig config);

  List<SourceRecord> poll();
}
