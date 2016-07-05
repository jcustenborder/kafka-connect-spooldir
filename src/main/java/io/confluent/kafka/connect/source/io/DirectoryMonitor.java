package io.confluent.kafka.connect.source.io;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Map;

public interface DirectoryMonitor {
  void configure(Map<?, ?> config);
  List<SourceRecord> poll();
}
