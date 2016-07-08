package io.confluent.kafka.connect.source;

import io.confluent.kafka.connect.source.io.DirectoryMonitor;
import io.confluent.kafka.connect.source.io.PollingDirectoryMonitor;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SpoolDirectoryTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(SpoolDirectoryTask.class);
  SpoolDirectoryConfig config;
  DirectoryMonitor directoryMonitor;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    this.config = new SpoolDirectoryConfig(map);
    this.directoryMonitor = new PollingDirectoryMonitor();
    this.directoryMonitor.configure(this.config);
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> results = this.directoryMonitor.poll();

    while (results.isEmpty()) {
      Thread.sleep(1000);
      results = this.directoryMonitor.poll();
    }

    return results;
  }

  @Override
  public void stop() {

  }
}