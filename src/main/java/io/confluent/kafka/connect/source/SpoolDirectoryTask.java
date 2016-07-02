package io.confluent.kafka.connect.source;

import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class SpoolDirectoryTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(SpoolDirectoryTask.class);

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    //TODO: Do things here that are required to start your task. This could be open a connection to a database, etc.
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {



    //TODO: Create SourceRecord objects that will be sent the kafka cluster.
    throw new UnsupportedOperationException("This has not been implemented.");
  }

  @Override
  public void stop() {
    //TODO: Do whatever is required to stop your task.
  }
}