package com.github.jcustenborder.kafka.connect.spooldir;

import com.google.common.io.Files;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

public class SchemaGeneratorTest {
  protected Map<String, String> settings;
  File tempRoot;
  File inputPath;
  File finishedPath;
  File errorPath;

  @BeforeEach
  public void createTempDir() {
    this.tempRoot = Files.createTempDir();
    this.inputPath = new File(this.tempRoot, "input");
    this.inputPath.mkdirs();
    this.finishedPath = new File(this.tempRoot, "finished");
    this.finishedPath.mkdirs();
    this.errorPath = new File(this.tempRoot, "error");
    this.errorPath.mkdirs();

    this.settings = new LinkedHashMap<>();
    this.settings.put(SpoolDirSourceConnectorConfig.INPUT_PATH_CONFIG, this.inputPath.getAbsolutePath());
    this.settings.put(SpoolDirSourceConnectorConfig.FINISHED_PATH_CONFIG, this.finishedPath.getAbsolutePath());
    this.settings.put(SpoolDirSourceConnectorConfig.ERROR_PATH_CONFIG, this.errorPath.getAbsolutePath());
    this.settings.put(SpoolDirSourceConnectorConfig.TOPIC_CONF, "dummy");
  }

}
