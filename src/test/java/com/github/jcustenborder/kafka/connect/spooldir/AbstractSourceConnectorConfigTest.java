package com.github.jcustenborder.kafka.connect.spooldir;

import com.github.jcustenborder.kafka.connect.spooldir.FileNotifierRecordCreator.DefaultFileNotifierRecordCreator;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AbstractSourceConnectorConfigTest {

  private final Map<String, String> baseConf =
          Map.of(
                  AbstractSourceConnectorConfig.TOPIC_CONF,
                  "/tmp",
                  AbstractSourceConnectorConfig.ERROR_PATH_CONFIG,
                  "/tmp",
                  AbstractSourceConnectorConfig.INPUT_FILE_PATTERN_CONF,
                  "/tmp",
                  AbstractSourceConnectorConfig.FINISHED_PATH_CONFIG,
                  "/tmp",
                  AbstractSourceConnectorConfig.INPUT_PATH_CONFIG,
                  "/tmp"
          );

  @Test
  void should_parse_config() {
    String clazz = "java.lang.Integer";
    String topic = "testme";
    Map<String, String> conf = new HashMap<>(
            Map.of(
                    AbstractSourceConnectorConfig.FILES_NOTIFICATIONS_CONF,
                    "true",
                    AbstractSourceConnectorConfig.FILES_NOTIFICATIONS_RECORD_CREATOR_CLASS_CONF,
                    clazz,
                    AbstractSourceConnectorConfig.FILES_NOTIFICATIONS_TOPIC_CONF,
                    topic
            )
    );

    conf.putAll(
            baseConf
    );
    AbstractSourceConnectorConfig config = new AbstractSourceConnectorConfig(
            AbstractSourceConnectorConfig.config(false),
            conf,
            false
    ) {};

    assertTrue(
            config.isFilesNotificationsEnabled
    );

    assertEquals(
            config.fileNotifierRecordCreatorClass,
            clazz
    );

    assertEquals(
            config.filesNotificationsTopic,
            topic
    );

  }

  @Test
  void should_populate_default() {
    AbstractSourceConnectorConfig config = new AbstractSourceConnectorConfig(
            AbstractSourceConnectorConfig.config(false),
            baseConf,
            false
    ) {
    };

    assertFalse(
            config.isFilesNotificationsEnabled
    );
    assertEquals(
            config.fileNotifierRecordCreatorClass,
            DefaultFileNotifierRecordCreator.class.getTypeName()
    );
    assertEquals(
            config.filesNotificationsTopic,
            AbstractSourceConnectorConfig.FILES_NOTIFICATIONS_TOPIC_DEFAULT
    );
  }
}