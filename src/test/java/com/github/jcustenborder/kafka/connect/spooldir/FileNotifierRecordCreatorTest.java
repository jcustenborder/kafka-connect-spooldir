package com.github.jcustenborder.kafka.connect.spooldir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jcustenborder.kafka.connect.spooldir.FileNotifierRecordCreator.DefaultFileNotifierRecordCreator;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FileNotifierRecordCreatorTest {

  @Test
  void should_raise_error_when_notifier_class_is_not_found() {
    assertEquals(
            ClassNotFoundException.class,
            assertThrows(
                    KafkaException.class,
                    () -> FileNotifierRecordCreator.newInstanceForClassName("com.do.not.Exist")
            ).getCause().getClass()
    );
  }

  @Test
  void should_not_provide_invalid_FileNotifierRecordCreator() {
    assertThrows(
            KafkaException.class,
            () -> FileNotifierRecordCreator.newInstanceForClassName(String.class.getTypeName())
    );
  }

  @Test
  void should_provide_notifier_impl() {
    assertEquals(
            DefaultFileNotifierRecordCreator.class,
            FileNotifierRecordCreator
                    .newInstanceForClassName(DefaultFileNotifierRecordCreator.class.getTypeName())
                    .getClass()

    );
  }

  @Test
  void should_create_start_record() throws IOException {
    checkRecord("file_start", creator -> creator::startFilePayload);
  }

  @Test
  void should_create_end_record() throws IOException {
    checkRecord("file_end", creator -> creator::endFilePayload);
  }

  private void checkRecord(String event,
                   Function<DefaultFileNotifierRecordCreator, Function<InputFile,SourceRecord>> methodTester)
          throws IOException {
    DefaultFileNotifierRecordCreator recordCreator = new DefaultFileNotifierRecordCreator();
    String topic = "test";
    recordCreator.setTopic(topic);
    InputFile mock = mock(InputFile.class);
    String expectedPath = "the path";

    when(
            mock.getPath()
    ).thenReturn(expectedPath);

    SourceRecord result = methodTester.apply(recordCreator).apply(mock);
    assertEquals(
            result.topic(),
            topic
    );
    ObjectMapper mapper = new ObjectMapper();
    JsonNode node = mapper.readValue((byte[]) result.value(), JsonNode.class);
    assertEquals(
      node.get("event").asText(),
      event
    );
    assertEquals(
            node.get("filepath").asText(),
            expectedPath
    );
  }
}