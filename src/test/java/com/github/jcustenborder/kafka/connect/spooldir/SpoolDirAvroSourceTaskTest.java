package com.github.jcustenborder.kafka.connect.spooldir;

import com.google.common.io.Files;
import io.confluent.connect.avro.AvroData;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public class SpoolDirAvroSourceTaskTest extends AbstractSpoolDirSourceTaskTest<SpoolDirAvroSourceTask> {
  private static final Logger log = LoggerFactory.getLogger(SpoolDirJsonSourceTaskTest.class);

  @Override
  protected SpoolDirAvroSourceTask createTask() {
    return new SpoolDirAvroSourceTask();
  }

  @Override
  protected Map<String, String> settings() {
    Map<String, String> settings = super.settings();
    return settings;
  }


  @Test
  public void foo() throws IOException {

    File outputFile = new File("src/test/resources/com/github/jcustenborder/kafka/connect/spooldir/avro/FieldsMatch.data");

    DatumWriter<GenericContainer> datumWriter = new GenericDatumWriter<>();
    DataFileWriter<GenericContainer> writer = new DataFileWriter<>(datumWriter);
    writer.setCodec(CodecFactory.bzip2Codec());

    final String packageName = "csv";
    List<TestCase> testCases = loadTestCases(packageName);
    Optional<TestCase> testcase = testCases.stream().filter(testCase -> testCase.path.getFileName().endsWith("FieldsMatch.json")).findFirst();
    assertTrue(testcase.isPresent());
    AvroData avroData = new AvroData(1235);
    Schema schema = null;
    for (SourceRecord expected : testcase.get().expected) {
      if (null == schema) {
        schema = avroData.fromConnectSchema(expected.valueSchema());
        writer.create(schema, outputFile);
      }
      GenericContainer value = (GenericContainer) avroData.fromConnectData(expected.valueSchema(), expected.value());
      writer.append(value);
    }

    writer.close();
  }

  @TestFactory
  public Stream<DynamicTest> poll() throws IOException {
    final String packageName = "avro";
    List<TestCase> testCases = loadTestCases(packageName);

    return testCases.stream().map(testCase -> {
      String name = Files.getNameWithoutExtension(testCase.path.toString());
      return dynamicTest(name, () -> {
        poll(packageName, testCase);
      });
    });
  }
}
