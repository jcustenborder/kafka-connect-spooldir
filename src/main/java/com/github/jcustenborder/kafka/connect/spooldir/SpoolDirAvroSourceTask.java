/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.spooldir;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SpoolDirAvroSourceTask extends AbstractSourceTask<SpoolDirAvroSourceConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(SpoolDirAvroSourceTask.class);
  long recordOffset;
  AvroData avroData = new AvroData(1024);
  DataFileReader<GenericContainer> dataFileReader;
  DatumReader<GenericContainer> datumReader = new GenericDatumReader<>();
  Schema connectSchema;

  @Override
  protected SpoolDirAvroSourceConnectorConfig config(Map<String, ?> settings) {
    return new SpoolDirAvroSourceConnectorConfig(settings);
  }

  @Override
  protected void configure(InputFile inputFile, Long lastOffset) throws IOException {
    if (null != this.dataFileReader) {
      this.dataFileReader.close();
    }
    this.dataFileReader = new DataFileReader<GenericContainer>(inputFile.file(), datumReader);
    this.connectSchema = avroData.toConnectSchema(dataFileReader.getSchema());
    this.recordOffset = 0;

    if (null != lastOffset) {
      while (recordOffset < lastOffset && this.dataFileReader.hasNext()) {
        this.dataFileReader.next();
        recordOffset++;
      }
    }

  }

  @Override
  protected List<SourceRecord> process() throws IOException {
    int recordCount = 0;
    List<SourceRecord> records = new ArrayList<>(this.config.batchSize);
    GenericContainer container = null;
    while (recordCount <= this.config.batchSize && dataFileReader.hasNext()) {
      container = dataFileReader.next(container);
      Object value = avroData.fromConnectData(this.connectSchema, container);
      SourceRecord sourceRecord = record(null, new SchemaAndValue(this.connectSchema, value), null);
      records.add(sourceRecord);
      recordCount++;
      recordOffset++;
    }
    return records;
  }

  @Override
  protected long recordOffset() {
    return recordOffset;
  }
}
