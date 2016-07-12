/**
 * Copyright (C) 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.connect.source.io.processing;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.io.File;
import java.util.Date;
import java.util.List;

public class FileMetadata {
  final static String FIELD_LAST_MODIFIED = "last_modified";
  final static String FIELD_PROCESSED_TIME = "processed_time";
  final static String FILE_PATH = "file_path";
  final static String FIELD_SIZE = "size";
  final static String FIELD_POSITION = "position";

  final static Schema METADATA_SCHEMA;
  final static String METADATA_FIELD = "__metadata";
  final static List<Field> METADATA_SCHEMA_FIELDS;

  static {
    SchemaBuilder metadataBuilder = SchemaBuilder.struct()
        .doc("Metadata for the ingested file.")
        .name(FileMetadata.class.getName());

    metadataBuilder.field(FIELD_LAST_MODIFIED, Timestamp.builder().doc("The last modified date for the file.").build());
    metadataBuilder.field(FIELD_PROCESSED_TIME, Timestamp.builder().doc("The timestamp when the event is processed.").build());
    metadataBuilder.field(FILE_PATH, SchemaBuilder.string().doc("The path for the input file.").build());
    metadataBuilder.field(FIELD_SIZE, SchemaBuilder.int64().doc("The size of the file at ingest time.").build());
    metadataBuilder.field(FIELD_POSITION, SchemaBuilder.int64().optional().doc("The position for the event within a file.").build());

    METADATA_SCHEMA = metadataBuilder.build();
    METADATA_SCHEMA_FIELDS = new ImmutableList.Builder<Field>().addAll(METADATA_SCHEMA.fields()).build();
  }

  final Struct metadata;
  final String fileName;

  public FileMetadata(File inputFile) {
    this.fileName = Files.getNameWithoutExtension(inputFile.getName());
    this.metadata = new Struct(METADATA_SCHEMA);
    this.metadata.put(FIELD_LAST_MODIFIED, new Date(inputFile.lastModified()));
    this.metadata.put(FIELD_PROCESSED_TIME, new Date());
    this.metadata.put(FILE_PATH, inputFile.getAbsolutePath());
    this.metadata.put(FIELD_SIZE, inputFile.length());
  }

  public String fileName() {
    return this.fileName;
  }

  public void addToStruct(Struct rowStruct, Long filePosition) {
    Struct metadataStruct = new Struct(METADATA_SCHEMA);
    for (Field field : METADATA_SCHEMA_FIELDS) {
      metadataStruct.put(
          field.name(),
          this.metadata.get(field)
      );
    }
    metadataStruct.put(FIELD_POSITION, filePosition);
    rowStruct.put(METADATA_FIELD, metadataStruct);
  }

  public static void addFieldSchema(SchemaBuilder parentBuilder) {
    parentBuilder.field(METADATA_FIELD, METADATA_SCHEMA);
  }

}
