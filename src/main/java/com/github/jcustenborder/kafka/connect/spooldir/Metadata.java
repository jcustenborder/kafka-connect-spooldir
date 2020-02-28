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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;

import java.io.File;
import java.util.Date;

/**
 * Class is used to write metadata for the InputFile.
 */
class Metadata {
  public static final String METADATA_SCHEMA_NAME = "com.github.jcustenborder.kafka.connect.spooldir.Metadata";
  static final String HEADER_PATH = "file.path";
  static final String HEADER_NAME = "file.name";
  static final String HEADER_LAST_MODIFIED = "file.last.modified";
  static final String HEADER_LENGTH = "file.length";
  static final String HEADER_OFFSET = "file.offset";

  static final String FIELD_PATH = "path";
  static final String FIELD_NAME = "name";
  static final String FIELD_LAST_MODIFIED = "last_modified";
  static final String FIELD_LENGTH = "length";
  static final String FIELD_OFFSET = "offset";
  static final Schema METADATA_SCHEMA;

  final String path;
  final String name;
  final Date lastModified;
  final long length;

  public Metadata(File file) {
    this.path = file.getAbsolutePath();
    this.name = file.getName();
    this.lastModified = new Date(file.lastModified());
    this.length = file.length();
  }

  static {
    METADATA_SCHEMA = SchemaBuilder.struct()
        .name(METADATA_SCHEMA_NAME)
        .optional()
        .field(FIELD_NAME, SchemaBuilder.string().doc("Name of the file.").build())
        .field(FIELD_PATH, SchemaBuilder.string().doc("Absolute path of the file.").build())
        .field(FIELD_LAST_MODIFIED, Timestamp.builder().doc("Absolute path of the file.").build())
        .field(FIELD_LENGTH, SchemaBuilder.int64().doc("Length of the file.").build())
        .field(FIELD_OFFSET, SchemaBuilder.int64().doc("Position of the record in the file.").build())
        .build();
  }

  /**
   * Method is used to copy metadata from the file to the headers of the file.
   *
   * @return Returns a Headers object populated with the metadata from the file.
   */
  public Headers headers(long offset) {
    ConnectHeaders headers = new ConnectHeaders();
    headers.addString(HEADER_NAME, this.name);
    headers.addString(HEADER_PATH, this.path);
    headers.addLong(HEADER_LENGTH, this.length);
    headers.addLong(HEADER_OFFSET, offset);
    headers.addTimestamp(HEADER_LAST_MODIFIED, this.lastModified);
    return headers;
  }

  /**
   * Method is used to copy the metadata from the headers to a struct.
   *
   * @return Struct containing the metadata from the file processed.
   */
  public Struct struct(long offset) {

    Struct result = new Struct(METADATA_SCHEMA);
    result.put(FIELD_NAME, this.name);
    result.put(FIELD_PATH, this.path);
    result.put(FIELD_LAST_MODIFIED, this.lastModified);
    result.put(FIELD_LENGTH, this.length);
    result.put(HEADER_OFFSET, offset);
    return result;
  }
}
