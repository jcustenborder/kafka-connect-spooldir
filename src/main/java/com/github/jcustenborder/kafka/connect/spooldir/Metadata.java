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

import java.util.Date;

public class Metadata {
  public static final String METADATA_SCHEMA_NAME = "com.github.jcustenborder.kafka.connect.spooldir.Metadata";
  static final String HEADER_PATH = "file.path";
  static final String HEADER_NAME = "file.name";
  static final String HEADER_LAST_MODIFIED = "file.last.modified";
  static final String HEADER_LENGTH = "file.length";
  static final String FIELD_PATH = "path";
  static final String FIELD_NAME = "name";
  static final String FIELD_LAST_MODIFIED = "last_modified";
  static final String FIELD_LENGTH = "length";
  static final Schema METADATA_SCHEMA;

  static {
    METADATA_SCHEMA = SchemaBuilder.struct()
        .name(METADATA_SCHEMA_NAME)
        .optional()
        .field(FIELD_NAME, SchemaBuilder.string().doc("Name of the file.").build())
        .field(FIELD_PATH, SchemaBuilder.string().doc("Absolute path of the file.").build())
        .field(FIELD_LAST_MODIFIED, Timestamp.builder().doc("Absolute path of the file.").build())
        .field(FIELD_LENGTH, SchemaBuilder.int64().doc("Length of the file.").build())
        .build();
  }

  /**
   * Method is used to copy metadata from the file to the headers of the file.
   *
   * @param file
   * @return
   */
  public static Headers headers(InputFile file) {
    ConnectHeaders headers = new ConnectHeaders();
    headers.addString(HEADER_NAME, file.getName());
    headers.addString(HEADER_PATH, file.getPath());
    headers.addLong(HEADER_LENGTH, file.length());
    headers.addTimestamp(HEADER_LAST_MODIFIED, new Date(file.lastModified()));
    return headers;
  }

  /**
   * Method is used to copy the metadata from the headers to a struct.
   *
   * @param headers
   * @return
   */
  public static Struct struct(Headers headers) {
    Struct result = new Struct(METADATA_SCHEMA);
    result.put(FIELD_NAME, headers.lastWithName(HEADER_NAME));
    result.put(FIELD_PATH, headers.lastWithName(HEADER_PATH));
    result.put(FIELD_LAST_MODIFIED, headers.lastWithName(HEADER_LAST_MODIFIED));
    result.put(FIELD_LENGTH, headers.lastWithName(HEADER_LENGTH));
    return result;
  }
}
