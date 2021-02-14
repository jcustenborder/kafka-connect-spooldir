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

import shaded.com.google.common.collect.ImmutableMap;
import shaded.com.google.common.io.Files;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;

import java.io.File;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class is used to write metadata for the InputFile.
 */
class Metadata {
  static final String HEADER_PATH = "file.path";
  static final String HEADER_NAME = "file.name";
  static final String HEADER_NAME_WITHOUT_EXTENSION = "file.name.without.extension";
  static final String HEADER_LAST_MODIFIED = "file.last.modified";
  static final String HEADER_LENGTH = "file.length";
  static final String HEADER_OFFSET = "file.offset";

  final String path;
  final String name;
  final String nameWithoutExtension;
  final Date lastModified;
  final long length;

  public static final Map<String, String> HEADER_DESCRIPTIONS;

  static {
    Map<String, String> result = new LinkedHashMap<>();
    result.put(HEADER_PATH, "The absolute path to the file ingested.");
    result.put(HEADER_NAME, "The name part of the file ingested.");
    result.put(HEADER_NAME_WITHOUT_EXTENSION, "The file name without the extension part of the file.");
    result.put(HEADER_LAST_MODIFIED, "The last modified date of the file.");
    result.put(HEADER_LENGTH, "The size of the file in bytes.");
    result.put(HEADER_OFFSET, "The offset for this piece of data within the file.");
    HEADER_DESCRIPTIONS = ImmutableMap.copyOf(result);
  }

  public static final String HEADER_DOCS;

  static {
    StringBuilder builder = new StringBuilder();

    HEADER_DESCRIPTIONS.forEach((key, value) -> {
      builder.append("* `");
      builder.append(key);
      builder.append("` - ");
      builder.append(value);
      builder.append('\n');
    });
    HEADER_DOCS = builder.toString();
  }



  public Metadata(File file) {
    this.path = file.getAbsolutePath();
    this.name = file.getName();
    this.lastModified = new Date(file.lastModified());
    this.length = file.length();
    this.nameWithoutExtension = Files.getNameWithoutExtension(this.name);
  }

  /**
   * Method is used to copy metadata from the file to the headers of the file.
   *
   * @return Returns a Headers object populated with the metadata from the file.
   */
  public Headers headers(long offset) {
    ConnectHeaders headers = new ConnectHeaders();
    headers.addString(HEADER_NAME, this.name);
    headers.addString(HEADER_NAME_WITHOUT_EXTENSION, this.nameWithoutExtension);
    headers.addString(HEADER_PATH, this.path);
    headers.addLong(HEADER_LENGTH, this.length);
    headers.addLong(HEADER_OFFSET, offset);
    headers.addTimestamp(HEADER_LAST_MODIFIED, this.lastModified);
    return headers;
  }
}
