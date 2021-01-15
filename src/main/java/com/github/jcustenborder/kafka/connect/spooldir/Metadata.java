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

import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;

import java.io.File;
import java.util.Date;

/**
 * Class is used to write metadata for the InputFile.
 */
class Metadata {
  static final String HEADER_PATH = "file.path";
  static final String HEADER_NAME = "file.name";
  static final String HEADER_LAST_MODIFIED = "file.last.modified";
  static final String HEADER_LENGTH = "file.length";
  static final String HEADER_OFFSET = "file.offset";

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
}
