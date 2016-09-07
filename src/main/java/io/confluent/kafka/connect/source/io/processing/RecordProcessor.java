/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.connect.source.io.processing;

import io.confluent.kafka.connect.source.SpoolDirectoryConfig;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface RecordProcessor extends AutoCloseable {
  void configure(SpoolDirectoryConfig config,
                 InputStream inputStream,
                 FileMetadata fileMetadata) throws IOException;

  long lineNumber();

  List<SourceRecord> poll() throws IOException;
}
