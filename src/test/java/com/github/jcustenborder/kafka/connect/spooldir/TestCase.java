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

import shaded.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TestCase implements NamedTest {
  @JsonIgnore
  public Path path;
  public Map<String, String> settings = new LinkedHashMap<>();
  public Map<String, Object> offset = new LinkedHashMap<>();
  public Schema keySchema;
  public Schema valueSchema;
  public List<SourceRecord> expected;

  @Override
  public void path(Path path) {
    this.path = path;
  }
}
