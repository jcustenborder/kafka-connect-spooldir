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
package com.github.jcustenborder.kafka.connect.spooldir.elf;

import com.github.jcustenborder.kafka.connect.spooldir.elf.converters.LogFieldConverter;
import com.github.jcustenborder.parsers.elf.LogEntry;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SchemaConversion {
  private static final Logger log = LoggerFactory.getLogger(SchemaConversion.class);
  private final Schema keySchema;
  private final Schema valueSchema;
  private final List<LogFieldConverter> keyConverters;
  private final List<LogFieldConverter> valueConverters;

  SchemaConversion(Schema keySchema, Schema valueSchema, List<LogFieldConverter> keyConverters, List<LogFieldConverter> valueConverters) {
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.keyConverters = keyConverters;
    this.valueConverters = valueConverters;
  }


  public Pair<Struct, Struct> convert(LogEntry entry) {
    final Struct key = null != this.keySchema ? new Struct(this.keySchema) : null;
    final Struct value = new Struct(this.valueSchema);

    if (null != key) {
      for (LogFieldConverter converter : this.keyConverters) {
        converter.convert(entry, key);
      }
    }

    for (LogFieldConverter converter : this.valueConverters) {
      converter.convert(entry, value);
    }

    if (null != key) {
      key.validate();
    }
    value.validate();
    return new ImmutablePair<>(key, value);
  }
}
