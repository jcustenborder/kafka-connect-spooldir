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
package com.github.jcustenborder.kafka.connect.spooldir.elf.converters;

import com.github.jcustenborder.parsers.elf.LogEntry;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class LogFieldConverter {
  private static final Logger log = LoggerFactory.getLogger(LogFieldConverter.class);
  protected final String logFieldName;
  protected final Field field;

  protected abstract Object convert(Object input);

  public LogFieldConverter(String logFieldName, Field field) {
    this.logFieldName = logFieldName;
    this.field = field;
  }

  public void convert(LogEntry logEntry, Struct struct) {
    final Object input = logEntry.fieldData().get(this.logFieldName);
    final Object output;
    if (null == input) {
      output = null;
    } else {
      output = convert(input);
    }

    log.trace("convert() - Setting {} to {}", field.name(), output);
    struct.put(this.field, output);
  }

}
