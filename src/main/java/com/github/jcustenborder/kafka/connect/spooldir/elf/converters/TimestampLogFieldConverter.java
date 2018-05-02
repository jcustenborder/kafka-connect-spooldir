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

import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;

public class TimestampLogFieldConverter extends LogFieldConverter {
  private final String timeField;
  private final String dateField;

  public TimestampLogFieldConverter(Field field, String timeField, String dateField) {
    super(null, field);
    this.timeField = timeField;
    this.dateField = dateField;
  }

  @Override
  protected Object convert(Object input) {
    return null;
  }

  @Override
  public void convert(LogEntry logEntry, Struct struct) {
    final LocalDate date = (LocalDate) logEntry.fieldData().get(this.dateField);
    final LocalTime time = (LocalTime) logEntry.fieldData().get(this.timeField);

    final Object value;

    if (null == date || null == time) {
      value = null;
    } else {
      final Instant instant = time.atDate(date).toInstant(ZoneOffset.UTC);
      value = Date.from(instant);
    }
    struct.put(this.field, value);
  }
}
