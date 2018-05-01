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
