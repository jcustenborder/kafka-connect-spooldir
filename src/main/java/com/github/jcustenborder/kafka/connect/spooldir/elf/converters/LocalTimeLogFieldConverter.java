package com.github.jcustenborder.kafka.connect.spooldir.elf.converters;

import org.apache.kafka.connect.data.Field;

import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;

public class LocalTimeLogFieldConverter extends LogFieldConverter {
  private static final LocalDate EPOCH_DATE = LocalDate.ofEpochDay(0);

  @Override
  protected Object convert(Object input) {
    final LocalTime localTime = (LocalTime) input;
    final Instant instant = localTime.atDate(EPOCH_DATE).toInstant(ZoneOffset.UTC);
    return Date.from(instant);
  }

  public LocalTimeLogFieldConverter(String logFieldName, Field field) {
    super(logFieldName, field);
  }
}
