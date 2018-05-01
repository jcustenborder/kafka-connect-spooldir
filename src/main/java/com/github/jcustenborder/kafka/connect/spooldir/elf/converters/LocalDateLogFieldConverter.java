package com.github.jcustenborder.kafka.connect.spooldir.elf.converters;

import org.apache.kafka.connect.data.Field;

import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

public class LocalDateLogFieldConverter extends LogFieldConverter {
  private static final ZoneId ZONE_ID = ZoneId.of("UTC");

  @Override
  protected Object convert(Object input) {
    final LocalDate localDate = (LocalDate) input;
    final Instant instant = localDate.atStartOfDay(ZONE_ID).toInstant();
    return Date.from(instant);
  }

  public LocalDateLogFieldConverter(String logFieldName, Field field) {
    super(logFieldName, field);
  }
}
