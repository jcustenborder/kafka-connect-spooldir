package com.github.jcustenborder.kafka.connect.spooldir.elf.converters;

import org.apache.kafka.connect.data.Field;

public class PrimitiveLogFieldConverter extends LogFieldConverter {
  @Override
  protected Object convert(Object input) {
    return input;
  }

  public PrimitiveLogFieldConverter(String logFieldName, Field field) {
    super(logFieldName, field);
  }
}
