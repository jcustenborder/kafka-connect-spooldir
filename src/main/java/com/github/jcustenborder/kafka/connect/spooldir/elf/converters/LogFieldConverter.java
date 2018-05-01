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
