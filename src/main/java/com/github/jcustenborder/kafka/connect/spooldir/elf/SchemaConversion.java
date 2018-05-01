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

    return new ImmutablePair<>(key, value);
  }
}
