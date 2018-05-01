package com.github.jcustenborder.kafka.connect.spooldir.elf.converters;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.time.LocalDate;
import java.time.LocalTime;

public class LogFieldConverterFactory {

  static Schema schema(Class<?> logClass, String logFieldName) {
    final SchemaBuilder builder;
    if (LocalDate.class.equals(logClass)) {
      builder = Date.builder();
    } else if (LocalTime.class.equals(logClass)) {
      builder = Time.builder();
    } else if (Integer.class.equals(logClass)) {
      builder = SchemaBuilder.int32();
    } else if (Long.class.equals(logClass)) {
      builder = SchemaBuilder.int64();
    } else if (String.class.equals(logClass)) {
      builder = SchemaBuilder.string();
    } else {
      throw new UnsupportedOperationException(
          String.format("%s is not a supported type.", logClass.getName())
      );
    }
    builder.optional();


    return builder.build();
  }

  private static final String LOGFIELD_PARAM = "logField";

  public LogFieldConverter create(
      SchemaBuilder builder,
      Class<?> logClass,
      String logFieldName,
      String schemaFieldName) {

    final Schema fieldSchema;
    final Field field;
    final LogFieldConverter converter;

    if (LocalDate.class.equals(logClass)) {
      fieldSchema = Date.builder()
          .optional()
          .parameter(LOGFIELD_PARAM, logFieldName)
          .build();
      builder.field(schemaFieldName, fieldSchema);
      field = builder.field(schemaFieldName);
      converter = new LocalDateLogFieldConverter(logFieldName, field);
    } else if (LocalTime.class.equals(logClass)) {
      fieldSchema = Time.builder()
          .optional()
          .parameter(LOGFIELD_PARAM, logFieldName)
          .build();
      builder.field(schemaFieldName, fieldSchema);
      field = builder.field(schemaFieldName);
      converter = new LocalTimeLogFieldConverter(logFieldName, field);
    } else if (Integer.class.equals(logClass)) {
      fieldSchema = SchemaBuilder.int32()
          .optional()
          .parameter(LOGFIELD_PARAM, logFieldName)
          .build();
      builder.field(schemaFieldName, fieldSchema);
      field = builder.field(schemaFieldName);
      converter = new PrimitiveLogFieldConverter(logFieldName, field);
    } else if (Long.class.equals(logClass)) {
      fieldSchema = SchemaBuilder.int64()
          .optional()
          .parameter(LOGFIELD_PARAM, logFieldName)
          .build();
      builder.field(schemaFieldName, fieldSchema);
      field = builder.field(schemaFieldName);
      converter = new PrimitiveLogFieldConverter(logFieldName, field);
    } else if (String.class.equals(logClass)) {
      fieldSchema = SchemaBuilder.string()
          .optional()
          .parameter(LOGFIELD_PARAM, logFieldName)
          .build();
      builder.field(schemaFieldName, fieldSchema);
      field = builder.field(schemaFieldName);
      converter = new PrimitiveLogFieldConverter(logFieldName, field);
    } else {
      throw new UnsupportedOperationException(
          String.format("%s is not a supported type.", logClass.getName())
      );
    }

    return converter;
  }

  public LogFieldConverter createDateTime(SchemaBuilder builder, String logEntryDateField, String logEntryTimeField, String connectTimestampField) {
    final Schema fieldSchema = Timestamp.builder()
        .optional()
        .parameter(LOGFIELD_PARAM, String.format("%s,%s", logEntryDateField, logEntryTimeField))
        .build();
    builder.field(connectTimestampField, fieldSchema);
    final Field field = builder.field(connectTimestampField);
    final LogFieldConverter converter = new TimestampLogFieldConverter(
        field,
        logEntryTimeField,
        logEntryDateField
    );
    return converter;
  }
}
