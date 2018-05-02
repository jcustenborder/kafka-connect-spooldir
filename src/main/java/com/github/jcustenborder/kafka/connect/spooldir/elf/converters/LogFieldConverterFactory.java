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
    } else if (Double.class.equals(logClass)) {
      fieldSchema = SchemaBuilder.float64()
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
