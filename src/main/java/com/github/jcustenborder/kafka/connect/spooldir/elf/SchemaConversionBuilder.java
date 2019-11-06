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
package com.github.jcustenborder.kafka.connect.spooldir.elf;

import com.github.jcustenborder.kafka.connect.spooldir.elf.converters.LogFieldConverter;
import com.github.jcustenborder.kafka.connect.spooldir.elf.converters.LogFieldConverterFactory;
import com.github.jcustenborder.parsers.elf.ElfParser;
import com.google.common.base.Preconditions;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SchemaConversionBuilder {
  private static final Logger log = LoggerFactory.getLogger(SchemaConversionBuilder.class);
  final ElfParser parser;

  public SchemaConversionBuilder(ElfParser parser) {
    this.parser = parser;
  }

  static String normalizeFieldName(String fieldName) {
    Preconditions.checkNotNull(fieldName, "fieldname cannot be null.");
    final String result = fieldName.replace('(', '_')
        .replace(")", "")
        .replace('-', '_')
        .toLowerCase();
    return result;
  }


  public SchemaConversion build() {
    log.trace("build() - Building SchemaConversion");

    final SchemaBuilder valueBuilder = SchemaBuilder.struct();
    valueBuilder.name("com.github.jcustenborder.kafka.connect.spooldir.LogEntry");

    LogFieldConverterFactory factory = new LogFieldConverterFactory();
    List<LogFieldConverter> valueConverters = new ArrayList<>();

    for (Map.Entry<String, Class<?>> entry : this.parser.fieldTypes().entrySet()) {
      final String logFieldName = entry.getKey();
      final Class<?> logFieldClass = entry.getValue();
      final String connectFieldName = normalizeFieldName(logFieldName);
      log.trace("build() - Mapping log field '{}' to schema field '{}'", logFieldName, connectFieldName);
      final LogFieldConverter converter = factory.create(
          valueBuilder,
          logFieldClass,
          logFieldName,
          connectFieldName
      );
      valueConverters.add(converter);
    }

    if (LocalDate.class.equals(this.parser.fieldTypes().get("date")) && LocalTime.class.equals(this.parser.fieldTypes().get("time"))) {
      log.trace("build() - found date and time field. Creating datetime field.");
      final LogFieldConverter converter = factory.createDateTime(
          valueBuilder,
          "date",
          "time",
          "datetime"
      );
      valueConverters.add(converter);
    }

    final Schema valueSchema = valueBuilder.build();

    return new SchemaConversion(valueSchema, valueConverters);
  }
}
