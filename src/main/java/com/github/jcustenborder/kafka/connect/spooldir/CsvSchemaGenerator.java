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
package com.github.jcustenborder.kafka.connect.spooldir;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.Map;

public class CsvSchemaGenerator extends SchemaGenerator<SpoolDirCsvSourceConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(CsvSchemaGenerator.class);

  public CsvSchemaGenerator(Map<String, ?> settings) {
    super(settings);
  }

  @Override
  protected SpoolDirCsvSourceConnectorConfig config(Map<String, ?> settings) {
    return new SpoolDirCsvSourceConnectorConfig(false, settings);
  }

  @Override
  protected Map<String, Schema.Type> determineFieldTypes(InputStream inputStream) throws IOException {
    Map<String, Schema.Type> typeMap = new LinkedHashMap<>();
    CSVParser parserBuilder = this.config.createCSVParserBuilder().build();
    try (InputStreamReader reader = new InputStreamReader(inputStream)) {
      try (CSVReader csvReader = new CSVReader(reader, 0, parserBuilder)) {
        String[] headers = null;

        if (this.config.firstRowAsHeader) {
          headers = csvReader.readNext();
        }

        String[] row = csvReader.readNext();

        if (null == headers) {
          headers = new String[row.length];
          for (int i = 1; i <= row.length; i++) {
            headers[(i - 1)] = String.format("column%02d", i);
          }
        }

        for (String s : headers) {
          typeMap.put(s, Schema.Type.STRING);
        }
      }
    }
    return typeMap;
  }


}
