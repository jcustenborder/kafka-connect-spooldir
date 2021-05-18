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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import org.apache.kafka.connect.data.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class JsonSchemaGenerator extends AbstractSchemaGenerator<SpoolDirJsonSourceConnectorConfig> {
  public JsonSchemaGenerator(Map<String, ?> settings) {
    super(settings);
  }

  @Override
  protected SpoolDirJsonSourceConnectorConfig config(Map<String, ?> settings) {
    return new SpoolDirJsonSourceConnectorConfig(false, settings);
  }

  @Override
  protected Map<String, Schema.Type> determineFieldTypes(InputStream inputStream) throws IOException {
    Map<String, Schema.Type> typeMap = new LinkedHashMap<>();

    JsonFactory factory = new JsonFactory();
    try (JsonParser parser = factory.createParser(inputStream)) {
      Iterator<JsonNode> iterator = ObjectMapperFactory.INSTANCE.readValues(parser, JsonNode.class);
      while (iterator.hasNext()) {
        JsonNode node = iterator.next();
        if (node.isObject()) {
          Iterator<String> fieldNames = node.fieldNames();
          while (fieldNames.hasNext()) {
            typeMap.put(fieldNames.next(), Schema.Type.STRING);
          }
          break;
        }
      }
    }

    return typeMap;
  }
}
