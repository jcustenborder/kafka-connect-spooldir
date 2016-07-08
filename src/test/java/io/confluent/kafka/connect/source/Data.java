/**
 * Copyright (C) 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.connect.source;

import io.confluent.kafka.connect.source.io.processing.csv.FieldConfig;
import io.confluent.kafka.connect.source.io.processing.csv.SchemaConfig;

import java.io.InputStream;

public class Data {

  public static InputStream getMockData() {
    return Data.class.getResourceAsStream("MOCK_DATA.csv");
  }


  static void addField(SchemaConfig schemaConfig, String name, FieldConfig.Type type, boolean required, Integer scale) {
    FieldConfig config = new FieldConfig();
    config.name = name;
    config.type = type;
    config.required = required;
    config.scale = scale;
    schemaConfig.fields.add(config);
  }

  public static SchemaConfig getMockDataSchemaConfig() {
    SchemaConfig schemaConfig = new SchemaConfig();
    schemaConfig.name = "io.confluent.kafka.connect.source.MockData";
    addField(schemaConfig, "id", FieldConfig.Type.INT32, true, null);
    addField(schemaConfig, "first_name", FieldConfig.Type.STRING, true, null);
    addField(schemaConfig, "last_name", FieldConfig.Type.STRING, true, null);
    addField(schemaConfig, "email", FieldConfig.Type.STRING, true, null);
    addField(schemaConfig, "gender", FieldConfig.Type.STRING, true, null);
    addField(schemaConfig, "ip_address", FieldConfig.Type.STRING, true, null);
    addField(schemaConfig, "last_login", FieldConfig.Type.TIMESTAMP, true, null);
    addField(schemaConfig, "account_balance", FieldConfig.Type.DECIMAL, false, 10);
    addField(schemaConfig, "country", FieldConfig.Type.STRING, true, null);
    addField(schemaConfig, "favorite_color", FieldConfig.Type.STRING, true, null);
    return schemaConfig;
  }

}
