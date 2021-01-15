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

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationImportant;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

import java.util.Map;

@Title("Schema Less Json Source Connector")
@Description("This connector is used to `stream <https://en.wikipedia.org/wiki/JSON_Streaming>_` JSON files from a directory " +
    "while converting the data based on the schema supplied in the configuration. This connector will read each file node by node " +
    "writing the result to Kafka. For example if your data file contains several json objects the connector will read from { to } " +
    "for each object and write each object to Kafka.")
@DocumentationImportant("This connector does not try to convert the json records to a schema. " +
    "The recommended converter to use is the StringConverter. " +
    "Example: `value.converter=org.apache.kafka.connect.storage.StringConverter`")
public class SpoolDirSchemaLessJsonSourceConnector extends AbstractSourceConnector<SpoolDirSchemaLessJsonSourceConnectorConfig> {
  @Override
  protected SpoolDirSchemaLessJsonSourceConnectorConfig config(Map<String, ?> settings) {
    return new SpoolDirSchemaLessJsonSourceConnectorConfig(settings);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SpoolDirSchemaLessJsonSourceTask.class;
  }

  @Override
  public ConfigDef config() {
    return SpoolDirSchemaLessJsonSourceConnectorConfig.config();
  }
}
