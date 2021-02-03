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

@Title("Avro Source Connector")
@Description("This connector is used to read avro data files from the file system and write their contents " +
    "to Kafka. The schema of the file is used to read the data and produce it to Kafka")
@DocumentationImportant("This connector has a dependency on the Confluent Schema Registry specifically kafka-connect-avro-converter. " +
    "This dependency is not shipped along with the connector to ensure that there are not potential version mismatch issues. " +
    "The easiest way to ensure this component is available is to use one of the Confluent packages or containers for deployment.")
public class SpoolDirAvroSourceConnector extends AbstractSourceConnector<SpoolDirAvroSourceConnectorConfig> {
  @Override
  protected SpoolDirAvroSourceConnectorConfig config(Map<String, ?> settings) {
    return new SpoolDirAvroSourceConnectorConfig(settings);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SpoolDirAvroSourceTask.class;
  }

  @Override
  public ConfigDef config() {
    return SpoolDirAvroSourceConnectorConfig.config();
  }
}
