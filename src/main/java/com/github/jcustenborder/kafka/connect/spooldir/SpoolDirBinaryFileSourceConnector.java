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
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationWarning;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

import java.util.Map;

@Title("Binary File Source Connector")
@Description("This connector is used to read an entire file as a byte array write the data to Kafka.")
@DocumentationImportant("The recommended converter to use is the ByteArrayConverter. " +
    "Example: `value.converter=org.apache.kafka.connect.storage.ByteArrayConverter`")
@DocumentationWarning("Large files will be read as a single byte array. This means that the process could " +
    "run out of memory or try to send a message to Kafka that is greater than the max message size. If this happens " +
    "an exception will be thrown.")
public class SpoolDirBinaryFileSourceConnector extends AbstractSourceConnector<SpoolDirBinaryFileSourceConnectorConfig> {
  @Override
  protected SpoolDirBinaryFileSourceConnectorConfig config(Map<String, ?> settings) {
    return new SpoolDirBinaryFileSourceConnectorConfig(settings);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SpoolDirLineDelimitedSourceTask.class;
  }

  @Override
  public ConfigDef config() {
    return SpoolDirBinaryFileSourceConnectorConfig.config();
  }
}
