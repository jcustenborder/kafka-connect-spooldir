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

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationImportant;
import com.github.jcustenborder.kafka.connect.utils.config.TaskConfigs;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.List;
import java.util.Map;

@Title("Line Delimited Source Connector")
@Description("This connector is used to read a file line by line and write the data to Kafka.")
@DocumentationImportant("The recommended converter to use is the StringConverter. " +
    "Example: `value.converter=org.apache.kafka.connect.storage.StringConverter`")
public class SpoolDirLineDelimitedSourceConnector extends SourceConnector {

  Map<String, String> settings;

  @Override
  public void start(Map<String, String> settings) {
    SpoolDirSchemaLessJsonSourceConnectorConfig config = new SpoolDirSchemaLessJsonSourceConnectorConfig(settings);
    this.settings = settings;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SpoolDirLineDelimitedSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    return TaskConfigs.single(this.settings);
  }

  @Override
  public void stop() {

  }

  @Override
  public ConfigDef config() {
    return SpoolDirLineDelimitedSourceConnectorConfig.config(true);
  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }
}
