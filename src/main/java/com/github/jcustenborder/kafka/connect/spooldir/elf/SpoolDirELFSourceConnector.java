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

import com.github.jcustenborder.kafka.connect.spooldir.SchemaGenerator;
import com.github.jcustenborder.kafka.connect.spooldir.SpoolDirSourceConnector;
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

import java.util.Map;

@Title("Extended Log File Format Source Connector")
@Description("This connector is used to stream `Extended Log File Format <https://www.w3.org/TR/WD-logfile.html>` " +
    "files from a directory while converting the data to a strongly typed schema.")
public class SpoolDirELFSourceConnector extends SpoolDirSourceConnector<SpoolDirELFSourceConnectorConfig> {

  @Override
  protected SpoolDirELFSourceConnectorConfig config(Map<String, String> settings) {
    return new SpoolDirELFSourceConnectorConfig(false, settings);
  }

  @Override
  protected SchemaGenerator<SpoolDirELFSourceConnectorConfig> generator(Map<String, String> settings) {
    return null;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SpoolDirELFSourceTask.class;
  }

  @Override
  public ConfigDef config() {
    return SpoolDirELFSourceConnectorConfig.config();
  }
}
