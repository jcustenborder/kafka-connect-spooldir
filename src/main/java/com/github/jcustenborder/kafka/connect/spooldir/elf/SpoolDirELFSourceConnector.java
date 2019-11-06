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

import com.github.jcustenborder.kafka.connect.spooldir.AbstractSourceConnectorConfig;
import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Title("Extended Log File Format Source Connector")
@Description("This connector is used to stream `Extended Log File Format <https://www.w3.org/TR/WD-logfile.html>` " +
    "files from a directory while converting the data to a strongly typed schema.")
public class SpoolDirELFSourceConnector extends SourceConnector {

  @Override
  public List<Map<String, String>> taskConfigs(int taskCount) {
    List<Map<String, String>> result = new ArrayList<>();

    for (int i = 0; i < taskCount; i++) {
      Map<String, String> taskConfig = new LinkedHashMap<>(this.settings);
      taskConfig.put(AbstractSourceConnectorConfig.TASK_INDEX_CONF, Integer.toString(i));
      taskConfig.put(AbstractSourceConnectorConfig.TASK_COUNT_CONF, Integer.toString(taskCount));
      result.add(taskConfig);
    }

    return result;
  }

  @Override
  public void stop() {

  }

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  Map<String, String> settings;

  @Override
  public void start(Map<String, String> settings) {
    SpoolDirELFSourceConnectorConfig config = new SpoolDirELFSourceConnectorConfig(settings);
    this.settings = settings;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SpoolDirELFSourceTask.class;
  }

  @Override
  public ConfigDef config() {
    return SpoolDirELFSourceConnectorConfig.config(true);
  }
}
