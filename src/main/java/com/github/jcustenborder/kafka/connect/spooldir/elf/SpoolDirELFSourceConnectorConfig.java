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
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

class SpoolDirELFSourceConnectorConfig extends AbstractSourceConnectorConfig {

  public SpoolDirELFSourceConnectorConfig(Map<String, ?> settings) {
    super(config(true), settings, true);
  }

  public static ConfigDef config(boolean bufferedInputStream) {
    return AbstractSourceConnectorConfig.config(bufferedInputStream);
  }
}
