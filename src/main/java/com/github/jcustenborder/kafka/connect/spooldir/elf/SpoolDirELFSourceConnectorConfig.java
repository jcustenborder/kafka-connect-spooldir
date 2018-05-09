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

import com.github.jcustenborder.kafka.connect.spooldir.SpoolDirSourceConnectorConfig;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

class SpoolDirELFSourceConnectorConfig extends SpoolDirSourceConnectorConfig {

  public final char separatorChar;

  public SpoolDirELFSourceConnectorConfig(final boolean isTask, Map<String, ?> settings) {
    super(isTask, config(), settings);
    this.separatorChar = (char) ((int) getInt(SEPARATOR_CHAR_CONF));
  }

  public static final String KEY_FIELDS_CONFIG = "key.fields";
  public static final String KEY_FIELDS_DOC = "key.fields";

  public static final String SEPARATOR_CHAR_CONF = "elf.separator.char";
  static final String SEPARATOR_CHAR_DISPLAY = "Separator Character";
  static final String SEPARATOR_CHAR_DOC = "The character that separates each field in the form " +
      "of an integer. Typically in a CSV this is a TAB(9) or SPACE(32).";

  @Override
  public boolean schemasRequired() {
    return false;
  }

  public static ConfigDef config() {
    return SpoolDirSourceConnectorConfig.config()
        .define(
            ConfigKeyBuilder.of(SEPARATOR_CHAR_CONF, ConfigDef.Type.INT)
                .documentation(SEPARATOR_CHAR_DOC)
                .importance(ConfigDef.Importance.HIGH)
                .defaultValue(9)
                .displayName(SEPARATOR_CHAR_DISPLAY)
                .build()
        );
  }

}
