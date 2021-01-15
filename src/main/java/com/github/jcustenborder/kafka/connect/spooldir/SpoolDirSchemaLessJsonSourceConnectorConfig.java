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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import org.apache.kafka.common.config.ConfigDef;

import java.nio.charset.Charset;
import java.util.Map;

public class SpoolDirSchemaLessJsonSourceConnectorConfig extends AbstractSourceConnectorConfig {
  public static final String CHARSET_CONF = "file.charset";
  static final String CHARSET_DOC = "Character set to read wth file with.";
  static final String CHARSET_DEFAULT = Charset.defaultCharset().name();
  static final String CHARSET_DISPLAY = "File character set.";

  public final Charset charset;

  public SpoolDirSchemaLessJsonSourceConnectorConfig(Map<?, ?> originals) {
    super(config(), originals, true);
    this.charset = ConfigUtils.charset(this, CHARSET_CONF);
  }

  public static ConfigDef config() {
    return AbstractSourceConnectorConfig.config(true)
        .define(
            ConfigKeyBuilder.of(CHARSET_CONF, ConfigDef.Type.STRING)
                .defaultValue(CHARSET_DEFAULT)
                .validator(SpoolDirCsvSourceConnectorConfig.CharsetValidator.of())
                .importance(ConfigDef.Importance.LOW)
                .documentation(CHARSET_DOC)
                .displayName(CHARSET_DISPLAY)
                .width(ConfigDef.Width.LONG)
                .build()
        );
  }
}
