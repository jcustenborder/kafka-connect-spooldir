/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.spooldir;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class SpoolDirSourceConnector<CONF extends SpoolDirSourceConnectorConfig> extends SourceConnector {
  private static Logger log = LoggerFactory.getLogger(SpoolDirSourceConnector.class);
  private CONF config;
  private Map<String, String> settings;

  protected abstract CONF config(Map<String, String> settings);


  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> settings) {
    config = config(settings);
    this.settings = settings;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int i) {
    return Arrays.asList(this.settings);
  }

  @Override
  public void stop() {

  }
}
