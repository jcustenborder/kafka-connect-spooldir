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
package io.confluent.kafka.connect.source;

import io.confluent.kafka.connect.source.io.DirectoryMonitor;
import io.confluent.kafka.connect.source.io.PollingDirectoryMonitor;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SpoolDirectoryTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(SpoolDirectoryTask.class);
  SpoolDirectoryConfig config;
  DirectoryMonitor directoryMonitor;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    this.config = new SpoolDirectoryConfig(map);
    this.directoryMonitor = new PollingDirectoryMonitor();
    this.directoryMonitor.configure(this.config);
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> results = this.directoryMonitor.poll();

    while (results.isEmpty()) {
      Thread.sleep(1000);
      results = this.directoryMonitor.poll();
    }

    return results;
  }

  @Override
  public void stop() {

  }
}