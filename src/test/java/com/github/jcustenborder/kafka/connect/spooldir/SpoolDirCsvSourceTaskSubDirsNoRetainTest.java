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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
public class SpoolDirCsvSourceTaskSubDirsNoRetainTest extends SpoolDirCsvSourceTaskTest {
  private static final Logger log = LoggerFactory.getLogger(SpoolDirCsvSourceTaskSubDirsNoRetainTest.class);

  @Override
  protected Map<String, String> settings() {
    Map<String, String> settings = super.settings();

    settings.put(AbstractSourceConnectorConfig.INPUT_PATH_WALK_RECURSIVELY,"true");
    settings.put(AbstractSourceConnectorConfig.CLEANUP_POLICY_MAINTAIN_RELATIVE_PATH,"false");

    return settings;
  }

  @Override
  protected String defineInputPathSubDir() {
    return "test/01/02/03";
  }

}
