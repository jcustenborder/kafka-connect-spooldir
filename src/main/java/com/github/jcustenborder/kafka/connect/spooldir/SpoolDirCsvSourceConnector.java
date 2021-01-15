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
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

import java.util.Map;

@Title("CSV Source Connector")
@Description("The SpoolDirCsvSourceConnector will monitor the directory specified in `input.path` for files and read them as a CSV " +
    "converting each of the records to the strongly typed equivalent specified in `key.schema` and `value.schema`.")
@DocumentationTip("To get a starting point for a schema you can use the following command to generate an all String schema. This " +
    "will give you the basic structure of a schema. From there you can changes the types to match what you expect.\n\n" +
    ".. code-block:: bash\n\n" +
    "   mvn clean package\n" +
    "   export CLASSPATH=\"$(find target/kafka-connect-target/usr/share/kafka-connect/kafka-connect-spooldir -type f -name '*.jar' | tr '\\n' ':')\"\n" +
    "   kafka-run-class com.github.jcustenborder.kafka.connect.spooldir.AbstractSchemaGenerator -t csv -f src/test/resources/com/github/jcustenborder/kafka/connect/spooldir/csv/FieldsMatch.data -c config/CSVExample.properties -i id\n" +
    "")
@DocumentationImportant("There are some caveats to running this connector with `" + SpoolDirCsvSourceConnectorConfig.SCHEMA_GENERATION_ENABLED_CONF +
    " = true`. If schema generation is enabled the connector will start by reading one of the files that match `" + SpoolDirCsvSourceConnectorConfig.INPUT_FILE_PATTERN_CONF +
    "` in the path specified by `" + SpoolDirCsvSourceConnectorConfig.INPUT_PATH_CONFIG + "`. If there are no files when the connector starts or is restarted " +
    "the connector will fail to start. If there are different fields in other files they will not be detected. The recommended path is to specify a schema that the " +
    "files will be parsed with. This will ensure that data written by this connector to Kafka will be consistent across files that have inconsistent columns. For example " +
    "if some files have an optional column that is not always included, create a schema that includes the column marked as optional.")
public class SpoolDirCsvSourceConnector extends AbstractSpoolDirSourceConnector<SpoolDirCsvSourceConnectorConfig> {
  @Override
  protected SpoolDirCsvSourceConnectorConfig config(Map<String, ?> settings) {
    return new SpoolDirCsvSourceConnectorConfig(false, settings);
  }

  @Override
  protected AbstractSchemaGenerator<SpoolDirCsvSourceConnectorConfig> generator(Map<String, String> settings) {
    return new CsvSchemaGenerator(settings);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SpoolDirCsvSourceTask.class;
  }

  @Override
  public ConfigDef config() {
    return SpoolDirCsvSourceConnectorConfig.config();
  }
}
