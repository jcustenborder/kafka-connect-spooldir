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
@Introduction(
    "This Kafka Connect connector provides the capability to watch a directory for files and " +
        "read the data as new files are written to the input directory. Each of the records in the " +
        "input file will be converted based on the user supplied schema. The connectors in this project " +
        "handle all different kinds of use cases like ingesting json, csv, tsv, avro, or binary files.")
@Title("Spool Dir")
@DocumentationWarning("Running these connectors with multiple tasks requires a shared volume across " +
    "all of the Kafka Connect workers. Kafka Connect does not have a mechanism for synchronization of " +
    "tasks. Because of this each task will select which file it will use the following " +
    "algorithm `hash(<filename>) % totalTasks == taskNumber`. If you are not using a shared volume " +
    "this could cause issues where files are not processed. Using more than one task could also affect " +
    "the order that the data is written to Kafka.")
@PluginOwner("jcustenborder")
@PluginName("kafka-connect-spooldir")
@DocumentationNote("Each of the connectors in this plugin emit the following headers for each record " +
    "written to kafka. \n\n" +
    "* `file.path` - The absolute path to the file ingested.\n" +
    "* `file.name` - The name part of the file ingested.\n" +
    "* `file.name.without.extension` - The file name without the extension part of the file.\n" +
    "* `file.last.modified` - The last modified date of the file.\n" +
    "* `file.length` - The size of the file in bytes.\n" +
    "* `file.offset` - The offset for this piece of data within the file.\n"
)
package com.github.jcustenborder.kafka.connect.spooldir;

import com.github.jcustenborder.kafka.connect.utils.config.DocumentationNote;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationWarning;
import com.github.jcustenborder.kafka.connect.utils.config.Introduction;
import com.github.jcustenborder.kafka.connect.utils.config.PluginName;
import com.github.jcustenborder.kafka.connect.utils.config.PluginOwner;
import com.github.jcustenborder.kafka.connect.utils.config.Title;