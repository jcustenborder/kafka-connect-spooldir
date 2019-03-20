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
    "input file will be converted based on the user supplied schema.\n" +
    "\n" +
    "The CSVRecordProcessor supports reading CSV or TSV files. It can convert a CSV on the fly " +
    "to the strongly typed Kafka Connect data types. It currently has support for all of the " +
    "schema types and logical types that are supported in Kafka Connect. If you couple this " +
    "with the Avro converter and Schema Registry by Confluent, you will be able to process " +
    "CSV, Json, or TSV files to strongly typed Avro data in real time.")
@Title("Spool Dir")
@DocumentationWarning("Running these connectors with multiple tasks requires a shared volume across " +
    "all of the Kafka Connect workers. Kafka Connect does not have a mechanism for synchronization of " +
    "tasks. Because of this each task will select which file it will use the following " +
    "algorithm `hash(<filename>) % totalTasks == taskNumber`. If you are not using a shared volume " +
    "this could cause issues where files are not processed. Using more than one task could also affect " +
    "the order that the data is written to Kafka.")
@PluginOwner("jcustenborder")
@PluginName("kafka-connect-spooldir")
package com.github.jcustenborder.kafka.connect.spooldir;

import com.github.jcustenborder.kafka.connect.utils.config.DocumentationWarning;
import com.github.jcustenborder.kafka.connect.utils.config.Introduction;
import com.github.jcustenborder.kafka.connect.utils.config.PluginName;
import com.github.jcustenborder.kafka.connect.utils.config.PluginOwner;
import com.github.jcustenborder.kafka.connect.utils.config.Title;