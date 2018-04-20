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
@Introduction("\n" +
    "This Kafka Connect connector provides the capability to watch a directory for files and " +
    "read the data as new files are written to the input directory. Each of the records in the " +
    "input file will be converted based on the user supplied schema.\n" +
    "\n" +
    "The CSVRecordProcessor supports reading CSV or TSV files. It can convert a CSV on the fly " +
    "to the strongly typed Kafka Connect data types. It currently has support for all of the " +
    "schema types and logical types that are supported in Kafka Connect. If you couple this " +
    "with the Avro converter and Schema Registry by Confluent, you will be able to process " +
    "CSV, Json, or TSV files to strongly typed Avro data in real time.")
    @Title("File Input")
package com.github.jcustenborder.kafka.connect.spooldir;

import com.github.jcustenborder.kafka.connect.utils.config.Introduction;
import com.github.jcustenborder.kafka.connect.utils.config.Title;