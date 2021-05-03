# Introduction
[Documentation](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-spooldir) | [Confluent Hub](https://www.confluent.io/hub/jcustenborder/kafka-connect-spooldir)

This Kafka Connect connector provides the capability to watch a directory for files and read the data as new files are written to the input directory. Each of the records in the input file will be converted based on the user supplied schema. The connectors in this project handle all different kinds of use cases like ingesting json, csv, tsv, avro, or binary files.

# Installation

## Confluent Hub

The following command can be used to install the plugin directly from the Confluent Hub using the
[Confluent Hub Client](https://docs.confluent.io/current/connect/managing/confluent-hub/client.html).

```bash
confluent-hub install jcustenborder/kafka-connect-spooldir:latest
```

## Manually

The zip file that is deployed to the [Confluent Hub](https://www.confluent.io/hub/jcustenborder/kafka-connect-spooldir) is available under
`target/components/packages/`. You can manually extract this zip file which includes all dependencies. All the dependencies
that are required to deploy the plugin are under `target/kafka-connect-target` as well. Make sure that you include all the dependencies that are required
to run the plugin.

1. Create a directory under the `plugin.path` on your Connect worker.
2. Copy all of the dependencies under the newly created subdirectory.
3. Restart the Connect worker.


# Source Connectors
## [Extended Log File Format Source Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-spooldir/sources/SpoolDirELFSourceConnector.html)

```
com.github.jcustenborder.kafka.connect.spooldir.elf.SpoolDirELFSourceConnector
```

This connector is used to stream `Extended Log File Format <https://www.w3.org/TR/WD-logfile.html>` files from a directory while converting the data to a strongly typed schema.
### Configuration

#### File System


##### `error.path`

The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING

*Validator:* Absolute path to a directory that exists and is writable.



##### `input.file.pattern`

Regular expression to check input file names against. This expression must match the entire filename. The equivalent of Matcher.matches().

*Importance:* HIGH

*Type:* STRING



##### `input.path`

The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING

*Validator:* Absolute path to a directory that exists and is writable.



##### `finished.path`

The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING



##### `halt.on.error`

Should the task halt when it encounters an error or continue to the next file.

*Importance:* HIGH

*Type:* BOOLEAN

*Default Value:* true



##### `cleanup.policy`

Determines how the connector should cleanup the files that have been successfully processed. NONE leaves the files in place which could cause them to be reprocessed if the connector is restarted. DELETE removes the file from the filesystem. MOVE will move the file to a finished directory. MOVEBYDATE will move the file to a finished directory with subdirectories by date

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* MOVE

*Validator:* Matches: ``NONE``, ``DELETE``, ``MOVE``, ``MOVEBYDATE``



##### `task.partitioner`

The task partitioner implementation is used when the connector is configured to use more than one task. This is used by each task to identify which files will be processed by that task. This ensures that each file is only assigned to one task.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* ByName

*Validator:* Matches: ``ByName``



##### `cleanup.policy.maintain.relative.path`

If `input.path.walk.recursively` is enabled in combination with this flag being `true`, the walked sub-directories which contained files will be retained as-is under the `input.path`. The actual files within the sub-directories will moved (with a copy of the sub-dir structure) or deleted as per the `cleanup.policy` defined, but the parent sub-directory structure will remain.

*Importance:* LOW

*Type:* BOOLEAN



##### `file.buffer.size.bytes`

The size of buffer for the BufferedInputStream that will be used to interact with the file system.

*Importance:* LOW

*Type:* INT

*Default Value:* 131072

*Validator:* [1,...]



##### `file.minimum.age.ms`

The amount of time in milliseconds after the file was last written to before the file can be processed.

*Importance:* LOW

*Type:* LONG

*Default Value:* 0

*Validator:* [0,...]



##### `files.sort.attributes`

The attributes each file will use to determine the sort order. `Name` is name of the file. `Length` is the length of the file preferring larger files first. `LastModified` is the LastModified attribute of the file preferring older files first.

*Importance:* LOW

*Type:* LIST

*Default Value:* [NameAsc]

*Validator:* Matches: ``NameAsc``, ``NameDesc``, ``LengthAsc``, ``LengthDesc``, ``LastModifiedAsc``, ``LastModifiedDesc``



##### `input.path.walk.recursively`

If enabled, any sub-directories dropped under `input.path` will be recursively walked looking for files matching the configured `input.file.pattern`. After processing is complete the discovered sub directory structure (as well as files within them) will handled according to the configured `cleanup.policy` (i.e. moved or deleted etc). For each discovered file, the walked sub-directory path will be set as a header named `file.relative.path`

*Importance:* LOW

*Type:* BOOLEAN



##### `processing.file.extension`

Before a file is processed, a flag is created in its directory to indicate the file is being handled. The flag file has the same name as the file, but with this property appended as a suffix.

*Importance:* LOW

*Type:* STRING

*Default Value:* .PROCESSING

*Validator:* Matches regex( ^.*\..+$ )


#### General


##### `topic`

The Kafka topic to write the data to.

*Importance:* HIGH

*Type:* STRING



##### `batch.size`

The number of records that should be returned with each batch.

*Importance:* LOW

*Type:* INT

*Default Value:* 1000



##### `empty.poll.wait.ms`

The amount of time to wait if a poll returns an empty list of records.

*Importance:* LOW

*Type:* LONG

*Default Value:* 500

*Validator:* [1,...,9223372036854775807]



##### `task.count`

Internal setting to the connector used to instruct a task on which files to select. The connector will override this setting.

*Importance:* LOW

*Type:* INT

*Default Value:* 1

*Validator:* [1,...]



##### `task.index`

Internal setting to the connector used to instruct a task on which files to select. The connector will override this setting.

*Importance:* LOW

*Type:* INT

*Default Value:* 0

*Validator:* [0,...]


#### Timestamps


##### `timestamp.mode`

Determines how the connector will set the timestamp for the [ConnectRecord](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/connector/ConnectRecord.html#timestamp()). If set to `Field` then the timestamp will be read from a field in the value. This field cannot be optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html). Specify the field  in `timestamp.field`. If set to `FILE_TIME` then the last modified time of the file will be used. If set to `PROCESS_TIME` the time the record is read will be used.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* PROCESS_TIME

*Validator:* Matches: ``FIELD``, ``FILE_TIME``, ``PROCESS_TIME``



## [Json Source Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-spooldir/sources/SpoolDirJsonSourceConnector.html)

```
com.github.jcustenborder.kafka.connect.spooldir.SpoolDirJsonSourceConnector
```

This connector is used to `stream <https://en.wikipedia.org/wiki/JSON_Streaming>` JSON files from a directory while converting the data based on the schema supplied in the configuration.
### Important

There are some caveats to running this connector with `schema.generation.enabled = true`. If schema generation is enabled the connector will start by reading one of the files that match `input.file.pattern` in the path specified by `input.path`. If there are no files when the connector starts or is restarted the connector will fail to start. If there are different fields in other files they will not be detected. The recommended path is to specify a schema that the files will be parsed with. This will ensure that data written by this connector to Kafka will be consistent across files that have inconsistent columns. For example if some files have an optional column that is not always included, create a schema that includes the column marked as optional.
### Note

If you want to import JSON node by node in the file and do not care about schemas, do not use this connector with Schema Generation enabled. Take a look at the Schema Less Json Source Connector.
### Tip

To get a starting point for a schema you can use the following command to generate an all String schema. This will give you the basic structure of a schema. From there you can changes the types to match what you expect.
.. code-block:: bash

   mvn clean package
   export CLASSPATH="$(find target/kafka-connect-target/usr/share/kafka-connect/kafka-connect-spooldir -type f -name '*.jar' | tr '\n' ':')"
   kafka-run-class com.github.jcustenborder.kafka.connect.spooldir.AbstractSchemaGenerator -t json -f src/test/resources/com/github/jcustenborder/kafka/connect/spooldir/json/FieldsMatch.data -c config/JsonExample.properties -i id

### Configuration

#### File System


##### `error.path`

The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING

*Validator:* Absolute path to a directory that exists and is writable.



##### `input.file.pattern`

Regular expression to check input file names against. This expression must match the entire filename. The equivalent of Matcher.matches().

*Importance:* HIGH

*Type:* STRING



##### `input.path`

The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING

*Validator:* Absolute path to a directory that exists and is writable.



##### `finished.path`

The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING



##### `halt.on.error`

Should the task halt when it encounters an error or continue to the next file.

*Importance:* HIGH

*Type:* BOOLEAN

*Default Value:* true



##### `cleanup.policy`

Determines how the connector should cleanup the files that have been successfully processed. NONE leaves the files in place which could cause them to be reprocessed if the connector is restarted. DELETE removes the file from the filesystem. MOVE will move the file to a finished directory. MOVEBYDATE will move the file to a finished directory with subdirectories by date

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* MOVE

*Validator:* Matches: ``NONE``, ``DELETE``, ``MOVE``, ``MOVEBYDATE``



##### `task.partitioner`

The task partitioner implementation is used when the connector is configured to use more than one task. This is used by each task to identify which files will be processed by that task. This ensures that each file is only assigned to one task.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* ByName

*Validator:* Matches: ``ByName``



##### `cleanup.policy.maintain.relative.path`

If `input.path.walk.recursively` is enabled in combination with this flag being `true`, the walked sub-directories which contained files will be retained as-is under the `input.path`. The actual files within the sub-directories will moved (with a copy of the sub-dir structure) or deleted as per the `cleanup.policy` defined, but the parent sub-directory structure will remain.

*Importance:* LOW

*Type:* BOOLEAN



##### `file.buffer.size.bytes`

The size of buffer for the BufferedInputStream that will be used to interact with the file system.

*Importance:* LOW

*Type:* INT

*Default Value:* 131072

*Validator:* [1,...]



##### `file.minimum.age.ms`

The amount of time in milliseconds after the file was last written to before the file can be processed.

*Importance:* LOW

*Type:* LONG

*Default Value:* 0

*Validator:* [0,...]



##### `files.sort.attributes`

The attributes each file will use to determine the sort order. `Name` is name of the file. `Length` is the length of the file preferring larger files first. `LastModified` is the LastModified attribute of the file preferring older files first.

*Importance:* LOW

*Type:* LIST

*Default Value:* [NameAsc]

*Validator:* Matches: ``NameAsc``, ``NameDesc``, ``LengthAsc``, ``LengthDesc``, ``LastModifiedAsc``, ``LastModifiedDesc``



##### `input.path.walk.recursively`

If enabled, any sub-directories dropped under `input.path` will be recursively walked looking for files matching the configured `input.file.pattern`. After processing is complete the discovered sub directory structure (as well as files within them) will handled according to the configured `cleanup.policy` (i.e. moved or deleted etc). For each discovered file, the walked sub-directory path will be set as a header named `file.relative.path`

*Importance:* LOW

*Type:* BOOLEAN



##### `processing.file.extension`

Before a file is processed, a flag is created in its directory to indicate the file is being handled. The flag file has the same name as the file, but with this property appended as a suffix.

*Importance:* LOW

*Type:* STRING

*Default Value:* .PROCESSING

*Validator:* Matches regex( ^.*\..+$ )


#### General


##### `topic`

The Kafka topic to write the data to.

*Importance:* HIGH

*Type:* STRING



##### `batch.size`

The number of records that should be returned with each batch.

*Importance:* LOW

*Type:* INT

*Default Value:* 1000



##### `empty.poll.wait.ms`

The amount of time to wait if a poll returns an empty list of records.

*Importance:* LOW

*Type:* LONG

*Default Value:* 500

*Validator:* [1,...,9223372036854775807]



##### `task.count`

Internal setting to the connector used to instruct a task on which files to select. The connector will override this setting.

*Importance:* LOW

*Type:* INT

*Default Value:* 1

*Validator:* [1,...]



##### `task.index`

Internal setting to the connector used to instruct a task on which files to select. The connector will override this setting.

*Importance:* LOW

*Type:* INT

*Default Value:* 0

*Validator:* [0,...]


#### Schema


##### `key.schema`

The schema for the key written to Kafka.

*Importance:* HIGH

*Type:* STRING



##### `value.schema`

The schema for the value written to Kafka.

*Importance:* HIGH

*Type:* STRING


#### Schema Generation


##### `schema.generation.enabled`

Flag to determine if schemas should be dynamically generated. If set  to true, `key.schema` and `value.schema` can be omitted, but `schema.generation.key.name` and `schema.generation.value.name` must be set.

*Importance:* MEDIUM

*Type:* BOOLEAN



##### `schema.generation.key.fields`

The field(s) to use to build a key schema. This is only used during schema generation.

*Importance:* MEDIUM

*Type:* LIST



##### `schema.generation.key.name`

The name of the generated key schema.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* com.github.jcustenborder.kafka.connect.model.Key



##### `schema.generation.value.name`

The name of the generated value schema.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* com.github.jcustenborder.kafka.connect.model.Value


#### Timestamps


##### `timestamp.field`

The field in the value schema that will contain the parsed timestamp for the record. This field cannot be marked as optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html)

*Importance:* MEDIUM

*Type:* STRING



##### `timestamp.mode`

Determines how the connector will set the timestamp for the [ConnectRecord](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/connector/ConnectRecord.html#timestamp()). If set to `Field` then the timestamp will be read from a field in the value. This field cannot be optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html). Specify the field  in `timestamp.field`. If set to `FILE_TIME` then the last modified time of the file will be used. If set to `PROCESS_TIME` the time the record is read will be used.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* PROCESS_TIME

*Validator:* Matches: ``FIELD``, ``FILE_TIME``, ``PROCESS_TIME``



##### `parser.timestamp.date.formats`

The date formats that are expected in the file. This is a list of strings that will be used to parse the date fields in order. The most accurate date format should be the first in the list. Take a look at the Java documentation for more info. https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html

*Importance:* LOW

*Type:* LIST

*Default Value:* [yyyy-MM-dd'T'HH:mm:ss, yyyy-MM-dd' 'HH:mm:ss]



##### `parser.timestamp.timezone`

The timezone that all of the dates will be parsed with.

*Importance:* LOW

*Type:* STRING

*Default Value:* UTC



## [Line Delimited Source Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-spooldir/sources/SpoolDirLineDelimitedSourceConnector.html)

```
com.github.jcustenborder.kafka.connect.spooldir.SpoolDirLineDelimitedSourceConnector
```

This connector is used to read a file line by line and write the data to Kafka.
### Important

The recommended converter to use is the StringConverter. Example: `value.converter=org.apache.kafka.connect.storage.StringConverter`
### Configuration

#### File System


##### `error.path`

The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING

*Validator:* Absolute path to a directory that exists and is writable.



##### `input.file.pattern`

Regular expression to check input file names against. This expression must match the entire filename. The equivalent of Matcher.matches().

*Importance:* HIGH

*Type:* STRING



##### `input.path`

The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING

*Validator:* Absolute path to a directory that exists and is writable.



##### `finished.path`

The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING



##### `halt.on.error`

Should the task halt when it encounters an error or continue to the next file.

*Importance:* HIGH

*Type:* BOOLEAN

*Default Value:* true



##### `cleanup.policy`

Determines how the connector should cleanup the files that have been successfully processed. NONE leaves the files in place which could cause them to be reprocessed if the connector is restarted. DELETE removes the file from the filesystem. MOVE will move the file to a finished directory. MOVEBYDATE will move the file to a finished directory with subdirectories by date

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* MOVE

*Validator:* Matches: ``NONE``, ``DELETE``, ``MOVE``, ``MOVEBYDATE``



##### `task.partitioner`

The task partitioner implementation is used when the connector is configured to use more than one task. This is used by each task to identify which files will be processed by that task. This ensures that each file is only assigned to one task.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* ByName

*Validator:* Matches: ``ByName``



##### `cleanup.policy.maintain.relative.path`

If `input.path.walk.recursively` is enabled in combination with this flag being `true`, the walked sub-directories which contained files will be retained as-is under the `input.path`. The actual files within the sub-directories will moved (with a copy of the sub-dir structure) or deleted as per the `cleanup.policy` defined, but the parent sub-directory structure will remain.

*Importance:* LOW

*Type:* BOOLEAN



##### `file.buffer.size.bytes`

The size of buffer for the BufferedInputStream that will be used to interact with the file system.

*Importance:* LOW

*Type:* INT

*Default Value:* 131072

*Validator:* [1,...]



##### `file.minimum.age.ms`

The amount of time in milliseconds after the file was last written to before the file can be processed.

*Importance:* LOW

*Type:* LONG

*Default Value:* 0

*Validator:* [0,...]



##### `files.sort.attributes`

The attributes each file will use to determine the sort order. `Name` is name of the file. `Length` is the length of the file preferring larger files first. `LastModified` is the LastModified attribute of the file preferring older files first.

*Importance:* LOW

*Type:* LIST

*Default Value:* [NameAsc]

*Validator:* Matches: ``NameAsc``, ``NameDesc``, ``LengthAsc``, ``LengthDesc``, ``LastModifiedAsc``, ``LastModifiedDesc``



##### `input.path.walk.recursively`

If enabled, any sub-directories dropped under `input.path` will be recursively walked looking for files matching the configured `input.file.pattern`. After processing is complete the discovered sub directory structure (as well as files within them) will handled according to the configured `cleanup.policy` (i.e. moved or deleted etc). For each discovered file, the walked sub-directory path will be set as a header named `file.relative.path`

*Importance:* LOW

*Type:* BOOLEAN



##### `processing.file.extension`

Before a file is processed, a flag is created in its directory to indicate the file is being handled. The flag file has the same name as the file, but with this property appended as a suffix.

*Importance:* LOW

*Type:* STRING

*Default Value:* .PROCESSING

*Validator:* Matches regex( ^.*\..+$ )


#### General


##### `topic`

The Kafka topic to write the data to.

*Importance:* HIGH

*Type:* STRING



##### `batch.size`

The number of records that should be returned with each batch.

*Importance:* LOW

*Type:* INT

*Default Value:* 1000



##### `empty.poll.wait.ms`

The amount of time to wait if a poll returns an empty list of records.

*Importance:* LOW

*Type:* LONG

*Default Value:* 500

*Validator:* [1,...,9223372036854775807]



##### `file.charset`

Character set to read wth file with.

*Importance:* LOW

*Type:* STRING

*Default Value:* UTF-8

*Validator:* Big5,Big5-HKSCS,CESU-8,EUC-JP,EUC-KR,GB18030,GB2312,GBK,IBM-Thai,IBM00858,IBM01140,IBM01141,IBM01142,IBM01143,IBM01144,IBM01145,IBM01146,IBM01147,IBM01148,IBM01149,IBM037,IBM1026,IBM1047,IBM273,IBM277,IBM278,IBM280,IBM284,IBM285,IBM290,IBM297,IBM420,IBM424,IBM437,IBM500,IBM775,IBM850,IBM852,IBM855,IBM857,IBM860,IBM861,IBM862,IBM863,IBM864,IBM865,IBM866,IBM868,IBM869,IBM870,IBM871,IBM918,ISO-2022-CN,ISO-2022-JP,ISO-2022-JP-2,ISO-2022-KR,ISO-8859-1,ISO-8859-13,ISO-8859-15,ISO-8859-16,ISO-8859-2,ISO-8859-3,ISO-8859-4,ISO-8859-5,ISO-8859-6,ISO-8859-7,ISO-8859-8,ISO-8859-9,JIS_X0201,JIS_X0212-1990,KOI8-R,KOI8-U,Shift_JIS,TIS-620,US-ASCII,UTF-16,UTF-16BE,UTF-16LE,UTF-32,UTF-32BE,UTF-32LE,UTF-8,windows-1250,windows-1251,windows-1252,windows-1253,windows-1254,windows-1255,windows-1256,windows-1257,windows-1258,windows-31j,x-Big5-HKSCS-2001,x-Big5-Solaris,x-euc-jp-linux,x-EUC-TW,x-eucJP-Open,x-IBM1006,x-IBM1025,x-IBM1046,x-IBM1097,x-IBM1098,x-IBM1112,x-IBM1122,x-IBM1123,x-IBM1124,x-IBM1129,x-IBM1166,x-IBM1364,x-IBM1381,x-IBM1383,x-IBM29626C,x-IBM300,x-IBM33722,x-IBM737,x-IBM833,x-IBM834,x-IBM856,x-IBM874,x-IBM875,x-IBM921,x-IBM922,x-IBM930,x-IBM933,x-IBM935,x-IBM937,x-IBM939,x-IBM942,x-IBM942C,x-IBM943,x-IBM943C,x-IBM948,x-IBM949,x-IBM949C,x-IBM950,x-IBM964,x-IBM970,x-ISCII91,x-ISO-2022-CN-CNS,x-ISO-2022-CN-GB,x-iso-8859-11,x-JIS0208,x-JISAutoDetect,x-Johab,x-MacArabic,x-MacCentralEurope,x-MacCroatian,x-MacCyrillic,x-MacDingbat,x-MacGreek,x-MacHebrew,x-MacIceland,x-MacRoman,x-MacRomania,x-MacSymbol,x-MacThai,x-MacTurkish,x-MacUkraine,x-MS932_0213,x-MS950-HKSCS,x-MS950-HKSCS-XP,x-mswin-936,x-PCK,x-SJIS_0213,x-UTF-16LE-BOM,X-UTF-32BE-BOM,X-UTF-32LE-BOM,x-windows-50220,x-windows-50221,x-windows-874,x-windows-949,x-windows-950,x-windows-iso2022jp



##### `task.count`

Internal setting to the connector used to instruct a task on which files to select. The connector will override this setting.

*Importance:* LOW

*Type:* INT

*Default Value:* 1

*Validator:* [1,...]



##### `task.index`

Internal setting to the connector used to instruct a task on which files to select. The connector will override this setting.

*Importance:* LOW

*Type:* INT

*Default Value:* 0

*Validator:* [0,...]


#### Timestamps


##### `timestamp.mode`

Determines how the connector will set the timestamp for the [ConnectRecord](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/connector/ConnectRecord.html#timestamp()). If set to `Field` then the timestamp will be read from a field in the value. This field cannot be optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html). Specify the field  in `timestamp.field`. If set to `FILE_TIME` then the last modified time of the file will be used. If set to `PROCESS_TIME` the time the record is read will be used.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* PROCESS_TIME

*Validator:* Matches: ``FIELD``, ``FILE_TIME``, ``PROCESS_TIME``



## [Avro Source Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-spooldir/sources/SpoolDirAvroSourceConnector.html)

```
com.github.jcustenborder.kafka.connect.spooldir.SpoolDirAvroSourceConnector
```

This connector is used to read avro data files from the file system and write their contents to Kafka. The schema of the file is used to read the data and produce it to Kafka
### Important

This connector has a dependency on the Confluent Schema Registry specifically kafka-connect-avro-converter. This dependency is not shipped along with the connector to ensure that there are not potential version mismatch issues. The easiest way to ensure this component is available is to use one of the Confluent packages or containers for deployment.
### Configuration

#### File System


##### `error.path`

The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING

*Validator:* Absolute path to a directory that exists and is writable.



##### `input.file.pattern`

Regular expression to check input file names against. This expression must match the entire filename. The equivalent of Matcher.matches().

*Importance:* HIGH

*Type:* STRING



##### `input.path`

The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING

*Validator:* Absolute path to a directory that exists and is writable.



##### `finished.path`

The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING



##### `halt.on.error`

Should the task halt when it encounters an error or continue to the next file.

*Importance:* HIGH

*Type:* BOOLEAN

*Default Value:* true



##### `cleanup.policy`

Determines how the connector should cleanup the files that have been successfully processed. NONE leaves the files in place which could cause them to be reprocessed if the connector is restarted. DELETE removes the file from the filesystem. MOVE will move the file to a finished directory. MOVEBYDATE will move the file to a finished directory with subdirectories by date

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* MOVE

*Validator:* Matches: ``NONE``, ``DELETE``, ``MOVE``, ``MOVEBYDATE``



##### `task.partitioner`

The task partitioner implementation is used when the connector is configured to use more than one task. This is used by each task to identify which files will be processed by that task. This ensures that each file is only assigned to one task.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* ByName

*Validator:* Matches: ``ByName``



##### `cleanup.policy.maintain.relative.path`

If `input.path.walk.recursively` is enabled in combination with this flag being `true`, the walked sub-directories which contained files will be retained as-is under the `input.path`. The actual files within the sub-directories will moved (with a copy of the sub-dir structure) or deleted as per the `cleanup.policy` defined, but the parent sub-directory structure will remain.

*Importance:* LOW

*Type:* BOOLEAN



##### `file.buffer.size.bytes`

The size of buffer for the BufferedInputStream that will be used to interact with the file system.

*Importance:* LOW

*Type:* INT

*Default Value:* 131072

*Validator:* [1,...]



##### `file.minimum.age.ms`

The amount of time in milliseconds after the file was last written to before the file can be processed.

*Importance:* LOW

*Type:* LONG

*Default Value:* 0

*Validator:* [0,...]



##### `files.sort.attributes`

The attributes each file will use to determine the sort order. `Name` is name of the file. `Length` is the length of the file preferring larger files first. `LastModified` is the LastModified attribute of the file preferring older files first.

*Importance:* LOW

*Type:* LIST

*Default Value:* [NameAsc]

*Validator:* Matches: ``NameAsc``, ``NameDesc``, ``LengthAsc``, ``LengthDesc``, ``LastModifiedAsc``, ``LastModifiedDesc``



##### `input.path.walk.recursively`

If enabled, any sub-directories dropped under `input.path` will be recursively walked looking for files matching the configured `input.file.pattern`. After processing is complete the discovered sub directory structure (as well as files within them) will handled according to the configured `cleanup.policy` (i.e. moved or deleted etc). For each discovered file, the walked sub-directory path will be set as a header named `file.relative.path`

*Importance:* LOW

*Type:* BOOLEAN



##### `processing.file.extension`

Before a file is processed, a flag is created in its directory to indicate the file is being handled. The flag file has the same name as the file, but with this property appended as a suffix.

*Importance:* LOW

*Type:* STRING

*Default Value:* .PROCESSING

*Validator:* Matches regex( ^.*\..+$ )


#### General


##### `topic`

The Kafka topic to write the data to.

*Importance:* HIGH

*Type:* STRING



##### `batch.size`

The number of records that should be returned with each batch.

*Importance:* LOW

*Type:* INT

*Default Value:* 1000



##### `empty.poll.wait.ms`

The amount of time to wait if a poll returns an empty list of records.

*Importance:* LOW

*Type:* LONG

*Default Value:* 500

*Validator:* [1,...,9223372036854775807]



##### `task.count`

Internal setting to the connector used to instruct a task on which files to select. The connector will override this setting.

*Importance:* LOW

*Type:* INT

*Default Value:* 1

*Validator:* [1,...]



##### `task.index`

Internal setting to the connector used to instruct a task on which files to select. The connector will override this setting.

*Importance:* LOW

*Type:* INT

*Default Value:* 0

*Validator:* [0,...]


#### Timestamps


##### `timestamp.mode`

Determines how the connector will set the timestamp for the [ConnectRecord](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/connector/ConnectRecord.html#timestamp()). If set to `Field` then the timestamp will be read from a field in the value. This field cannot be optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html). Specify the field  in `timestamp.field`. If set to `FILE_TIME` then the last modified time of the file will be used. If set to `PROCESS_TIME` the time the record is read will be used.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* PROCESS_TIME

*Validator:* Matches: ``FIELD``, ``FILE_TIME``, ``PROCESS_TIME``



## [Schema Less Json Source Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-spooldir/sources/SpoolDirSchemaLessJsonSourceConnector.html)

```
com.github.jcustenborder.kafka.connect.spooldir.SpoolDirSchemaLessJsonSourceConnector
```

This connector is used to `stream <https://en.wikipedia.org/wiki/JSON_Streaming>_` JSON files from a directory while converting the data based on the schema supplied in the configuration. This connector will read each file node by node writing the result to Kafka. For example if your data file contains several json objects the connector will read from { to } for each object and write each object to Kafka.
### Important

This connector does not try to convert the json records to a schema. The recommended converter to use is the StringConverter. Example: `value.converter=org.apache.kafka.connect.storage.StringConverter`
### Configuration

#### File System


##### `error.path`

The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING

*Validator:* Absolute path to a directory that exists and is writable.



##### `input.file.pattern`

Regular expression to check input file names against. This expression must match the entire filename. The equivalent of Matcher.matches().

*Importance:* HIGH

*Type:* STRING



##### `input.path`

The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING

*Validator:* Absolute path to a directory that exists and is writable.



##### `finished.path`

The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING



##### `halt.on.error`

Should the task halt when it encounters an error or continue to the next file.

*Importance:* HIGH

*Type:* BOOLEAN

*Default Value:* true



##### `cleanup.policy`

Determines how the connector should cleanup the files that have been successfully processed. NONE leaves the files in place which could cause them to be reprocessed if the connector is restarted. DELETE removes the file from the filesystem. MOVE will move the file to a finished directory. MOVEBYDATE will move the file to a finished directory with subdirectories by date

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* MOVE

*Validator:* Matches: ``NONE``, ``DELETE``, ``MOVE``, ``MOVEBYDATE``



##### `task.partitioner`

The task partitioner implementation is used when the connector is configured to use more than one task. This is used by each task to identify which files will be processed by that task. This ensures that each file is only assigned to one task.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* ByName

*Validator:* Matches: ``ByName``



##### `cleanup.policy.maintain.relative.path`

If `input.path.walk.recursively` is enabled in combination with this flag being `true`, the walked sub-directories which contained files will be retained as-is under the `input.path`. The actual files within the sub-directories will moved (with a copy of the sub-dir structure) or deleted as per the `cleanup.policy` defined, but the parent sub-directory structure will remain.

*Importance:* LOW

*Type:* BOOLEAN



##### `file.buffer.size.bytes`

The size of buffer for the BufferedInputStream that will be used to interact with the file system.

*Importance:* LOW

*Type:* INT

*Default Value:* 131072

*Validator:* [1,...]



##### `file.minimum.age.ms`

The amount of time in milliseconds after the file was last written to before the file can be processed.

*Importance:* LOW

*Type:* LONG

*Default Value:* 0

*Validator:* [0,...]



##### `files.sort.attributes`

The attributes each file will use to determine the sort order. `Name` is name of the file. `Length` is the length of the file preferring larger files first. `LastModified` is the LastModified attribute of the file preferring older files first.

*Importance:* LOW

*Type:* LIST

*Default Value:* [NameAsc]

*Validator:* Matches: ``NameAsc``, ``NameDesc``, ``LengthAsc``, ``LengthDesc``, ``LastModifiedAsc``, ``LastModifiedDesc``



##### `input.path.walk.recursively`

If enabled, any sub-directories dropped under `input.path` will be recursively walked looking for files matching the configured `input.file.pattern`. After processing is complete the discovered sub directory structure (as well as files within them) will handled according to the configured `cleanup.policy` (i.e. moved or deleted etc). For each discovered file, the walked sub-directory path will be set as a header named `file.relative.path`

*Importance:* LOW

*Type:* BOOLEAN



##### `processing.file.extension`

Before a file is processed, a flag is created in its directory to indicate the file is being handled. The flag file has the same name as the file, but with this property appended as a suffix.

*Importance:* LOW

*Type:* STRING

*Default Value:* .PROCESSING

*Validator:* Matches regex( ^.*\..+$ )


#### General


##### `topic`

The Kafka topic to write the data to.

*Importance:* HIGH

*Type:* STRING



##### `batch.size`

The number of records that should be returned with each batch.

*Importance:* LOW

*Type:* INT

*Default Value:* 1000



##### `empty.poll.wait.ms`

The amount of time to wait if a poll returns an empty list of records.

*Importance:* LOW

*Type:* LONG

*Default Value:* 500

*Validator:* [1,...,9223372036854775807]



##### `file.charset`

Character set to read wth file with.

*Importance:* LOW

*Type:* STRING

*Default Value:* UTF-8

*Validator:* Big5,Big5-HKSCS,CESU-8,EUC-JP,EUC-KR,GB18030,GB2312,GBK,IBM-Thai,IBM00858,IBM01140,IBM01141,IBM01142,IBM01143,IBM01144,IBM01145,IBM01146,IBM01147,IBM01148,IBM01149,IBM037,IBM1026,IBM1047,IBM273,IBM277,IBM278,IBM280,IBM284,IBM285,IBM290,IBM297,IBM420,IBM424,IBM437,IBM500,IBM775,IBM850,IBM852,IBM855,IBM857,IBM860,IBM861,IBM862,IBM863,IBM864,IBM865,IBM866,IBM868,IBM869,IBM870,IBM871,IBM918,ISO-2022-CN,ISO-2022-JP,ISO-2022-JP-2,ISO-2022-KR,ISO-8859-1,ISO-8859-13,ISO-8859-15,ISO-8859-16,ISO-8859-2,ISO-8859-3,ISO-8859-4,ISO-8859-5,ISO-8859-6,ISO-8859-7,ISO-8859-8,ISO-8859-9,JIS_X0201,JIS_X0212-1990,KOI8-R,KOI8-U,Shift_JIS,TIS-620,US-ASCII,UTF-16,UTF-16BE,UTF-16LE,UTF-32,UTF-32BE,UTF-32LE,UTF-8,windows-1250,windows-1251,windows-1252,windows-1253,windows-1254,windows-1255,windows-1256,windows-1257,windows-1258,windows-31j,x-Big5-HKSCS-2001,x-Big5-Solaris,x-euc-jp-linux,x-EUC-TW,x-eucJP-Open,x-IBM1006,x-IBM1025,x-IBM1046,x-IBM1097,x-IBM1098,x-IBM1112,x-IBM1122,x-IBM1123,x-IBM1124,x-IBM1129,x-IBM1166,x-IBM1364,x-IBM1381,x-IBM1383,x-IBM29626C,x-IBM300,x-IBM33722,x-IBM737,x-IBM833,x-IBM834,x-IBM856,x-IBM874,x-IBM875,x-IBM921,x-IBM922,x-IBM930,x-IBM933,x-IBM935,x-IBM937,x-IBM939,x-IBM942,x-IBM942C,x-IBM943,x-IBM943C,x-IBM948,x-IBM949,x-IBM949C,x-IBM950,x-IBM964,x-IBM970,x-ISCII91,x-ISO-2022-CN-CNS,x-ISO-2022-CN-GB,x-iso-8859-11,x-JIS0208,x-JISAutoDetect,x-Johab,x-MacArabic,x-MacCentralEurope,x-MacCroatian,x-MacCyrillic,x-MacDingbat,x-MacGreek,x-MacHebrew,x-MacIceland,x-MacRoman,x-MacRomania,x-MacSymbol,x-MacThai,x-MacTurkish,x-MacUkraine,x-MS932_0213,x-MS950-HKSCS,x-MS950-HKSCS-XP,x-mswin-936,x-PCK,x-SJIS_0213,x-UTF-16LE-BOM,X-UTF-32BE-BOM,X-UTF-32LE-BOM,x-windows-50220,x-windows-50221,x-windows-874,x-windows-949,x-windows-950,x-windows-iso2022jp



##### `task.count`

Internal setting to the connector used to instruct a task on which files to select. The connector will override this setting.

*Importance:* LOW

*Type:* INT

*Default Value:* 1

*Validator:* [1,...]



##### `task.index`

Internal setting to the connector used to instruct a task on which files to select. The connector will override this setting.

*Importance:* LOW

*Type:* INT

*Default Value:* 0

*Validator:* [0,...]


#### Timestamps


##### `timestamp.mode`

Determines how the connector will set the timestamp for the [ConnectRecord](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/connector/ConnectRecord.html#timestamp()). If set to `Field` then the timestamp will be read from a field in the value. This field cannot be optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html). Specify the field  in `timestamp.field`. If set to `FILE_TIME` then the last modified time of the file will be used. If set to `PROCESS_TIME` the time the record is read will be used.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* PROCESS_TIME

*Validator:* Matches: ``FIELD``, ``FILE_TIME``, ``PROCESS_TIME``



## [Binary File Source Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-spooldir/sources/SpoolDirBinaryFileSourceConnector.html)

```
com.github.jcustenborder.kafka.connect.spooldir.SpoolDirBinaryFileSourceConnector
```

This connector is used to read an entire file as a byte array write the data to Kafka.
### Warning

Large files will be read as a single byte array. This means that the process could run out of memory or try to send a message to Kafka that is greater than the max message size. If this happens an exception will be thrown.
### Important

The recommended converter to use is the ByteArrayConverter. Example: `value.converter=org.apache.kafka.connect.storage.ByteArrayConverter`
### Configuration

#### File System


##### `error.path`

The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING

*Validator:* Absolute path to a directory that exists and is writable.



##### `input.file.pattern`

Regular expression to check input file names against. This expression must match the entire filename. The equivalent of Matcher.matches().

*Importance:* HIGH

*Type:* STRING



##### `input.path`

The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING

*Validator:* Absolute path to a directory that exists and is writable.



##### `finished.path`

The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING



##### `halt.on.error`

Should the task halt when it encounters an error or continue to the next file.

*Importance:* HIGH

*Type:* BOOLEAN

*Default Value:* true



##### `cleanup.policy`

Determines how the connector should cleanup the files that have been successfully processed. NONE leaves the files in place which could cause them to be reprocessed if the connector is restarted. DELETE removes the file from the filesystem. MOVE will move the file to a finished directory. MOVEBYDATE will move the file to a finished directory with subdirectories by date

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* MOVE

*Validator:* Matches: ``NONE``, ``DELETE``, ``MOVE``, ``MOVEBYDATE``



##### `task.partitioner`

The task partitioner implementation is used when the connector is configured to use more than one task. This is used by each task to identify which files will be processed by that task. This ensures that each file is only assigned to one task.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* ByName

*Validator:* Matches: ``ByName``



##### `cleanup.policy.maintain.relative.path`

If `input.path.walk.recursively` is enabled in combination with this flag being `true`, the walked sub-directories which contained files will be retained as-is under the `input.path`. The actual files within the sub-directories will moved (with a copy of the sub-dir structure) or deleted as per the `cleanup.policy` defined, but the parent sub-directory structure will remain.

*Importance:* LOW

*Type:* BOOLEAN



##### `file.buffer.size.bytes`

The size of buffer for the BufferedInputStream that will be used to interact with the file system.

*Importance:* LOW

*Type:* INT

*Default Value:* 131072

*Validator:* [1,...]



##### `file.minimum.age.ms`

The amount of time in milliseconds after the file was last written to before the file can be processed.

*Importance:* LOW

*Type:* LONG

*Default Value:* 0

*Validator:* [0,...]



##### `files.sort.attributes`

The attributes each file will use to determine the sort order. `Name` is name of the file. `Length` is the length of the file preferring larger files first. `LastModified` is the LastModified attribute of the file preferring older files first.

*Importance:* LOW

*Type:* LIST

*Default Value:* [NameAsc]

*Validator:* Matches: ``NameAsc``, ``NameDesc``, ``LengthAsc``, ``LengthDesc``, ``LastModifiedAsc``, ``LastModifiedDesc``



##### `input.path.walk.recursively`

If enabled, any sub-directories dropped under `input.path` will be recursively walked looking for files matching the configured `input.file.pattern`. After processing is complete the discovered sub directory structure (as well as files within them) will handled according to the configured `cleanup.policy` (i.e. moved or deleted etc). For each discovered file, the walked sub-directory path will be set as a header named `file.relative.path`

*Importance:* LOW

*Type:* BOOLEAN



##### `processing.file.extension`

Before a file is processed, a flag is created in its directory to indicate the file is being handled. The flag file has the same name as the file, but with this property appended as a suffix.

*Importance:* LOW

*Type:* STRING

*Default Value:* .PROCESSING

*Validator:* Matches regex( ^.*\..+$ )


#### General


##### `topic`

The Kafka topic to write the data to.

*Importance:* HIGH

*Type:* STRING



##### `batch.size`

The number of records that should be returned with each batch.

*Importance:* LOW

*Type:* INT

*Default Value:* 1000



##### `empty.poll.wait.ms`

The amount of time to wait if a poll returns an empty list of records.

*Importance:* LOW

*Type:* LONG

*Default Value:* 500

*Validator:* [1,...,9223372036854775807]



##### `task.count`

Internal setting to the connector used to instruct a task on which files to select. The connector will override this setting.

*Importance:* LOW

*Type:* INT

*Default Value:* 1

*Validator:* [1,...]



##### `task.index`

Internal setting to the connector used to instruct a task on which files to select. The connector will override this setting.

*Importance:* LOW

*Type:* INT

*Default Value:* 0

*Validator:* [0,...]


#### Timestamps


##### `timestamp.mode`

Determines how the connector will set the timestamp for the [ConnectRecord](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/connector/ConnectRecord.html#timestamp()). If set to `Field` then the timestamp will be read from a field in the value. This field cannot be optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html). Specify the field  in `timestamp.field`. If set to `FILE_TIME` then the last modified time of the file will be used. If set to `PROCESS_TIME` the time the record is read will be used.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* PROCESS_TIME

*Validator:* Matches: ``FIELD``, ``FILE_TIME``, ``PROCESS_TIME``



## [CSV Source Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-spooldir/sources/SpoolDirCsvSourceConnector.html)

```
com.github.jcustenborder.kafka.connect.spooldir.SpoolDirCsvSourceConnector
```

The SpoolDirCsvSourceConnector will monitor the directory specified in `input.path` for files and read them as a CSV converting each of the records to the strongly typed equivalent specified in `key.schema` and `value.schema`.
### Important

There are some caveats to running this connector with `schema.generation.enabled = true`. If schema generation is enabled the connector will start by reading one of the files that match `input.file.pattern` in the path specified by `input.path`. If there are no files when the connector starts or is restarted the connector will fail to start. If there are different fields in other files they will not be detected. The recommended path is to specify a schema that the files will be parsed with. This will ensure that data written by this connector to Kafka will be consistent across files that have inconsistent columns. For example if some files have an optional column that is not always included, create a schema that includes the column marked as optional.
### Tip

To get a starting point for a schema you can use the following command to generate an all String schema. This will give you the basic structure of a schema. From there you can changes the types to match what you expect.

.. code-block:: bash

   mvn clean package
   export CLASSPATH="$(find target/kafka-connect-target/usr/share/kafka-connect/kafka-connect-spooldir -type f -name '*.jar' | tr '\n' ':')"
   kafka-run-class com.github.jcustenborder.kafka.connect.spooldir.AbstractSchemaGenerator -t csv -f src/test/resources/com/github/jcustenborder/kafka/connect/spooldir/csv/FieldsMatch.data -c config/CSVExample.properties -i id

### Configuration

#### File System


##### `error.path`

The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING

*Validator:* Absolute path to a directory that exists and is writable.



##### `input.file.pattern`

Regular expression to check input file names against. This expression must match the entire filename. The equivalent of Matcher.matches().

*Importance:* HIGH

*Type:* STRING



##### `input.path`

The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING

*Validator:* Absolute path to a directory that exists and is writable.



##### `finished.path`

The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* HIGH

*Type:* STRING



##### `halt.on.error`

Should the task halt when it encounters an error or continue to the next file.

*Importance:* HIGH

*Type:* BOOLEAN

*Default Value:* true



##### `cleanup.policy`

Determines how the connector should cleanup the files that have been successfully processed. NONE leaves the files in place which could cause them to be reprocessed if the connector is restarted. DELETE removes the file from the filesystem. MOVE will move the file to a finished directory. MOVEBYDATE will move the file to a finished directory with subdirectories by date

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* MOVE

*Validator:* Matches: ``NONE``, ``DELETE``, ``MOVE``, ``MOVEBYDATE``



##### `task.partitioner`

The task partitioner implementation is used when the connector is configured to use more than one task. This is used by each task to identify which files will be processed by that task. This ensures that each file is only assigned to one task.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* ByName

*Validator:* Matches: ``ByName``



##### `cleanup.policy.maintain.relative.path`

If `input.path.walk.recursively` is enabled in combination with this flag being `true`, the walked sub-directories which contained files will be retained as-is under the `input.path`. The actual files within the sub-directories will moved (with a copy of the sub-dir structure) or deleted as per the `cleanup.policy` defined, but the parent sub-directory structure will remain.

*Importance:* LOW

*Type:* BOOLEAN

*Default Value:* false



##### `file.buffer.size.bytes`

The size of buffer for the BufferedInputStream that will be used to interact with the file system.

*Importance:* LOW

*Type:* INT

*Default Value:* 131072

*Validator:* [1,...]



##### `file.minimum.age.ms`

The amount of time in milliseconds after the file was last written to before the file can be processed.

*Importance:* LOW

*Type:* LONG

*Default Value:* 0

*Validator:* [0,...]



##### `files.sort.attributes`

The attributes each file will use to determine the sort order. `Name` is name of the file. `Length` is the length of the file preferring larger files first. `LastModified` is the LastModified attribute of the file preferring older files first.

*Importance:* LOW

*Type:* LIST

*Default Value:* [NameAsc]

*Validator:* Matches: ``NameAsc``, ``NameDesc``, ``LengthAsc``, ``LengthDesc``, ``LastModifiedAsc``, ``LastModifiedDesc``



##### `input.path.walk.recursively`

If enabled, any sub-directories dropped under `input.path` will be recursively walked looking for files matching the configured `input.file.pattern`. After processing is complete the discovered sub directory structure (as well as files within them) will handled according to the configured `cleanup.policy` (i.e. moved or deleted etc). For each discovered file, the walked sub-directory path will be set as a header named `file.relative.path`

*Importance:* LOW

*Type:* BOOLEAN

*Default Value:* false


##### `processing.file.extension`

Before a file is processed, a flag is created in its directory to indicate the file is being handled. The flag file has the same name as the file, but with this property appended as a suffix.

*Importance:* LOW

*Type:* STRING

*Default Value:* .PROCESSING

*Validator:* Matches regex( ^.*\..+$ )


#### General


##### `topic`

The Kafka topic to write the data to.

*Importance:* HIGH

*Type:* STRING



##### `batch.size`

The number of records that should be returned with each batch.

*Importance:* LOW

*Type:* INT

*Default Value:* 1000



##### `csv.case.sensitive.field.names`

Flag to determine if the field names in the header row should be treated as case sensitive.

*Importance:* LOW

*Type:* BOOLEAN



##### `csv.rfc.4180.parser.enabled`

Flag to determine if the RFC 4180 parser should be used instead of the default parser.

*Importance:* LOW

*Type:* BOOLEAN



##### `empty.poll.wait.ms`

The amount of time to wait if a poll returns an empty list of records.

*Importance:* LOW

*Type:* LONG

*Default Value:* 500

*Validator:* [1,...,9223372036854775807]



##### `task.count`

Internal setting to the connector used to instruct a task on which files to select. The connector will override this setting.

*Importance:* LOW

*Type:* INT

*Default Value:* 1

*Validator:* [1,...]



##### `task.index`

Internal setting to the connector used to instruct a task on which files to select. The connector will override this setting.

*Importance:* LOW

*Type:* INT

*Default Value:* 0

*Validator:* [0,...]


#### Schema


##### `key.schema`

The schema for the key written to Kafka.

*Importance:* HIGH

*Type:* STRING



##### `value.schema`

The schema for the value written to Kafka.

*Importance:* HIGH

*Type:* STRING


#### CSV Parsing


##### `csv.first.row.as.header`

Flag to indicate if the fist row of data contains the header of the file. If true the position of the columns will be determined by the first row to the CSV. The column position will be inferred from the position of the schema supplied in `value.schema`. If set to true the number of columns must be greater than or equal to the number of fields in the schema.

*Importance:* MEDIUM

*Type:* BOOLEAN



##### `csv.escape.char`

The character as an integer to use when a special character is encountered. The default escape character is typically a \(92)

*Importance:* LOW

*Type:* INT

*Default Value:* 92



##### `csv.file.charset`

Character set to read wth file with.

*Importance:* LOW

*Type:* STRING

*Default Value:* UTF-8

*Validator:* Big5,Big5-HKSCS,CESU-8,EUC-JP,EUC-KR,GB18030,GB2312,GBK,IBM-Thai,IBM00858,IBM01140,IBM01141,IBM01142,IBM01143,IBM01144,IBM01145,IBM01146,IBM01147,IBM01148,IBM01149,IBM037,IBM1026,IBM1047,IBM273,IBM277,IBM278,IBM280,IBM284,IBM285,IBM290,IBM297,IBM420,IBM424,IBM437,IBM500,IBM775,IBM850,IBM852,IBM855,IBM857,IBM860,IBM861,IBM862,IBM863,IBM864,IBM865,IBM866,IBM868,IBM869,IBM870,IBM871,IBM918,ISO-2022-CN,ISO-2022-JP,ISO-2022-JP-2,ISO-2022-KR,ISO-8859-1,ISO-8859-13,ISO-8859-15,ISO-8859-16,ISO-8859-2,ISO-8859-3,ISO-8859-4,ISO-8859-5,ISO-8859-6,ISO-8859-7,ISO-8859-8,ISO-8859-9,JIS_X0201,JIS_X0212-1990,KOI8-R,KOI8-U,Shift_JIS,TIS-620,US-ASCII,UTF-16,UTF-16BE,UTF-16LE,UTF-32,UTF-32BE,UTF-32LE,UTF-8,windows-1250,windows-1251,windows-1252,windows-1253,windows-1254,windows-1255,windows-1256,windows-1257,windows-1258,windows-31j,x-Big5-HKSCS-2001,x-Big5-Solaris,x-euc-jp-linux,x-EUC-TW,x-eucJP-Open,x-IBM1006,x-IBM1025,x-IBM1046,x-IBM1097,x-IBM1098,x-IBM1112,x-IBM1122,x-IBM1123,x-IBM1124,x-IBM1129,x-IBM1166,x-IBM1364,x-IBM1381,x-IBM1383,x-IBM29626C,x-IBM300,x-IBM33722,x-IBM737,x-IBM833,x-IBM834,x-IBM856,x-IBM874,x-IBM875,x-IBM921,x-IBM922,x-IBM930,x-IBM933,x-IBM935,x-IBM937,x-IBM939,x-IBM942,x-IBM942C,x-IBM943,x-IBM943C,x-IBM948,x-IBM949,x-IBM949C,x-IBM950,x-IBM964,x-IBM970,x-ISCII91,x-ISO-2022-CN-CNS,x-ISO-2022-CN-GB,x-iso-8859-11,x-JIS0208,x-JISAutoDetect,x-Johab,x-MacArabic,x-MacCentralEurope,x-MacCroatian,x-MacCyrillic,x-MacDingbat,x-MacGreek,x-MacHebrew,x-MacIceland,x-MacRoman,x-MacRomania,x-MacSymbol,x-MacThai,x-MacTurkish,x-MacUkraine,x-MS932_0213,x-MS950-HKSCS,x-MS950-HKSCS-XP,x-mswin-936,x-PCK,x-SJIS_0213,x-UTF-16LE-BOM,X-UTF-32BE-BOM,X-UTF-32LE-BOM,x-windows-50220,x-windows-50221,x-windows-874,x-windows-949,x-windows-950,x-windows-iso2022jp



##### `csv.ignore.leading.whitespace`

Sets the ignore leading whitespace setting - if true, white space in front of a quote in a field is ignored.

*Importance:* LOW

*Type:* BOOLEAN

*Default Value:* true



##### `csv.ignore.quotations`

Sets the ignore quotations mode - if true, quotations are ignored.

*Importance:* LOW

*Type:* BOOLEAN



##### `csv.keep.carriage.return`

Flag to determine if the carriage return at the end of the line should be maintained.

*Importance:* LOW

*Type:* BOOLEAN



##### `csv.null.field.indicator`

Indicator to determine how the CSV Reader can determine if a field is null. Valid values are EMPTY_SEPARATORS, EMPTY_QUOTES, BOTH, NEITHER. For more information see http://opencsv.sourceforge.net/apidocs/com/opencsv/enums/CSVReaderNullFieldIndicator.html.

*Importance:* LOW

*Type:* STRING

*Default Value:* NEITHER

*Validator:* Matches: ``EMPTY_SEPARATORS``, ``EMPTY_QUOTES``, ``BOTH``, ``NEITHER``



##### `csv.quote.char`

The character that is used to quote a field. This typically happens when the csv.separator.char character is within the data.

*Importance:* LOW

*Type:* INT

*Default Value:* 34



##### `csv.separator.char`

The character that separates each field in the form of an integer. Typically in a CSV this is a ,(44) character. A TSV would use a tab(9) character. If `csv.separator.char` is defined as a null(0), then the RFC 4180 parser must be utilized by default. This is the equivalent of `csv.rfc.4180.parser.enabled = true`.

*Importance:* LOW

*Type:* INT

*Default Value:* 44



##### `csv.skip.lines`

Number of lines to skip in the beginning of the file.

*Importance:* LOW

*Type:* INT

*Default Value:* 0



##### `csv.strict.quotes`

Sets the strict quotes setting - if true, characters outside the quotes are ignored.

*Importance:* LOW

*Type:* BOOLEAN



##### `csv.verify.reader`

Flag to determine if the reader should be verified.

*Importance:* LOW

*Type:* BOOLEAN

*Default Value:* true


#### Schema Generation


##### `schema.generation.enabled`

Flag to determine if schemas should be dynamically generated. If set  to true, `key.schema` and `value.schema` can be omitted, but `schema.generation.key.name` and `schema.generation.value.name` must be set.

*Importance:* MEDIUM

*Type:* BOOLEAN



##### `schema.generation.key.fields`

The field(s) to use to build a key schema. This is only used during schema generation.

*Importance:* MEDIUM

*Type:* LIST



##### `schema.generation.key.name`

The name of the generated key schema.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* com.github.jcustenborder.kafka.connect.model.Key



##### `schema.generation.value.name`

The name of the generated value schema.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* com.github.jcustenborder.kafka.connect.model.Value


#### Timestamps


##### `timestamp.field`

The field in the value schema that will contain the parsed timestamp for the record. This field cannot be marked as optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html)

*Importance:* MEDIUM

*Type:* STRING



##### `timestamp.mode`

Determines how the connector will set the timestamp for the [ConnectRecord](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/connector/ConnectRecord.html#timestamp()). If set to `Field` then the timestamp will be read from a field in the value. This field cannot be optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html). Specify the field  in `timestamp.field`. If set to `FILE_TIME` then the last modified time of the file will be used. If set to `PROCESS_TIME` the time the record is read will be used.

*Importance:* MEDIUM

*Type:* STRING

*Default Value:* PROCESS_TIME

*Validator:* Matches: ``FIELD``, ``FILE_TIME``, ``PROCESS_TIME``



##### `parser.timestamp.date.formats`

The date formats that are expected in the file. This is a list of strings that will be used to parse the date fields in order. The most accurate date format should be the first in the list. Take a look at the Java documentation for more info. https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html

*Importance:* LOW

*Type:* LIST

*Default Value:* [yyyy-MM-dd'T'HH:mm:ss, yyyy-MM-dd' 'HH:mm:ss]



##### `parser.timestamp.timezone`

The timezone that all of the dates will be parsed with.

*Importance:* LOW

*Type:* STRING

*Default Value:* UTC






# Development

## Building the source

```bash
mvn clean package
```

## Contributions

Contributions are always welcomed! Before you start any development please create an issue and
start a discussion. Create a pull request against your newly created issue and we're happy to see
if we can merge your pull request. First and foremost any time you're adding code to the code base
you need to include test coverage. Make sure that you run `mvn clean package` before submitting your
pull to ensure that all of the tests, checkstyle rules, and the package can be successfully built.