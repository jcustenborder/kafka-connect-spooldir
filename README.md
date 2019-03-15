
# Introduction

This Kafka Connect connector provides the capability to watch a directory for files and read the data as new files are written to the input directory. Each of the records in the input file will be converted based on the user supplied schema.

The CSVRecordProcessor supports reading CSV or TSV files. It can convert a CSV on the fly to the strongly typed Kafka Connect data types. It currently has support for all of the schema types and logical types that are supported in Kafka Connect. If you couple this with the Avro converter and Schema Registry by Confluent, you will be able to process CSV, Json, or TSV files to strongly typed Avro data in real time.

### Warning

Running these connectors with multiple tasks requires a shared volume across all of the Kafka Connect workers. Kafka Connect does not have a mechanism for synchronization of tasks. Because of this each task will select which file it will use the following algorithm `hash(<filename>) % totalTasks == taskNumber`. If you are not using a shared volume this could cause issues where files are not processed. Using more than one task could also affect the order that the data is written to Kafka.

# Source Connectors


## CSV Source Connector

The SpoolDirCsvSourceConnector will monitor the directory specified in `input.path` for files and read them as a CSV converting each of the records to the strongly typed equivalent specified in `key.schema` and `value.schema`.



### Configuration

#### CSV Parsing


##### `csv.first.row.as.header`

Flag to indicate if the fist row of data contains the header of the file. If true the position of the columns will be determined by the first row to the CSV. The column position will be inferred from the position of the schema supplied in `value.schema`. If set to true the number of columns must be greater than or equal to the number of fields in the schema.

*Importance:* Medium

*Type:* Boolean

*Default Value:* false



##### `csv.escape.char`

The character as an integer to use when a special character is encountered. The default escape character is typically a \(92)

*Importance:* Low

*Type:* Int

*Default Value:* 92



##### `csv.file.charset`

Character set to read wth file with.

*Importance:* Low

*Type:* String

*Default Value:* UTF-8

*Validator:* Big5,Big5-HKSCS,CESU-8,EUC-JP,EUC-KR,GB18030,GB2312,GBK,IBM-Thai,IBM00858,IBM01140,IBM01141,IBM01142,IBM01143,IBM01144,IBM01145,IBM01146,IBM01147,IBM01148,IBM01149,IBM037,IBM1026,IBM1047,IBM273,IBM277,IBM278,IBM280,IBM284,IBM285,IBM290,IBM297,IBM420,IBM424,IBM437,IBM500,IBM775,IBM850,IBM852,IBM855,IBM857,IBM860,IBM861,IBM862,IBM863,IBM864,IBM865,IBM866,IBM868,IBM869,IBM870,IBM871,IBM918,ISO-2022-CN,ISO-2022-JP,ISO-2022-JP-2,ISO-2022-KR,ISO-8859-1,ISO-8859-13,ISO-8859-15,ISO-8859-2,ISO-8859-3,ISO-8859-4,ISO-8859-5,ISO-8859-6,ISO-8859-7,ISO-8859-8,ISO-8859-9,JIS_X0201,JIS_X0212-1990,KOI8-R,KOI8-U,Shift_JIS,TIS-620,US-ASCII,UTF-16,UTF-16BE,UTF-16LE,UTF-32,UTF-32BE,UTF-32LE,UTF-8,windows-1250,windows-1251,windows-1252,windows-1253,windows-1254,windows-1255,windows-1256,windows-1257,windows-1258,windows-31j,x-Big5-HKSCS-2001,x-Big5-Solaris,x-COMPOUND_TEXT,x-euc-jp-linux,x-EUC-TW,x-eucJP-Open,x-IBM1006,x-IBM1025,x-IBM1046,x-IBM1097,x-IBM1098,x-IBM1112,x-IBM1122,x-IBM1123,x-IBM1124,x-IBM1166,x-IBM1364,x-IBM1381,x-IBM1383,x-IBM300,x-IBM33722,x-IBM737,x-IBM833,x-IBM834,x-IBM856,x-IBM874,x-IBM875,x-IBM921,x-IBM922,x-IBM930,x-IBM933,x-IBM935,x-IBM937,x-IBM939,x-IBM942,x-IBM942C,x-IBM943,x-IBM943C,x-IBM948,x-IBM949,x-IBM949C,x-IBM950,x-IBM964,x-IBM970,x-ISCII91,x-ISO-2022-CN-CNS,x-ISO-2022-CN-GB,x-iso-8859-11,x-JIS0208,x-JISAutoDetect,x-Johab,x-MacArabic,x-MacCentralEurope,x-MacCroatian,x-MacCyrillic,x-MacDingbat,x-MacGreek,x-MacHebrew,x-MacIceland,x-MacRoman,x-MacRomania,x-MacSymbol,x-MacThai,x-MacTurkish,x-MacUkraine,x-MS932_0213,x-MS950-HKSCS,x-MS950-HKSCS-XP,x-mswin-936,x-PCK,x-SJIS_0213,x-UTF-16LE-BOM,X-UTF-32BE-BOM,X-UTF-32LE-BOM,x-windows-50220,x-windows-50221,x-windows-874,x-windows-949,x-windows-950,x-windows-iso2022jp



##### `csv.ignore.leading.whitespace`

Sets the ignore leading whitespace setting - if true, white space in front of a quote in a field is ignored.

*Importance:* Low

*Type:* Boolean

*Default Value:* true



##### `csv.ignore.quotations`

Sets the ignore quotations mode - if true, quotations are ignored.

*Importance:* Low

*Type:* Boolean

*Default Value:* false



##### `csv.keep.carriage.return`

Flag to determine if the carriage return at the end of the line should be maintained.

*Importance:* Low

*Type:* Boolean

*Default Value:* false



##### `csv.null.field.indicator`

Indicator to determine how the CSV Reader can determine if a field is null. Valid values are EMPTY_SEPARATORS, EMPTY_QUOTES, BOTH, NEITHER. For more information see http://opencsv.sourceforge.net/apidocs/com/opencsv/enums/CSVReaderNullFieldIndicator.html.

*Importance:* Low

*Type:* String

*Default Value:* NEITHER

*Validator:* Matches: ``EMPTY_SEPARATORS``, ``EMPTY_QUOTES``, ``BOTH``, ``NEITHER``



##### `csv.quote.char`

The character that is used to quote a field. This typically happens when the csv.separator.char character is within the data.

*Importance:* Low

*Type:* Int

*Default Value:* 34



##### `csv.separator.char`

The character that separates each field in the form of an integer. Typically in a CSV this is a ,(44) character. A TSV would use a tab(9) character.

*Importance:* Low

*Type:* Int

*Default Value:* 44



##### `csv.skip.lines`

Number of lines to skip in the beginning of the file.

*Importance:* Low

*Type:* Int

*Default Value:* 0



##### `csv.strict.quotes`

Sets the strict quotes setting - if true, characters outside the quotes are ignored.

*Importance:* Low

*Type:* Boolean

*Default Value:* false



##### `csv.verify.reader`

Flag to determine if the reader should be verified.

*Importance:* Low

*Type:* Boolean

*Default Value:* true


#### File System


##### `error.path`

The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.

*Importance:* High

*Type:* String

*Validator:* Absolute path to a directory that exists and is writable.



##### `input.file.pattern`

Regular expression to check input file names against. This expression must match the entire filename. The equivalent of Matcher.matches().

*Importance:* High

*Type:* String



##### `input.path`

The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* High

*Type:* String

*Validator:* Absolute path to a directory that exists and is writable.



##### `finished.path`

The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* High

*Type:* String



##### `halt.on.error`

Should the task halt when it encounters an error or continue to the next file.

*Importance:* High

*Type:* Boolean

*Default Value:* true



##### `cleanup.policy`

Determines how the connector should cleanup the files that have been successfully processed. NONE leaves the files in place which could cause them to be reprocessed if the connector is restarted. DELETE removes the file from the filesystem. MOVE will move the file to a finished directory.

*Importance:* Medium

*Type:* String

*Default Value:* MOVE

*Validator:* Matches: ``NONE``, ``DELETE``, ``MOVE``



##### `file.minimum.age.ms`

The amount of time in milliseconds after the file was last written to before the file can be processed.

*Importance:* Low

*Type:* Long

*Default Value:* 0

*Validator:* [0,...]



##### `processing.file.extension`

Before a file is processed, it is renamed to indicate that it is currently being processed. This setting is appended to the end of the file.

*Importance:* Low

*Type:* String

*Default Value:* .PROCESSING

*Validator:* Matches regex( ^.*\..+$ )


#### Schema


##### `key.schema`

The schema for the key written to Kafka.

*Importance:* High

*Type:* String



##### `value.schema`

The schema for the value written to Kafka.

*Importance:* High

*Type:* String


#### Schema Generation


##### `schema.generation.enabled`

Flag to determine if schemas should be dynamically generated. If set  to true, `key.schema` and `value.schema` can be omitted, but `schema.generation.key.name` and `schema.generation.value.name` must be set.

*Importance:* Medium

*Type:* Boolean

*Default Value:* false



##### `schema.generation.key.fields`

The field(s) to use to build a key schema. This is only used during schema generation.

*Importance:* Medium

*Type:* List

*Default Value:* []



##### `schema.generation.key.name`

The name of the generated key schema.

*Importance:* Medium

*Type:* String

*Default Value:* com.github.jcustenborder.kafka.connect.model.Key



##### `schema.generation.value.name`

The name of the generated value schema.

*Importance:* Medium

*Type:* String

*Default Value:* com.github.jcustenborder.kafka.connect.model.Value


#### Timestamps


##### `timestamp.field`

The field in the value schema that will contain the parsed timestamp for the record. This field cannot be marked as optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html)

*Importance:* Medium

*Type:* String



##### `timestamp.mode`

Determines how the connector will set the timestamp for the [ConnectRecord](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/connector/ConnectRecord.html#timestamp()). If set to `Field` then the timestamp will be read from a field in the value. This field cannot be optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html). Specify the field  in `timestamp.field`. If set to `FILE_TIME` then the last modified time of the file will be used. If set to `PROCESS_TIME` the time the record is read will be used.

*Importance:* Medium

*Type:* String

*Default Value:* PROCESS_TIME

*Validator:* Matches: ``FIELD``, ``FILE_TIME``, ``PROCESS_TIME``



##### `parser.timestamp.date.formats`

The date formats that are expected in the file. This is a list of strings that will be used to parse the date fields in order. The most accurate date format should be the first in the list. Take a look at the Java documentation for more info. https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html

*Importance:* Low

*Type:* List

*Default Value:* [yyyy-MM-dd'T'HH:mm:ss, yyyy-MM-dd' 'HH:mm:ss]



##### `parser.timestamp.timezone`

The timezone that all of the dates will be parsed with.

*Importance:* Low

*Type:* String

*Default Value:* UTC


#### General


##### `topic`

The Kafka topic to write the data to.

*Importance:* High

*Type:* String



##### `batch.size`

The number of records that should be returned with each batch.

*Importance:* Low

*Type:* Int

*Default Value:* 1000



##### `csv.case.sensitive.field.names`

Flag to determine if the field names in the header row should be treated as case sensitive.

*Importance:* Low

*Type:* Boolean

*Default Value:* false



##### `empty.poll.wait.ms`

The amount of time to wait if a poll returns an empty list of records.

*Importance:* Low

*Type:* Long

*Default Value:* 250

*Validator:* [1,...,9223372036854775807]





## Json Source Connector

This connector is used to `stream <https://en.wikipedia.org/wiki/JSON_Streaming>` JSON files from a directory while converting the data based on the schema supplied in the configuration.



### Configuration

#### File System


##### `error.path`

The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.

*Importance:* High

*Type:* String

*Validator:* Absolute path to a directory that exists and is writable.



##### `input.file.pattern`

Regular expression to check input file names against. This expression must match the entire filename. The equivalent of Matcher.matches().

*Importance:* High

*Type:* String



##### `input.path`

The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* High

*Type:* String

*Validator:* Absolute path to a directory that exists and is writable.



##### `finished.path`

The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* High

*Type:* String



##### `halt.on.error`

Should the task halt when it encounters an error or continue to the next file.

*Importance:* High

*Type:* Boolean

*Default Value:* true



##### `cleanup.policy`

Determines how the connector should cleanup the files that have been successfully processed. NONE leaves the files in place which could cause them to be reprocessed if the connector is restarted. DELETE removes the file from the filesystem. MOVE will move the file to a finished directory.

*Importance:* Medium

*Type:* String

*Default Value:* MOVE

*Validator:* Matches: ``NONE``, ``DELETE``, ``MOVE``



##### `file.minimum.age.ms`

The amount of time in milliseconds after the file was last written to before the file can be processed.

*Importance:* Low

*Type:* Long

*Default Value:* 0

*Validator:* [0,...]



##### `processing.file.extension`

Before a file is processed, it is renamed to indicate that it is currently being processed. This setting is appended to the end of the file.

*Importance:* Low

*Type:* String

*Default Value:* .PROCESSING

*Validator:* Matches regex( ^.*\..+$ )


#### Schema


##### `key.schema`

The schema for the key written to Kafka.

*Importance:* High

*Type:* String



##### `value.schema`

The schema for the value written to Kafka.

*Importance:* High

*Type:* String


#### Schema Generation


##### `schema.generation.enabled`

Flag to determine if schemas should be dynamically generated. If set  to true, `key.schema` and `value.schema` can be omitted, but `schema.generation.key.name` and `schema.generation.value.name` must be set.

*Importance:* Medium

*Type:* Boolean

*Default Value:* false



##### `schema.generation.key.fields`

The field(s) to use to build a key schema. This is only used during schema generation.

*Importance:* Medium

*Type:* List

*Default Value:* []



##### `schema.generation.key.name`

The name of the generated key schema.

*Importance:* Medium

*Type:* String

*Default Value:* com.github.jcustenborder.kafka.connect.model.Key



##### `schema.generation.value.name`

The name of the generated value schema.

*Importance:* Medium

*Type:* String

*Default Value:* com.github.jcustenborder.kafka.connect.model.Value


#### Timestamps


##### `timestamp.field`

The field in the value schema that will contain the parsed timestamp for the record. This field cannot be marked as optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html)

*Importance:* Medium

*Type:* String



##### `timestamp.mode`

Determines how the connector will set the timestamp for the [ConnectRecord](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/connector/ConnectRecord.html#timestamp()). If set to `Field` then the timestamp will be read from a field in the value. This field cannot be optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html). Specify the field  in `timestamp.field`. If set to `FILE_TIME` then the last modified time of the file will be used. If set to `PROCESS_TIME` the time the record is read will be used.

*Importance:* Medium

*Type:* String

*Default Value:* PROCESS_TIME

*Validator:* Matches: ``FIELD``, ``FILE_TIME``, ``PROCESS_TIME``



##### `parser.timestamp.date.formats`

The date formats that are expected in the file. This is a list of strings that will be used to parse the date fields in order. The most accurate date format should be the first in the list. Take a look at the Java documentation for more info. https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html

*Importance:* Low

*Type:* List

*Default Value:* [yyyy-MM-dd'T'HH:mm:ss, yyyy-MM-dd' 'HH:mm:ss]



##### `parser.timestamp.timezone`

The timezone that all of the dates will be parsed with.

*Importance:* Low

*Type:* String

*Default Value:* UTC


#### General


##### `topic`

The Kafka topic to write the data to.

*Importance:* High

*Type:* String



##### `batch.size`

The number of records that should be returned with each batch.

*Importance:* Low

*Type:* Int

*Default Value:* 1000



##### `empty.poll.wait.ms`

The amount of time to wait if a poll returns an empty list of records.

*Importance:* Low

*Type:* Long

*Default Value:* 250

*Validator:* [1,...,9223372036854775807]





## Line Delimited Source Connector

This connector is used to read a file line by line and write the data to Kafka.

### Important

The recommended converter to use is the StringConverter. Example: `value.converter=org.apache.kafka.connect.storage.StringConverter`


### Configuration

#### File System


##### `error.path`

The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.

*Importance:* High

*Type:* String

*Validator:* Absolute path to a directory that exists and is writable.



##### `input.file.pattern`

Regular expression to check input file names against. This expression must match the entire filename. The equivalent of Matcher.matches().

*Importance:* High

*Type:* String



##### `input.path`

The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* High

*Type:* String

*Validator:* Absolute path to a directory that exists and is writable.



##### `finished.path`

The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* High

*Type:* String



##### `halt.on.error`

Should the task halt when it encounters an error or continue to the next file.

*Importance:* High

*Type:* Boolean

*Default Value:* true



##### `cleanup.policy`

Determines how the connector should cleanup the files that have been successfully processed. NONE leaves the files in place which could cause them to be reprocessed if the connector is restarted. DELETE removes the file from the filesystem. MOVE will move the file to a finished directory.

*Importance:* Medium

*Type:* String

*Default Value:* MOVE

*Validator:* Matches: ``NONE``, ``DELETE``, ``MOVE``



##### `file.minimum.age.ms`

The amount of time in milliseconds after the file was last written to before the file can be processed.

*Importance:* Low

*Type:* Long

*Default Value:* 0

*Validator:* [0,...]



##### `processing.file.extension`

Before a file is processed, it is renamed to indicate that it is currently being processed. This setting is appended to the end of the file.

*Importance:* Low

*Type:* String

*Default Value:* .PROCESSING

*Validator:* Matches regex( ^.*\..+$ )


#### Timestamps


##### `timestamp.mode`

Determines how the connector will set the timestamp for the [ConnectRecord](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/connector/ConnectRecord.html#timestamp()). If set to `Field` then the timestamp will be read from a field in the value. This field cannot be optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html). Specify the field  in `timestamp.field`. If set to `FILE_TIME` then the last modified time of the file will be used. If set to `PROCESS_TIME` the time the record is read will be used.

*Importance:* Medium

*Type:* String

*Default Value:* PROCESS_TIME

*Validator:* Matches: ``FIELD``, ``FILE_TIME``, ``PROCESS_TIME``


#### General


##### `topic`

The Kafka topic to write the data to.

*Importance:* High

*Type:* String



##### `batch.size`

The number of records that should be returned with each batch.

*Importance:* Low

*Type:* Int

*Default Value:* 1000



##### `empty.poll.wait.ms`

The amount of time to wait if a poll returns an empty list of records.

*Importance:* Low

*Type:* Long

*Default Value:* 250

*Validator:* [1,...,9223372036854775807]





## Schema Less Json Source Connector

This connector is used to `stream <https://en.wikipedia.org/wiki/JSON_Streaming>_` JSON files from a directory while converting the data based on the schema supplied in the configuration.

### Important

This connector does not try to convert the json records to a schema. The recommended converter to use is the StringConverter. Example: `value.converter=org.apache.kafka.connect.storage.StringConverter`


### Configuration

#### File System


##### `error.path`

The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.

*Importance:* High

*Type:* String

*Validator:* Absolute path to a directory that exists and is writable.



##### `input.file.pattern`

Regular expression to check input file names against. This expression must match the entire filename. The equivalent of Matcher.matches().

*Importance:* High

*Type:* String



##### `input.path`

The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* High

*Type:* String

*Validator:* Absolute path to a directory that exists and is writable.



##### `finished.path`

The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* High

*Type:* String



##### `halt.on.error`

Should the task halt when it encounters an error or continue to the next file.

*Importance:* High

*Type:* Boolean

*Default Value:* true



##### `cleanup.policy`

Determines how the connector should cleanup the files that have been successfully processed. NONE leaves the files in place which could cause them to be reprocessed if the connector is restarted. DELETE removes the file from the filesystem. MOVE will move the file to a finished directory.

*Importance:* Medium

*Type:* String

*Default Value:* MOVE

*Validator:* Matches: ``NONE``, ``DELETE``, ``MOVE``



##### `file.minimum.age.ms`

The amount of time in milliseconds after the file was last written to before the file can be processed.

*Importance:* Low

*Type:* Long

*Default Value:* 0

*Validator:* [0,...]



##### `processing.file.extension`

Before a file is processed, it is renamed to indicate that it is currently being processed. This setting is appended to the end of the file.

*Importance:* Low

*Type:* String

*Default Value:* .PROCESSING

*Validator:* Matches regex( ^.*\..+$ )


#### Timestamps


##### `timestamp.mode`

Determines how the connector will set the timestamp for the [ConnectRecord](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/connector/ConnectRecord.html#timestamp()). If set to `Field` then the timestamp will be read from a field in the value. This field cannot be optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html). Specify the field  in `timestamp.field`. If set to `FILE_TIME` then the last modified time of the file will be used. If set to `PROCESS_TIME` the time the record is read will be used.

*Importance:* Medium

*Type:* String

*Default Value:* PROCESS_TIME

*Validator:* Matches: ``FIELD``, ``FILE_TIME``, ``PROCESS_TIME``


#### General


##### `topic`

The Kafka topic to write the data to.

*Importance:* High

*Type:* String



##### `batch.size`

The number of records that should be returned with each batch.

*Importance:* Low

*Type:* Int

*Default Value:* 1000



##### `empty.poll.wait.ms`

The amount of time to wait if a poll returns an empty list of records.

*Importance:* Low

*Type:* Long

*Default Value:* 250

*Validator:* [1,...,9223372036854775807]





## Extended Log File Format Source Connector

This connector is used to stream `Extended Log File Format <https://www.w3.org/TR/WD-logfile.html>` files from a directory while converting the data to a strongly typed schema.



### Configuration

#### File System


##### `error.path`

The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.

*Importance:* High

*Type:* String

*Validator:* Absolute path to a directory that exists and is writable.



##### `input.file.pattern`

Regular expression to check input file names against. This expression must match the entire filename. The equivalent of Matcher.matches().

*Importance:* High

*Type:* String



##### `input.path`

The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* High

*Type:* String

*Validator:* Absolute path to a directory that exists and is writable.



##### `finished.path`

The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.

*Importance:* High

*Type:* String



##### `halt.on.error`

Should the task halt when it encounters an error or continue to the next file.

*Importance:* High

*Type:* Boolean

*Default Value:* true



##### `cleanup.policy`

Determines how the connector should cleanup the files that have been successfully processed. NONE leaves the files in place which could cause them to be reprocessed if the connector is restarted. DELETE removes the file from the filesystem. MOVE will move the file to a finished directory.

*Importance:* Medium

*Type:* String

*Default Value:* MOVE

*Validator:* Matches: ``NONE``, ``DELETE``, ``MOVE``



##### `file.minimum.age.ms`

The amount of time in milliseconds after the file was last written to before the file can be processed.

*Importance:* Low

*Type:* Long

*Default Value:* 0

*Validator:* [0,...]



##### `processing.file.extension`

Before a file is processed, it is renamed to indicate that it is currently being processed. This setting is appended to the end of the file.

*Importance:* Low

*Type:* String

*Default Value:* .PROCESSING

*Validator:* Matches regex( ^.*\..+$ )


#### Schema


##### `key.schema`

The schema for the key written to Kafka.

*Importance:* High

*Type:* String



##### `value.schema`

The schema for the value written to Kafka.

*Importance:* High

*Type:* String


#### Schema Generation


##### `schema.generation.enabled`

Flag to determine if schemas should be dynamically generated. If set  to true, `key.schema` and `value.schema` can be omitted, but `schema.generation.key.name` and `schema.generation.value.name` must be set.

*Importance:* Medium

*Type:* Boolean

*Default Value:* false



##### `schema.generation.key.fields`

The field(s) to use to build a key schema. This is only used during schema generation.

*Importance:* Medium

*Type:* List

*Default Value:* []



##### `schema.generation.key.name`

The name of the generated key schema.

*Importance:* Medium

*Type:* String

*Default Value:* com.github.jcustenborder.kafka.connect.model.Key



##### `schema.generation.value.name`

The name of the generated value schema.

*Importance:* Medium

*Type:* String

*Default Value:* com.github.jcustenborder.kafka.connect.model.Value


#### Timestamps


##### `timestamp.field`

The field in the value schema that will contain the parsed timestamp for the record. This field cannot be marked as optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html)

*Importance:* Medium

*Type:* String



##### `timestamp.mode`

Determines how the connector will set the timestamp for the [ConnectRecord](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/connector/ConnectRecord.html#timestamp()). If set to `Field` then the timestamp will be read from a field in the value. This field cannot be optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html). Specify the field  in `timestamp.field`. If set to `FILE_TIME` then the last modified time of the file will be used. If set to `PROCESS_TIME` the time the record is read will be used.

*Importance:* Medium

*Type:* String

*Default Value:* PROCESS_TIME

*Validator:* Matches: ``FIELD``, ``FILE_TIME``, ``PROCESS_TIME``



##### `parser.timestamp.date.formats`

The date formats that are expected in the file. This is a list of strings that will be used to parse the date fields in order. The most accurate date format should be the first in the list. Take a look at the Java documentation for more info. https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html

*Importance:* Low

*Type:* List

*Default Value:* [yyyy-MM-dd'T'HH:mm:ss, yyyy-MM-dd' 'HH:mm:ss]



##### `parser.timestamp.timezone`

The timezone that all of the dates will be parsed with.

*Importance:* Low

*Type:* String

*Default Value:* UTC


#### General


##### `topic`

The Kafka topic to write the data to.

*Importance:* High

*Type:* String



##### `elf.separator.char`

The character that separates each field in the form of an integer. Typically in a CSV this is a TAB(9) or SPACE(32).

*Importance:* High

*Type:* Int

*Default Value:* 9



##### `batch.size`

The number of records that should be returned with each batch.

*Importance:* Low

*Type:* Int

*Default Value:* 1000



##### `empty.poll.wait.ms`

The amount of time to wait if a poll returns an empty list of records.

*Importance:* Low

*Type:* Long

*Default Value:* 250

*Validator:* [1,...,9223372036854775807]






