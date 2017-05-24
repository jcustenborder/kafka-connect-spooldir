# Overview

This Kafka Connect connector provides the capability to watch a directory for files and read the data as new files are
written to the input directory. Each of the records in the input file will be converted based on the user supplied schema. 

The CSVRecordProcessor supports reading CSV or TSV files. It can convert a CSV on the fly to the strongly typed Kafka
Connect data types. It currently has support for all of the schema types and logical types that are supported in Kafka 0.10.x.
If you couple this with the Avro converter and Schema Registry by Confluent, you will be able to process csv files to
strongly typed Avro data in real time.

# Schema Configuration

This connector requires you to specify the schema of the files to be read. This is controlled by the
`key.schema` and the `value.schema` configuration settings. This setting is a schema in json form stored as a string. The 
field schemas are used to parse the data in each row. This allows the user to strongly define the types for each field. 

## Generating Schemas

### Csv

```bash
mvn clean package
export CLASSPATH="$(find target/kafka-connect-target/usr/share/java -type f -name '*.jar' | tr '\n' ':')"
kafka-run-class com.github.jcustenborder.kafka.connect.spooldir.SchemaGenerator -t csv -f src/test/resources/com/github/jcustenborder/kafka/connect/spooldir/csv/FieldsMatch.data -c config/CSVExample.properties -i id
```

### Json

```bash
mvn clean package
export CLASSPATH="$(find target/kafka-connect-target/usr/share/java -type f -name '*.jar' | tr '\n' ':')"
kafka-run-class com.github.jcustenborder.kafka.connect.spooldir.SchemaGenerator -t json -f src/test/resources/com/github/jcustenborder/kafka/connect/spooldir/json/FieldsMatch.data -c config/JsonExample.properties -i id
```

## Key schema example 

```json
{
    "name" : "com.example.users.UserKey",
    "type" : "STRUCT",
    "isOptional" : false,
    "fieldSchemas" : {
        "id" : {
          "type" : "INT64",
          "isOptional" : false
        }
    }
}
```

## Value schema example

```json
{
  "name" : "com.example.users.User",
  "type" : "STRUCT",
  "isOptional" : false,
  "fieldSchemas" : {
    "id" : {
      "type" : "INT64",
      "isOptional" : false
    },
    "first_name" : {
      "type" : "STRING",
      "isOptional" : true
    },
    "last_name" : {
      "type" : "STRING",
      "isOptional" : true
    },
    "email" : {
      "type" : "STRING",
      "isOptional" : true
    },
    "gender" : {
      "type" : "STRING",
      "isOptional" : true
    },
    "ip_address" : {
      "type" : "STRING",
      "isOptional" : true
    },
    "last_login" : {
      "name" : "org.apache.kafka.connect.data.Timestamp",
      "type" : "INT64",
      "version" : 1,
      "isOptional" : true
    },
    "account_balance" : {
      "name" : "org.apache.kafka.connect.data.Decimal",
      "type" : "BYTES",
      "version" : 1,
      "parameters" : {
        "scale" : "2"
      },
      "isOptional" : true
    },
    "country" : {
      "type" : "STRING",
      "isOptional" : true
    },
    "favorite_color" : {
      "type" : "STRING",
      "isOptional" : true
    }
  }
}
```

## Logical Type Examples

### Timestamp

```json
{
  "name" : "org.apache.kafka.connect.data.Timestamp",
  "type" : "INT64",
  "version" : 1,
  "isOptional" : true
}
```

### Decimal

```json
{
  "name" : "org.apache.kafka.connect.data.Decimal",
  "type" : "BYTES",
  "version" : 1,
  "parameters" : {
    "scale" : "2"
  },
  "isOptional" : true
}
```

### Date

```json
{
  "name" : "org.apache.kafka.connect.data.Date",
  "type" : "INT32",
  "version" : 1,
  "isOptional" : true
}
```

### Time

```json
{
  "name" : "org.apache.kafka.connect.data.Time",
  "type" : "INT32",
  "version" : 1,
  "isOptional" : true
}
```

# Configuration

## SpoolDirCsvSourceConnector

The `SpoolDirCsvSourceConnector` will monitor the directory specified in `input.path` for files and read them as a CSV converting each of the records to the strongly typed equavalent specified in `key.schema` and `value.schema`.

```properties
name=connector1
tasks.max=1
connector.class=com.github.jcustenborder.kafka.connect.spooldir.SpoolDirCsvSourceConnector

# Set these required values
finished.path=
input.file.pattern=
error.path=
value.schema=
key.schema=
topic=
input.path=
```

| Name                           | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | Type    | Default                                        | Valid Values                                                                                         | Importance |
|--------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|------------------------------------------------|------------------------------------------------------------------------------------------------------|------------|
| error.path                     | The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | string  |                                                |                                                                                                      | high       |
| finished.path                  | The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.                                                                                                                                                                                                                                                                                                                                                                                                                                                     | string  |                                                |                                                                                                      | high       |
| input.file.pattern             | Regular expression to check input file names against. This expression must match the entire filename. The equivalent of Matcher.matches().                                                                                                                                                                                                                                                                                                                                                                                                                                                           | string  |                                                |                                                                                                      | high       |
| input.path                     | The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | string  |                                                |                                                                                                      | high       |
| key.schema                     | The schema for the key written to Kafka.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | string  |                                                |                                                                                                      | high       |
| topic                          | The Kafka topic to write the data to.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | string  |                                                |                                                                                                      | high       |
| value.schema                   | The schema for the value written to Kafka.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | string  |                                                |                                                                                                      | high       |
| halt.on.error                  | Should the task halt when it encounters an error or continue to the next file.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | boolean | true                                           |                                                                                                      | high       |
| csv.first.row.as.header        | Flag to indicate if the fist row of data contains the header of the file. If true the position of the columns will be determined by the first row to the CSV. The column position will be inferred from the position of the schema supplied in `value.schema`. If set to true the number of columns must be greater than or equal to the number of fields in the schema.                                                                                                                                                                                                                             | boolean | false                                          |                                                                                                      | medium     |
| timestamp.field                | The field in the value schema that will contain the parsed timestamp for the record. This field cannot be marked as optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html)                                                                                                                                                                                                                                                                                                                                                              | string  | ""                                             |                                                                                                      | medium     |
| timestamp.mode                 | Determines how the connector will set the timestamp for the [ConnectRecord](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/connector/ConnectRecord.html#timestamp()). If set to `Field` then the timestamp will be read from a field in the value. This field cannot be optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html). Specify the field  in `timestamp.field`. If set to `FILE_TIME` then the last modified time of the file will be used. If set to `PROCESS_TIME` the time the record is read will be used. | string  | PROCESS_TIME                                   | ValidEnum{enum=TimestampMode, allowed=[FIELD, FILE_TIME, PROCESS_TIME]}                              | medium     |
| batch.size                     | The number of records that should be returned with each batch.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | int     | 1000                                           |                                                                                                      | low        |
| csv.case.sensitive.field.names | Flag to determine if the field names in the header row should be treated as case sensitive.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | boolean | false                                          |                                                                                                      | low        |
| csv.escape.char                | Escape character.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | int     | 92                                             |                                                                                                      | low        |
| csv.file.charset               | Character set to read wth file with.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | string  | UTF-8                                          |                                                                                                      | low        |
| csv.ignore.leading.whitespace  | Sets the ignore leading whitespace setting - if true, white space in front of a quote in a field is ignored.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | boolean | true                                           |                                                                                                      | low        |
| csv.ignore.quotations          | Sets the ignore quotations mode - if true, quotations are ignored.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | boolean | false                                          |                                                                                                      | low        |
| csv.keep.carriage.return       | Flag to determine if the carriage return at the end of the line should be maintained.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | boolean | false                                          |                                                                                                      | low        |
| csv.null.field.indicator       | Indicator to determine how the CSV Reader can determine if a field is null. Valid values are EMPTY_SEPARATORS, EMPTY_QUOTES, BOTH, NEITHER. For more information see http://opencsv.sourceforge.net/apidocs/com/opencsv/enums/CSVReaderNullFieldIndicator.html.                                                                                                                                                                                                                                                                                                                                      | string  | NEITHER                                        | ValidEnum{enum=CSVReaderNullFieldIndicator, allowed=[EMPTY_SEPARATORS, EMPTY_QUOTES, BOTH, NEITHER]} | low        |
| csv.quote.char                 | The character that is used to quote a field. This typically happens when the csv.separator.char character is within the data.                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | int     | 34                                             |                                                                                                      | low        |
| csv.separator.char             | The character that seperates each field. Typically in a CSV this is a , character. A TSV would use \t.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | int     | 44                                             |                                                                                                      | low        |
| csv.skip.lines                 | Number of lines to skip in the beginning of the file.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | int     | 0                                              |                                                                                                      | low        |
| csv.strict.quotes              | Sets the strict quotes setting - if true, characters outside the quotes are ignored.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | boolean | false                                          |                                                                                                      | low        |
| csv.verify.reader              | Flag to determine if the reader should be verified.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | boolean | true                                           |                                                                                                      | low        |
| empty.poll.wait.ms             | The amount of time to wait if a poll returns an empty list of records.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | long    | 1000                                           | [1,...,9223372036854775807]                                                                          | low        |
| file.minimum.age.ms            | The amount of time in milliseconds after the file was last written to before the file can be processed.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | long    | 0                                              | [0,...,9223372036854775807]                                                                          | low        |
| parser.timestamp.date.formats  | The date formats that are expected in the file. This is a list of strings that will be used to parse the date fields in order. The most accurate date format should be the first in the list. Take a look at the Java documentation for more info. https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html                                                                                                                                                                                                                                                                         | list    | [yyyy-MM-dd'T'HH:mm:ss, yyyy-MM-dd' 'HH:mm:ss] |                                                                                                      | low        |
| parser.timestamp.timezone      | The timezone that all of the dates will be parsed with.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | string  | UTC                                            |                                                                                                      | low        |
| processing.file.extension      | Before a file is processed, it is renamed to indicate that it is currently being processed. This setting is appended to the end of the file.                                                                                                                                                                                                                                                                                                                                                                                                                                                         | string  | .PROCESSING                                    | ValidPattern{pattern=^.*\..+$}                                                                       | low        |

## SpoolDirJsonSourceConnector

This connector is used to [stream](https://en.wikipedia.org/wiki/JSON_Streaming) JSON files from a directory while converting the data based on the schema supplied in the configuration.

```properties
name=connector1
tasks.max=1
connector.class=com.github.jcustenborder.kafka.connect.spooldir.SpoolDirJsonSourceConnector

# Set these required values
finished.path=
key.schema=
input.file.pattern=
topic=
error.path=
input.path=
value.schema=
```

| Name                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | Type    | Default                                        | Valid Values                                                            | Importance |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|------------------------------------------------|-------------------------------------------------------------------------|------------|
| error.path                    | The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | string  |                                                |                                                                         | high       |
| finished.path                 | The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.                                                                                                                                                                                                                                                                                                                                                                                                                                                     | string  |                                                |                                                                         | high       |
| input.file.pattern            | Regular expression to check input file names against. This expression must match the entire filename. The equivalent of Matcher.matches().                                                                                                                                                                                                                                                                                                                                                                                                                                                           | string  |                                                |                                                                         | high       |
| input.path                    | The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | string  |                                                |                                                                         | high       |
| key.schema                    | The schema for the key written to Kafka.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | string  |                                                |                                                                         | high       |
| topic                         | The Kafka topic to write the data to.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | string  |                                                |                                                                         | high       |
| value.schema                  | The schema for the value written to Kafka.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | string  |                                                |                                                                         | high       |
| halt.on.error                 | Should the task halt when it encounters an error or continue to the next file.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | boolean | true                                           |                                                                         | high       |
| timestamp.field               | The field in the value schema that will contain the parsed timestamp for the record. This field cannot be marked as optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html)                                                                                                                                                                                                                                                                                                                                                              | string  | ""                                             |                                                                         | medium     |
| timestamp.mode                | Determines how the connector will set the timestamp for the [ConnectRecord](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/connector/ConnectRecord.html#timestamp()). If set to `Field` then the timestamp will be read from a field in the value. This field cannot be optional and must be a [Timestamp](https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.html). Specify the field  in `timestamp.field`. If set to `FILE_TIME` then the last modified time of the file will be used. If set to `PROCESS_TIME` the time the record is read will be used. | string  | PROCESS_TIME                                   | ValidEnum{enum=TimestampMode, allowed=[FIELD, FILE_TIME, PROCESS_TIME]} | medium     |
| batch.size                    | The number of records that should be returned with each batch.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | int     | 1000                                           |                                                                         | low        |
| empty.poll.wait.ms            | The amount of time to wait if a poll returns an empty list of records.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | long    | 1000                                           | [1,...,9223372036854775807]                                             | low        |
| file.minimum.age.ms           | The amount of time in milliseconds after the file was last written to before the file can be processed.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | long    | 0                                              | [0,...,9223372036854775807]                                             | low        |
| parser.timestamp.date.formats | The date formats that are expected in the file. This is a list of strings that will be used to parse the date fields in order. The most accurate date format should be the first in the list. Take a look at the Java documentation for more info. https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html                                                                                                                                                                                                                                                                         | list    | [yyyy-MM-dd'T'HH:mm:ss, yyyy-MM-dd' 'HH:mm:ss] |                                                                         | low        |
| parser.timestamp.timezone     | The timezone that all of the dates will be parsed with.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | string  | UTC                                            |                                                                         | low        |
| processing.file.extension     | Before a file is processed, it is renamed to indicate that it is currently being processed. This setting is appended to the end of the file.                                                                                                                                                                                                                                                                                                                                                                                                                                                         | string  | .PROCESSING                                    | ValidPattern{pattern=^.*\..+$}                                          | low        |

# Building on you workstation

```
git clone git@github.com:jcustenborder/kafka-connect-spooldir.git
cd kafka-connect-spooldir
mvn clean package
```

# Debugging 

## Running with debugging

```bash
./bin/debug.sh
```
## Running with debugging - suspending

```bash
export SUSPEND='y'
./bin/debug.sh
```