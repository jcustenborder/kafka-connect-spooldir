# Overview

This Kafka Connect connector provides the capability to watch a directory for files and read the data as new files are
written to the input directory. Each of the records in the input file will be converted based on the user supplied schema. 

The CSVRecordProcessor supports reading CSV or TSV files. It can convert a CSV on the fly to the strongly typed Kafka
Connect data types. It currently has support for all of the schema types and logical types that are supported in Kafka 0.10.x.
If you couple this with the Avro converter and Schema Registry by Confluent, you will be able to process csv files to
strongly typed Avro data in real time.

The LineRecordProcessor supports reading a file line by line and emitting the line.



# Schema Configuration

This connector requires you to specify the schema of the files to be read. This is controlled by the
`key.schema` and the `value.schema` configuration settings. This setting is a schema in json form stored as a string. The 
field schemas are used to parse the data in each row. This allows the user to strongly define the types for each field. 

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



# SpoolDirCsvSourceConnector

The `SpoolDirCsvSourceConnector` will monitor the directory specified in `input.path` for files and read them as a CSV 
converting each of the records to the strongly typed equavalent specified in `key.schema` and `value.schema`.

| Name                          | Description                                                                                                                                                                                                                                                                                                                  | Type    | Default                 | Valid Values                   | Importance |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|-------------------------|--------------------------------|------------|
| error.path                    | The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.                                                                                                                                                                                            | string  |                         |                                | high       |
| finished.path                 | The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.                                                                                                                                                                             | string  |                         |                                | high       |
| input.file.pattern            | Regular expression to check input file names against. This expression must match the entire filename. The equivalent of Matcher.matches().                                                                                                                                                                                   | string  |                         |                                | high       |
| input.path                    | The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.                                                                                                                                                                                             | string  |                         |                                | high       |
| key.schema                    | The schema for the key written to Kafka.                                                                                                                                                                                                                                                                                     | string  |                         |                                | high       |
| topic                         | The Kafka topic to write the data to.                                                                                                                                                                                                                                                                                        | string  |                         |                                | high       |
| value.schema                  | The schema for the value written to Kafka.                                                                                                                                                                                                                                                                                   | string  |                         |                                | high       |
| halt.on.error                 | Should the task halt when it encounters an error or continue to the next file.                                                                                                                                                                                                                                               | boolean | true                    |                                | high       |
| batch.size                    | The number of records that should be returned with each batch.                                                                                                                                                                                                                                                               | int     | 1000                    |                                | low        |
| empty.poll.wait.ms            | The amount of time to wait if a poll returns an empty list of records.                                                                                                                                                                                                                                                       | long    | 1000                    | [1,...,9223372036854775807]    | low        |
| file.minimum.age.ms           | The amount of time in milliseconds after the file was last written to before the file can be processed.                                                                                                                                                                                                                      | long    | 0                       | [0,...,9223372036854775807]    | low        |
| parser.timestamp.date.formats | The date formats that are expected in the file. This is a list of strings that will be used to parse the date fields in order. The most accurate date format should be the first in the list. Take a look at the Java documentation for more info. https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html | list    | [yyyy-MM-dd' 'HH:mm:ss] |                                | low        |
| parser.timestamp.timezone     | The timezone that all of the dates will be parsed with.                                                                                                                                                                                                                                                                      | string  | UTC                     |                                | low        |
| processing.file.extension     | Before a file is processed, it is renamed to indicate that it is currently being processed. This setting is appended to the end of the file.                                                                                                                                                                                 | string  | .PROCESSING             | ValidPattern{pattern=^.*\..+$} | low        |

# SpoolDirJsonSourceConnector

| Name                          | Description                                                                                                                                                                                                                                                                                                                  | Type    | Default                 | Valid Values                   | Importance |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|-------------------------|--------------------------------|------------|
| error.path                    | The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.                                                                                                                                                                                            | string  |                         |                                | high       |
| finished.path                 | The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.                                                                                                                                                                             | string  |                         |                                | high       |
| input.file.pattern            | Regular expression to check input file names against. This expression must match the entire filename. The equivalent of Matcher.matches().                                                                                                                                                                                   | string  |                         |                                | high       |
| input.path                    | The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.                                                                                                                                                                                             | string  |                         |                                | high       |
| key.schema                    | The schema for the key written to Kafka.                                                                                                                                                                                                                                                                                     | string  |                         |                                | high       |
| topic                         | The Kafka topic to write the data to.                                                                                                                                                                                                                                                                                        | string  |                         |                                | high       |
| value.schema                  | The schema for the value written to Kafka.                                                                                                                                                                                                                                                                                   | string  |                         |                                | high       |
| halt.on.error                 | Should the task halt when it encounters an error or continue to the next file.                                                                                                                                                                                                                                               | boolean | true                    |                                | high       |
| batch.size                    | The number of records that should be returned with each batch.                                                                                                                                                                                                                                                               | int     | 1000                    |                                | low        |
| empty.poll.wait.ms            | The amount of time to wait if a poll returns an empty list of records.                                                                                                                                                                                                                                                       | long    | 1000                    | [1,...,9223372036854775807]    | low        |
| file.minimum.age.ms           | The amount of time in milliseconds after the file was last written to before the file can be processed.                                                                                                                                                                                                                      | long    | 0                       | [0,...,9223372036854775807]    | low        |
| parser.timestamp.date.formats | The date formats that are expected in the file. This is a list of strings that will be used to parse the date fields in order. The most accurate date format should be the first in the list. Take a look at the Java documentation for more info. https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html | list    | [yyyy-MM-dd' 'HH:mm:ss] |                                | low        |
| parser.timestamp.timezone     | The timezone that all of the dates will be parsed with.                                                                                                                                                                                                                                                                      | string  | UTC                     |                                | low        |
| processing.file.extension     | Before a file is processed, it is renamed to indicate that it is currently being processed. This setting is appended to the end of the file.                                                                                                                                                                                 | string  | .PROCESSING             | ValidPattern{pattern=^.*\..+$} | low        |

# Building on you workstation

```
git clone git@github.com:jcustenborder/kafka-connect-spooldir.git
cd kafka-connect-spooldir
mvn clean package
```

# Debugging 

## 

```bash

```