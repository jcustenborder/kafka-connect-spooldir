# Introduction
[Documentation](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-spooldir)

Installation through the [Confluent Hub Client](https://docs.confluent.io/current/connect/managing/confluent-hub/client.html)

This Kafka Connect connector provides the capability to watch a directory for files and read the data as new files are written to the input directory. Each of the records in the input file will be converted based on the user supplied schema.

The CSVRecordProcessor supports reading CSV or TSV files. It can convert a CSV on the fly to the strongly typed Kafka Connect data types. It currently has support for all of the schema types and logical types that are supported in Kafka Connect. If you couple this with the Avro converter and Schema Registry by Confluent, you will be able to process CSV, Json, or TSV files to strongly typed Avro data in real time.

## [Schema Less Json Source Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-spooldir/sources/SpoolDirSchemaLessJsonSourceConnector.html)

This connector is used to `stream <https://en.wikipedia.org/wiki/JSON_Streaming>_` JSON files from a directory while converting the data based on the schema supplied in the configuration.
## [CSV Source Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-spooldir/sources/SpoolDirCsvSourceConnector.html)

The SpoolDirCsvSourceConnector will monitor the directory specified in `input.path` for files and read them as a CSV converting each of the records to the strongly typed equivalent specified in `key.schema` and `value.schema`.
## [Json Source Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-spooldir/sources/SpoolDirJsonSourceConnector.html)

This connector is used to `stream <https://en.wikipedia.org/wiki/JSON_Streaming>` JSON files from a directory while converting the data based on the schema supplied in the configuration.
## [Line Delimited Source Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-spooldir/sources/SpoolDirLineDelimitedSourceConnector.html)

This connector is used to read a file line by line and write the data to Kafka.
## [Extended Log File Format Source Connector](https://jcustenborder.github.io/kafka-connect-documentation/projects/kafka-connect-spooldir/sources/SpoolDirELFSourceConnector.html)

This connector is used to stream `Extended Log File Format <https://www.w3.org/TR/WD-logfile.html>` files from a directory while converting the data to a strongly typed schema.



# Development

## Building the source

```bash
mvn clean package
```