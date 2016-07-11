Overview
========

This Kafka Connect connector provides the capability to watch a directory for files and read the data as new files are
written to the input directory. The RecordProcessor implementation can be overridden so any file type can be supported.
Currently there is support for delimited files and reading a file line by line.

The CSVRecordProcessor supports reading CSV or TSV files. It can convert a CSV on the fly to the strongly typed Kafka
Connect data types. It currently has support for all of the schema types and logical types that are supported in Kafka 0.10.x.
If you couple this with the Avro converter and Schema Registry by Confluent, you will be able to process csv files to
strongly typed Avro data in real time.

The LineRecordProcessor supports reading a file line by line and emitting the line.

Building on you workstation
===========================

This project is dependant upon another project to handle parsing data from text to Kafka Connect compatible types. To build the project use the following. ::

    cd ~/source
    git clone git@github.com:jcustenborder/connect-utils.git
    cd connect-utils
    mvn clean install
    git@github.com:jcustenborder/kafka-connect-spooldir.git
    cd kafka-connect-spooldir
    mvn clean package

Running on your workstation
===========================


Schema Configuration
====================

This connector allows you to either infer a schema with nullable strings from the header row, or you can specify the schema in json format.
To use the automatic schema generation set ``csv.first.row.as.header=true``, ``csv.schema.from.header=true``, ``csv.schema.from.header.keys=key1,key2``.
To manually define the schema set ``csv.schema`` to a json representation of the schema. The example below works is for the mock data in the test class. ::

    {
      "keys": [
        "id"
      ],
      "fields": [
        {
          "name": "id",
          "type": "int32",
          "required": true
        },
        {
          "name": "first_name",
          "type": "string",
          "required": true
        },
        {
          "name": "last_name",
          "type": "string",
          "required": true
        },
        {
          "name": "email",
          "type": "string",
          "required": true
        },
        {
          "name": "gender",
          "type": "string",
          "required": true
        },
        {
          "name": "ip_address",
          "type": "string",
          "required": true
        },
        {
          "name": "last_login",
          "type": "timestamp",
          "required": false
        },
        {
          "name": "account_balance",
          "type": "decimal",
          "scale": 10,
          "required": false
        },
        {
          "name": "country",
          "type": "string",
          "required": true
        },
        {
          "name": "favorite_color",
          "type": "string",
          "required": false
        }
      ]
    }

``name``
    The name of the schema. If you are using the Confluent schema registry this will be the name of the AVRO schema.

``keys``
    The field names for the keys of the message. These fields must exist in the fields array.

``fields``
    The field definitions for the schema.

+------------+------------+-----------+-----------+------------+-----------+-----------+------------+-----------+-----------+-----------+-----------+-----------+
| Property   | Description            | Notes                                                                                                                   |
+============+============+===========+===========+============+===========+===========+============+===========+===========+===========+===========+===========+
| name       | Name of the field.     |                                                                                                                         |
+------------+------------+-----------+-----------+------------+-----------+-----------+------------+-----------+-----------+-----------+-----------+-----------+
| type       | Type for the field     | Valid values are decimal, time, timestamp, date, int8, int16, int32, int64, float32, float64, boolean, string, bytes    |
+------------+------------+-----------+-----------+------------+-----------+-----------+------------+-----------+-----------+-----------+-----------+-----------+
| required   | Is the field required? |                                                                                                                         |
+------------+------------+-----------+-----------+------------+-----------+-----------+------------+-----------+-----------+-----------+-----------+-----------+
| scale      | Scale for a decimal    | Only used for decimals. Ignored for all other types                                                                     |
+------------+------------+-----------+-----------+------------+-----------+-----------+------------+-----------+-----------+-----------+-----------+-----------+


Configuration Options
=====================

``error.path``
  The directory to place files in which have error(s). This directory must exist and be writable by the user running Kafka Connect.

  * Type: string
  * Default: ""
  * Importance: high

``finished.path``
  The directory to place files that have been successfully processed. This directory must exist and be writable by the user running Kafka Connect.

  * Type: string
  * Default: ""
  * Importance: high

``input.file.pattern``
  Regular expression to check input file names against. This expression must match the entire filename. The equivalent of Matcher.matches().

  * Type: string
  * Default: ""
  * Importance: high

``input.path``
  The directory to read files that will be processed. This directory must exist and be writable by the user running Kafka Connect.

  * Type: string
  * Default: ""
  * Importance: high

``record.processor.class``
  Class that implements RecordProcessor. This class is used to process data as it arrives.

  * Type: class
  * Default:
  * Importance: high

``topic``
  The Kafka topic to write the data to.

  * Type: string
  * Default: ""
  * Importance: high

``halt.on.error``
  Should the task halt when it encounters an error or continue to the next file.

  * Type: boolean
  * Default: true
  * Importance: high

``csv.first.row.as.header``
  Flag to indicate if the fist row of data contains the header of the file.

  * Type: boolean
  * Default: false
  * Importance: medium

``csv.schema``
  Schema representation in json.

  * Type: string
  * Default: ""
  * Importance: medium

``batch.size``
  The number of records that should be returned with each batch.

  * Type: int
  * Default: 1000
  * Importance: low

``csv.case.sensitive.field.names``
  Flag to determine if the field names in the header row should be treated as case sensitive.

  * Type: boolean
  * Default: false
  * Importance: low

``csv.escape.char``
  Escape character.

  * Type: int
  * Default: 92
  * Importance: low

``csv.file.charset``
  Character set to read wth file with.

  * Type: string
  * Default: "UTF-8"
  * Importance: low

``csv.ignore.leading.whitespace``
  Sets the ignore leading whitespace setting - if true, white space in front of a quote in a field is ignored.

  * Type: boolean
  * Default: true
  * Importance: low

``csv.ignore.quotations``
  Sets the ignore quotations mode - if true, quotations are ignored.

  * Type: boolean
  * Default: false
  * Importance: low

``csv.keep.carriage.return``
  Flag to determine if the carriage return at the end of the line should be maintained.

  * Type: boolean
  * Default: false
  * Importance: low

``csv.null.field.indicator``
  Indicator to determine how the CSV Reader can determine if a field is null. Valid values are EMPTY_SEPARATORS, EMPTY_QUOTES, BOTH, NEITHER. For more information see http://opencsv.sourceforge.net/apidocs/com/opencsv/enums/CSVReaderNullFieldIndicator.html.

  * Type: string
  * Default: "NEITHER"
  * Importance: low

``csv.parser.timestamp.date.formats``
  The date formats that are expected in the file. This is a list of strings that will be used to parse the date fields in order. The most accurate date format should be the first in the list. Take a look at the Java documentation for more info. https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html

  * Type: list
  * Default: [yyyy-MM-dd' 'HH:mm:ss]
  * Importance: low

``csv.parser.timestamp.timezone``
  The timezone that all of the dates will be parsed with.

  * Type: string
  * Default: "UTC"
  * Importance: low

``csv.quote.char``
  The character that is used to quote a field. This typically happens when the csv.separator.char character is within the data.

  * Type: int
  * Default: 34
  * Importance: low

``csv.schema.from.header``
  Flag to determine if the schema should be generated based on the header row.

  * Type: boolean
  * Default: false
  * Importance: low

``csv.schema.from.header.keys``
  csv.schema.from.header.keys

  * Type: list
  * Default: []
  * Importance: low

``csv.separator.char``
  The character that seperates each field. Typically in a CSV this is a , character. A TSV would use \t.

  * Type: int
  * Default: 44
  * Importance: low

``csv.skip.lines``
  Number of lines to skip in the beginning of the file.

  * Type: int
  * Default: 0
  * Importance: low

``csv.strict.quotes``
  Sets the strict quotes setting - if true, characters outside the quotes are ignored.

  * Type: boolean
  * Default: false
  * Importance: low

``csv.verify.reader``
  Flag to determine if the reader should be verified.

  * Type: boolean
  * Default: true
  * Importance: low

``file.minimum.age.ms``
  The amount of time in milliseconds after the file was last written to before the file can be processed.

  * Type: long
  * Default: 0
  * Importance: low

``processing.file.extension``
  Before a file is processed, it is renamed to indicate that it is currently being processed. This setting is appended to the end of the file.

  * Type: string
  * Default: ".PROCESSING"
  * Importance: low








