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

This project is dependant upon another project to handle parsing data from text to Kafka Connect compatible types. To build the project use the following.

::.
    cd ~/source
    git clone git@github.com:jcustenborder/connect-utils.git
    cd connect-utils
    mvn clean install
    git@github.com:jcustenborder/kafka-connect-spooldir.git
    cd kafka-connect-spooldir
    mvn clean package


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
  Regular expression to check input file names against.

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

``file.minimum.age.ms``
  The amount of time in milliseconds after the file was last written to before the file can be processed.

  * Type: long
  * Default: 0
  * Importance: high

``first.row.as.header``
  Flag to indicate if the fist row of data contains the header of the file.

  * Type: boolean
  * Default: false
  * Importance: high

``halt.on.error``
  Should the task halt when it encounters an error or continue to the next file.

  * Type: boolean
  * Default: true
  * Importance: high

``key.fields``
  The fields that should be used as a key for the message.

  * Type: list
  * Default: []
  * Importance: high

``escape.char``
  Escape character.

  * Type: int
  * Default: 92
  * Importance: medium

``file.charset``
  Character set to read wth file with.

  * Type: string
  * Default: "UTF-8"
  * Importance: medium

``parser.timestamp.date.formats``
  The date formats that are expected in the file. This is a list of strings that will be used to parse the date fields in order. The most accurate date format should be the first in the list. Take a look at the Java documentation for more info. https://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html

  * Type: list
  * Default: [yyyy-MM-dd' 'HH:mm:ss]
  * Importance: medium

``parser.timestamp.timezone``
  The timezone that all of the dates will be parsed with.

  * Type: string
  * Default: "UTC"
  * Importance: medium

``quote.char``
  The character that is used to quote a field. This typically happens when the separator.char character is within the data.

  * Type: int
  * Default: 34
  * Importance: medium

``schema``
  Schema representaiton in json.

  * Type: string
  * Default: ""
  * Importance: medium

``separator.char``
  The character that seperates each field. Typically in a CSV this is a , character. A TSV would use \t.

  * Type: int
  * Default: 44
  * Importance: medium

``batch.size``
  The number of records that should be returned with each batch.

  * Type: int
  * Default: 100
  * Importance: low

``ignore.leading.whitespace``
  Flag to determine if the whitespace leading the field should be ignored.

  * Type: boolean
  * Default: true
  * Importance: low

``ignore.quotations``
  ignore_quotations character.

  * Type: boolean
  * Default: false
  * Importance: low

``keep.carriage.return``
  Flag to determine if the carriage return at the end of the line should be maintained.

  * Type: boolean
  * Default: false
  * Importance: low

``null.field.indicator``
  Indicator to determine how the CSV Reader can determine if a field is null. Valid values are EMPTY_SEPARATORS, EMPTY_QUOTES, BOTH, NEITHER. For more information see http://opencsv.sourceforge.net/apidocs/com/opencsv/enums/CSVReaderNullFieldIndicator.html.

  * Type: string
  * Default: "NEITHER"
  * Importance: low

``skip.lines``
  Number of lines to skip in the beginning of the file.

  * Type: int
  * Default: 0
  * Importance: low

``strict.quotes``
  strict quotes.

  * Type: boolean
  * Default: false
  * Importance: low

``verify.reader``
  Flag to determine if the reader should be verified.

  * Type: boolean
  * Default: true
  * Importance: low






