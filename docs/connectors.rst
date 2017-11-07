==================
Spooldir Connector
==================

This Kafka Connect connector provides the capability to watch a directory for files and read the data as new files are
written to the input directory. Each of the records in the input file will be converted based on the user supplied schema.

The CSVRecordProcessor supports reading CSV or TSV files. It can convert a CSV on the fly to the strongly typed Kafka
Connect data types. It currently has support for all of the schema types and logical types that are supported in Kafka 0.10.x.
If you couple this with the Avro converter and Schema Registry by Confluent, you will be able to process csv files to
strongly typed Avro data in real time.



.. toctree::
    :maxdepth: 1
    :caption: Source Connectors:
    :hidden:
    :glob:

    sources/*


.. toctree::
    :maxdepth: 1
    :caption: Sink Connectors:
    :hidden:
    :glob:

    sinks/*

.. toctree::
    :maxdepth: 0
    :caption: Schemas:
    :hidden:

    schemas
    info
