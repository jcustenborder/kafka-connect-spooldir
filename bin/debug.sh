#!/usr/bin/env bash
mvn clean package

export CLASSPATH="$(find `pwd`/target/kafka-connect-spooldir-1.0-SNAPSHOT-package/share/java/ -type f -name '*.jar' | tr '\n' ':')"
export KAFKA_JMX_OPTS='-Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005'

mkdir -p /tmp/spooldir/input /tmp/spooldir/finished /tmp/spooldir/error

cp src/test/resources/io/confluent/kafka/connect/source/MOCK_DATA.csv /tmp/spooldir/input

$CONFLUENT_HOME/bin/connect-standalone connect/connect-avro-docker.properties config/CSVExample.properties