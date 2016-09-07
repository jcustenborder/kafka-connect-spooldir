#!/usr/bin/env bash
#
# Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

mvn clean package

export CLASSPATH="$(find `pwd`/target/kafka-connect-spooldir-1.0-SNAPSHOT-package/share/java/ -type f -name '*.jar' | tr '\n' ':')"
export KAFKA_JMX_OPTS='-Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005'

mkdir -p /tmp/spooldir/input /tmp/spooldir/finished /tmp/spooldir/error

cp src/test/resources/io/confluent/kafka/connect/source/MOCK_DATA.csv /tmp/spooldir/input

$CONFLUENT_HOME/bin/connect-standalone connect/connect-avro-docker.properties config/CSVExample.properties