#!/usr/bin/env bash
#
# Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

: ${INPUT_PATH:='/tmp/spooldir/input'}
: ${ERROR_PATH:='/tmp/spooldir/error'}
: ${FINISHED_PATH:='/tmp/spooldir/finished'}
: ${DEBUG_SUSPEND_FLAG:='y'}
export KAFKA_DEBUG='n'
export DEBUG_SUSPEND_FLAG='n'
# export KAFKA_OPTS='-agentpath:/Applications/YourKit-Java-Profiler-2017.02.app/Contents/Resources/bin/mac/libyjpagent.jnilib=disablestacktelemetry,exceptions=disable,delay=10000'
set -e

# mvn clean package

if [ ! -d "${INPUT_PATH}" ]; then
   mkdir -p "${INPUT_PATH}"
fi

if [ ! -d "${ERROR_PATH}" ]; then
   mkdir -p "${ERROR_PATH}"
fi

if [ ! -d "${FINISHED_PATH}" ]; then
   mkdir -p "${FINISHED_PATH}"
fi

cp /Users/jeremy/Downloads/csv-spooldir-source.csv "${INPUT_PATH}/csv-spooldir-source.csv"
# cp src/test/resources/com/github/jcustenborder/kafka/connect/spooldir/csv/FieldsMatch.data "${INPUT_PATH}/FieldsMatch.csv"
# cp src/test/resources/com/github/jcustenborder/kafka/connect/spooldir/json/FieldsMatch.data "${INPUT_PATH}/FieldsMatch.json"
# connect-standalone config/connect-avro-docker.properties config/CSVSchemaGenerator.properties
# connect-standalone config/connect-avro-docker.properties config/JsonExample.properties
# connect-standalone config/connect-avro-docker.properties config/AvroExample.properties

export DOCKER_IMAGE="confluentinc/cp-kafka-connect:5.5.2-1-ubi8"

docker run --rm --network=kafka-connect-spooldir_default \
    -p "5005:5005" \
    -v "/tmp/spooldir:/tmp/spooldir" \
    -v "$(pwd)/config:/config" \
    -v "$(pwd)/target/kafka-connect-target/usr/share/kafka-connect:/plugins" \
    "${DOCKER_IMAGE}" /bin/connect-standalone /config/connect-avro-docker.properties /config/CSVSchemaGenerator.properties

