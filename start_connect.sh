#!/usr/bin/env bash

set -e

export KAFKA_VERSION=7.0.1
export LOCAL_PLUGIN_PATH=$(pwd)/target/kafka-connect-target/usr/share/kafka-connect/
export WORKDIR=$(pwd)/target/spooldir

mvn clean package
mkdir -p $WORKDIR/input
mkdir -p $WORKDIR/finished
mkdir -p $WORKDIR/error


docker compose -f src/test/resources/docker/docker-compose.yml rm -vf
docker compose -f src/test/resources/docker/docker-compose.yml up