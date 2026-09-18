#!/bin/bash
echo "Building ThingsBoard protobuf-containing packages..."
MAVEN_OPTS="-Xmx1024m" NODE_OPTIONS="--max_old_space_size=3072" \
mvn clean compile -T4 --also-make --projects='
libs/common/cluster-api,
libs/transport/coap,
libs/transport/mqtt,
libs/transport/transport-api'
