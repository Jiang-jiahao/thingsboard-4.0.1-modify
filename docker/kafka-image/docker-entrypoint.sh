#!/bin/bash
#
# Copyright © 2016-2025 The Thingsboard Authors
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
# 自建 Kafka 镜像的入口脚本：
#  1) 把 KAFKA_* 环境变量写进 config/kraft/server.properties（变量命名与官方 apache/kafka、
#     cp-kafka 镜像一致：KAFKA_FOO_BAR -> foo.bar）；
#  2) 数据目录为空时按 CLUSTER_ID 格式化 KRaft 存储；
#  3) 前台启动 broker。
set -euo pipefail

SERVER_CONF="${KAFKA_HOME}/config/kraft/server.properties"
DATA_DIR="${KAFKA_LOG_DIRS:-/var/lib/kafka/data}"
CLUSTER_ID="${CLUSTER_ID:-MkU3OEVBNTcwNTJENDM2Qk}"

prop() {                      # prop <property.name> <value>
    [ -n "${2:-}" ] || return 0
    if grep -qE "^#?[[:space:]]*${1}=" "${SERVER_CONF}"; then
        sed -i "s|^#\?[[:space:]]*${1}=.*|${1}=${2}|" "${SERVER_CONF}"
    else
        echo "${1}=${2}" >> "${SERVER_CONF}"
    fi
}

prop log.dirs                                  "${DATA_DIR}"
prop node.id                                   "${KAFKA_NODE_ID:-1}"
prop process.roles                             "${KAFKA_PROCESS_ROLES:-broker,controller}"
prop controller.quorum.voters                  "${KAFKA_CONTROLLER_QUORUM_VOTERS:-1@localhost:9094}"
prop controller.listener.names                 "${KAFKA_CONTROLLER_LISTENER_NAMES:-CONTROLLER}"
prop listeners                                 "${KAFKA_LISTENERS:-PLAINTEXT://:9092,CONTROLLER://:9094}"
prop advertised.listeners                      "${KAFKA_ADVERTISED_LISTENERS:-PLAINTEXT://localhost:9092}"
prop listener.security.protocol.map            "${KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:-CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT}"
prop inter.broker.listener.name                "${KAFKA_INTER_BROKER_LISTENER_NAME:-PLAINTEXT}"
prop offsets.topic.replication.factor          "${KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR:-1}"
prop transaction.state.log.replication.factor  "${KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR:-1}"
prop transaction.state.log.min.isr             "${KAFKA_TRANSACTION_STATE_LOG_MIN_ISR:-1}"
prop group.initial.rebalance.delay.ms          "${KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS:-0}"
prop auto.create.topics.enable                 "${KAFKA_AUTO_CREATE_TOPICS_ENABLE:-false}"
prop log.retention.bytes                       "${KAFKA_LOG_RETENTION_BYTES:-}"
prop log.segment.bytes                         "${KAFKA_LOG_SEGMENT_BYTES:-}"
prop log.retention.ms                          "${KAFKA_LOG_RETENTION_MS:-}"
prop log.cleanup.policy                        "${KAFKA_LOG_CLEANUP_POLICY:-delete}"

if [ ! -f "${DATA_DIR}/meta.properties" ]; then
    echo "格式化 KRaft 存储：cluster.id=${CLUSTER_ID} log.dirs=${DATA_DIR}"
    "${KAFKA_HOME}/bin/kafka-storage.sh" format -t "${CLUSTER_ID}" -c "${SERVER_CONF}" --ignore-formatted
fi

exec "${KAFKA_HOME}/bin/kafka-server-start.sh" "${SERVER_CONF}"
