#!/usr/bin/env bash
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
# 用本机已有的 Kafka 发行版打一个 Kafka 镜像（不需要联网）。
#
# 用法：
#   bash docker/kafka-image/build.sh
#   KAFKA_DIST=/path/to/kafka_2.13-3.7.1 bash docker/kafka-image/build.sh
#   KAFKA_IMAGE=myreg/tb-kafka:3.7.1 bash docker/kafka-image/build.sh   # 打上 registry 前缀便于推送
#
# 发行版从哪来：Apache 官方或国内镜像的 kafka_2.13-<版本>.tgz 解压即可
# （例如清华 TUNA 的 apache 镜像目录下有 kafka/），解压后把目录传给 KAFKA_DIST。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIST="${KAFKA_DIST:-$HOME/Library/Caches/tb-msa-infra/kafka_2.13-3.7.1}"
IMAGE="${KAFKA_IMAGE:-thingsboard/tb-kafka:3.7.1}"

if [ ! -d "${DIST}/bin" ]; then
    echo "找不到 Kafka 发行版：${DIST}" >&2
    echo "（它应该包含 bin/、libs/、config/；用 KAFKA_DIST=/path/to/kafka_2.13-3.7.1 指定）" >&2
    exit 1
fi

# 发行版 100+MB，不放进仓库；构建时拷进临时上下文
CTX="$(mktemp -d)"
trap 'rm -rf "${CTX}"' EXIT

cp -R "${DIST}" "${CTX}/kafka"
cp "${HERE}/Dockerfile" "${HERE}/docker-entrypoint.sh" "${CTX}/"

echo "构建 ${IMAGE}（发行版来自 ${DIST}）"
docker build -t "${IMAGE}" "${CTX}"
docker images --format '{{.Repository}}:{{.Tag}} {{.Size}}' | grep "${IMAGE%%:*}" || true
