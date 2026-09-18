#!/usr/bin/env bash
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
