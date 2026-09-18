#!/usr/bin/env bash
#
# Copyright © 2016-2025 The ThingsBoard Authors
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
# 从 Maven 产出的 *-boot.jar 构建全部 ThingsBoard 镜像（不再经过 deb 打包）。
#
# 用法（在仓库根目录执行）:
#   bash docker/build-images.sh                    # 构建 JVM 服务镜像（core/rule-engine/各 transport/edqs/monitoring/vc-executor）
#   bash docker/build-images.sh --with-node-images # 额外构建 web-ui 与 js-executor（需要联网下载 node/yarn）
#   bash docker/build-images.sh --skip-maven       # 跳过 Maven 打包，直接对现有 target/ 产物做 docker build
#
# 产物镜像名与 docker/.env 里的 *_DOCKER_NAME + TB_VERSION 对齐，docker-compose 直接可用。
if [ -z "${BASH_VERSION:-}" ]; then exec bash "$0" "$@"; fi
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

WITH_NODE_IMAGES=false
SKIP_MAVEN=false
for arg in "$@"; do
  case "${arg}" in
    --with-node-images) WITH_NODE_IMAGES=true ;;
    --skip-maven)       SKIP_MAVEN=true ;;
    *) echo "未知参数: ${arg}" >&2; exit 2 ;;
  esac
done

# 镜像仓库与 tag 取自 docker/.env，保证与 compose 一致
set -a; source docker/.env; set +a
REPO="${DOCKER_REPO:-thingsboard}"
TAG="${TB_VERSION:-latest}"

# module 路径 : 镜像名 : target 下的产物文件名（"-" 表示非 JVM 镜像，不校验 jar）
# tb-core / tb-rule-engine 各自打包自己的 boot jar；tb-monolith 打包 monolith（${pkg.name}=thingsboard），产物是 thingsboard.jar
IMAGES=(
  "images/tb-core:tb-core:thingsboard-core.jar"
  "images/tb-rule-engine:tb-rule-engine:thingsboard-rule-engine.jar"
  "images/tb-monolith:tb-monolith:thingsboard.jar"
  "images/tb-transport-images/tb-http-transport-image:tb-http-transport:tb-http-transport.jar"
  "images/tb-transport-images/tb-mqtt-transport-image:tb-mqtt-transport:tb-mqtt-transport.jar"
  "images/tb-transport-images/tb-coap-transport-image:tb-coap-transport:tb-coap-transport.jar"
  "images/tb-transport-images/tb-lwm2m-transport-image:tb-lwm2m-transport:tb-lwm2m-transport.jar"
  "images/tb-transport-images/tb-snmp-transport-image:tb-snmp-transport:tb-snmp-transport.jar"
  "images/tb-transport-images/tb-tcp-transport-image:tb-tcp-transport:tb-tcp-transport.jar"
  "images/tb-transport-images/tb-udp-transport-image:tb-udp-transport:tb-udp-transport.jar"
  "images/tb-vc-executor-image:tb-vc-executor:tb-vc-executor.jar"
  "images/tb-edqs-image:tb-edqs:tb-edqs.jar"
  "images/tb-monitoring-image:tb-monitoring:tb-monitoring.jar"
)
NODE_IMAGES=(
  "images/web-ui:tb-web-ui:-"
  "images/js-executor:tb-js-executor:-"
)

if [[ "${WITH_NODE_IMAGES}" == "true" ]]; then
  IMAGES+=("${NODE_IMAGES[@]}")
fi

if [[ "${SKIP_MAVEN}" != "true" ]]; then
  pl="$(printf '%s,' "${IMAGES[@]%%:*}" | sed 's/,$//')"
  echo "== 1/2 Maven 打包（产出 boot jar + 过滤后的 Dockerfile） =="
  "${MVN:-mvn}" -pl "${pl}" -am package -DskipTests -Dmaven.test.skip=true
fi

echo "== 2/2 docker build =="
failed=()
for spec in "${IMAGES[@]}"; do
  dir="$(echo "${spec}" | cut -d: -f1)"
  name="$(echo "${spec}" | cut -d: -f2)"
  jar="$(echo "${spec}" | cut -d: -f3)"
  ctx="${dir}/target"
  if [[ ! -f "${ctx}/Dockerfile" ]]; then
    echo "  SKIP ${name}: 缺少 ${ctx}/Dockerfile（先跑 Maven 打包）" >&2
    continue
  fi
  if [[ "${jar}" != "-" && ! -f "${ctx}/${jar}" ]]; then
    echo "  SKIP ${name}: 缺少 ${ctx}/${jar}（先跑 Maven 打包）" >&2
    continue
  fi
  echo "  building ${REPO}/${name}:${TAG}"
  # 单个镜像失败（如镜像内 apt 需要外网）不应中断其余镜像
  if ! docker build -t "${REPO}/${name}:${TAG}" "${ctx}"; then
    echo "  FAILED ${name}（继续构建其余镜像）" >&2
    failed+=("${name}")
  fi
done
if (( ${#failed[@]} > 0 )); then
  echo
  echo "以下镜像构建失败：${failed[*]}" >&2
fi

echo
echo "完成。镜像仓库=${REPO} tag=${TAG}"
echo "起栈：cd docker && docker compose up -d   （完整流程见 docker/README.md）"
