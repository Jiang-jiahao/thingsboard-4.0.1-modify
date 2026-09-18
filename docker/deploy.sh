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
# 在干净的 Linux 机器上一键部署 ThingsBoard（微服务 + nginx 统一入口网关）。
#
# 流程：前置检查 → 构建/拉取镜像 → 写配置 → 准备数据库 → 生成证书 → 建日志目录 → 起栈 → 验证
#
# 用法（在仓库根目录执行）：
#   bash docker/deploy.sh --check                                  # 只做前置检查
#   bash docker/deploy.sh --db-url 'jdbc:postgresql://10.0.0.5:5432/thingsboard' \
#                         --db-user postgres --db-password 'secret'
#   bash docker/deploy.sh --db-dump /path/to/thingsboard.dump      # 恢复进自带 postgres
#   bash docker/deploy.sh --help
#
# 注意：**这套镜像不能初始化数据库**（安装流程不在源码里），所以必须二选一：
#   ① --db-url 指向一个已经初始化好的库；② --db-dump 提供一份 dump，恢复进自带的 postgres。
set -euo pipefail

DEFAULT_VERSION="4.0.1"
UI_BASE_HREF="/"                 # 前端 base href；改了要同步改 docker/nginx/config/locations.conf
BUILD_UI=true
IMAGE_SOURCE="build"             # build | pull
DB_MODE=""                       # external | dump
DB_URL=""; DB_USER=""; DB_PASSWORD=""; DB_DUMP=""
QUEUE_TYPE="kafka"
CACHE_TYPE="redis"
INSTALL_DOCKER=false
CHECK_ONLY=false
DRY_RUN=false
SKIP_CHOWN=false
ASSUME_YES=false
TB_VERSION="${DEFAULT_VERSION}"

usage() {
    sed -n '/^# 在干净的 Linux 机器上一键部署/,/^#   ② --db-dump/p' "$0" | sed 's/^# \{0,1\}//'
    cat <<'EOF'

选项：
  --db-url <jdbc-url>       使用已初始化的外部库（配 --db-user/--db-password）
  --db-user <user>          数据库用户（默认 postgres）
  --db-password <password>  数据库口令
  --db-dump <file>          提供 .dump/.sql 文件，自动恢复进自带的 postgres 容器
  --version <tag>           镜像 tag（默认 4.0.1）
  --images build|pull       镜像来源：本机构建（默认）或从 registry 拉取
  --skip-ui                 不构建前端与 js-executor（只有后端 API，没有管理界面）
  --queue <type>            队列类型（默认 kafka）
  --cache <type>            缓存类型（默认 redis）
  --install-docker          缺 Docker 时代为安装（否则只打印安装命令）
  --check                   只做前置检查，不安装、不构建、不起服务
  --dry-run                 只打印将要执行的步骤与命令，不改文件、不动系统
  --skip-chown              跳过 docker-create-log-folders.sh（不用 sudo；你自己保证目录属主）
  -y, --yes                 非交互（不询问）
  -h, --help                显示本帮助
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --db-url)         DB_MODE=external; DB_URL="${2:-}"; shift 2 ;;
        --db-user)        DB_USER="${2:-}"; shift 2 ;;
        --db-password)    DB_PASSWORD="${2:-}"; shift 2 ;;
        --db-dump)        DB_MODE=dump; DB_DUMP="${2:-}"; shift 2 ;;
        --version)        TB_VERSION="${2:-}"; shift 2 ;;
        --images)         IMAGE_SOURCE="${2:-}"; shift 2 ;;
        --skip-ui)        BUILD_UI=false; shift ;;
        --queue)          QUEUE_TYPE="${2:-}"; shift 2 ;;
        --cache)          CACHE_TYPE="${2:-}"; shift 2 ;;
        --install-docker) INSTALL_DOCKER=true; shift ;;
        --check)          CHECK_ONLY=true; shift ;;
        --dry-run)        DRY_RUN=true; shift ;;
        --skip-chown)     SKIP_CHOWN=true; shift ;;
        -y|--yes)         ASSUME_YES=true; shift ;;
        -h|--help)        usage; exit 0 ;;
        *) echo "未知参数：$1（用 --help 看用法）" >&2; exit 2 ;;
    esac
done

# ---------------------------------------------------------------- 基础

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCKER_DIR="${ROOT}/docker"
cd "${ROOT}"

if [ -t 1 ]; then C_OK=$'\033[32m'; C_WARN=$'\033[33m'; C_ERR=$'\033[31m'; C_HL=$'\033[36m'; C_END=$'\033[0m'
else C_OK=""; C_WARN=""; C_ERR=""; C_HL=""; C_END=""; fi

STEP=0; TOTAL=7
step() { STEP=$((STEP+1)); printf '\n%s[%d/%d] %s%s\n' "${C_HL}" "${STEP}" "${TOTAL}" "$*" "${C_END}"; }
ok()   { printf '  %s✓%s %s\n' "${C_OK}" "${C_END}" "$*"; }
warn() { printf '  %s!%s %s\n' "${C_WARN}" "${C_END}" "$*"; }
die()  { printf '\n%s✗ %s%s\n' "${C_ERR}" "$*" "${C_END}" >&2; exit 1; }
ask()  { [ "${ASSUME_YES}" = true ] && return 0; [ -t 0 ] || return 1
         read -r -p "  $1 [y/N] " a; [[ "${a:-n}" =~ ^[Yy]$ ]]; }
run()  { if [ "${DRY_RUN}" = true ]; then printf '  %s[dry-run]%s %s\n' "${C_WARN}" "${C_END}" "$*"
         else "$@"; fi }

# ---------------------------------------------------------------- 1. 前置检查

step "前置检查"

command -v uname >/dev/null && [ "$(uname -s)" = "Linux" ] || warn "当前不是 Linux（$(uname -s 2>/dev/null)），脚本按 Linux 写，继续可能失败"

# docker 可能装了但不在 PATH（典型：macOS 的 Docker Desktop、snap），先兜底找一下
if ! command -v docker >/dev/null 2>&1; then
    for d in /Applications/Docker.app/Contents/Resources/bin /usr/local/bin /opt/homebrew/bin /snap/bin /usr/bin; do
        if [ -x "${d}/docker" ]; then export PATH="${d}:${PATH}"; break; fi
    done
fi

if command -v docker >/dev/null 2>&1; then
    ok "docker：$(docker --version | cut -d, -f1)"
else
    warn "没有 docker"
    if [ "${INSTALL_DOCKER}" = true ]; then
        echo "  将执行：curl -fsSL https://get.docker.com | sh"
        if ask "现在安装 Docker？"; then
            curl -fsSL https://get.docker.com | sh
            sudo systemctl enable --now docker || true
            ok "docker 已安装"
        else
            die "未安装 Docker"
        fi
    else
        die "请先安装 Docker，或加 --install-docker 让脚本代装：
    curl -fsSL https://get.docker.com | sh && sudo systemctl enable --now docker"
    fi
fi

docker compose version >/dev/null 2>&1 || die "缺 docker compose 插件（需要 Docker 24+ 自带的 compose v2）"
ok "compose：$(docker compose version --short 2>/dev/null | head -1)"

mem_gb=$(awk '/MemTotal/{printf "%d", $2/1024/1024}' /proc/meminfo 2>/dev/null || echo 0)
if [ "${mem_gb}" -gt 0 ] && [ "${mem_gb}" -lt 8 ]; then
    warn "物理内存只有 ${mem_gb}G。完整栈 12 个 JVM 实测约需 8G 给 Docker；"
    warn "内存不够就调小 docker/.env 的 JAVA_OPTS（transport 建议 -Xmx256M）"
elif [ "${mem_gb}" -gt 0 ]; then
    ok "内存：${mem_gb}G"
fi

if command -v ss >/dev/null 2>&1; then
    for p in 80 443 1883 5683; do
        ss -ltn 2>/dev/null | grep -q ":${p} " && die "端口 ${p} 已被占用（入口网关需要它）"
    done
    ok "端口 80/443/1883/5683 未被占用"
fi

for f in docker/docker-compose.yml docker/build-images.sh docker/nginx/config/nginx.conf \
         docker/nginx/gen-self-signed-cert.sh docker/compose-utils.sh .mvn/maven.config; do
    [ -e "${ROOT}/${f}" ] || die "仓库不完整，缺少 ${f}（要用完整仓库目录，不能只拷 docker/）"
done
ok "仓库文件完整"

if [ "${CHECK_ONLY}" = true ]; then
    printf '\n%s前置检查通过（--check 到此结束，什么都没改）%s\n' "${C_OK}" "${C_END}"
    exit 0
fi

# ---------------------------------------------------------------- 2. 数据库参数

step "数据库"

case "${DB_MODE}" in
    external)
        [ -n "${DB_URL}" ] || die "--db-url 不能为空"
        DB_USER="${DB_USER:-postgres}"
        [ -n "${DB_PASSWORD}" ] || die "请用 --db-password 提供口令"
        ok "外部库：${DB_URL}（用户 ${DB_USER}）"
        ;;
    dump)
        [ -f "${DB_DUMP}" ] || die "找不到 dump 文件：${DB_DUMP}"
        DB_USER="${DB_USER:-postgres}"
        DB_PASSWORD="${DB_PASSWORD:-postgres}"
        DB_URL="jdbc:postgresql://postgres:5432/thingsboard"
        ok "把 ${DB_DUMP} 恢复进自带的 postgres 容器"
        ;;
    *)
        die "**这套镜像不能初始化数据库**（安装流程不在源码里），必须二选一：
    --db-url 'jdbc:postgresql://<host>:5432/thingsboard' --db-user <u> --db-password <p>
    --db-dump /path/to/thingsboard.dump
  详见 docker/DEPLOY.md 第 1 节。"
        ;;
esac

# ---------------------------------------------------------------- 3. 构建 / 拉取镜像

step "镜像（$([ "${IMAGE_SOURCE}" = build ] && echo 本机构建 || echo 从 registry 拉取)）"

if [ "${IMAGE_SOURCE}" = build ]; then
    if [ -z "${JAVA_HOME:-}" ]; then
        for j in /usr/lib/jvm/java-21-openjdk* /usr/lib/jvm/jdk-21*; do
            [ -x "${j}/bin/javac" ] && export JAVA_HOME="${j}" && break
        done
    fi
    [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/javac" ] \
        || die "本机构建需要 JDK 21（未找到，可设置 JAVA_HOME=/usr/lib/jvm/java-21-openjdk-*）"
    ok "JDK：${JAVA_HOME}"

    if [ -z "${MVN:-}" ]; then
        if command -v mvn >/dev/null 2>&1; then MVN="$(command -v mvn)"
        else MVN="$(find "${HOME}/.m2/wrapper/dists" -maxdepth 4 -type f -path '*/bin/mvn' 2>/dev/null | head -1)"; fi
    fi
    [ -n "${MVN}" ] && [ -x "${MVN}" ] \
        || die "本机构建需要 Maven >= 3.6.3（未找到；可显式指定：MVN=/path/to/mvn bash docker/deploy.sh ...）"
    ok "Maven：$("${MVN}" -v 2>/dev/null | head -1 | awk '{print $3}')"

    if [ "${BUILD_UI}" = true ]; then
        if [ -f "${ROOT}/ui/target/generated-resources/public/index.html" ]; then
            ok "已有前端产物 ui/target/generated-resources/public"
        else
            echo "  构建前端（首次会下载 node/yarn 与 npm 依赖，比较久，需要 ~4G 内存）"
            if [ "${DRY_RUN}" = true ]; then
                run bash -c "cd '${ROOT}/ui' && '${MVN}' -B package -DskipTests"
            else
                ( cd "${ROOT}/ui" && "${MVN}" -B package -DskipTests >/tmp/tb-ui-build.log 2>&1 ) \
                    || die "前端构建失败，见 /tmp/tb-ui-build.log"
                ok "前端产物已生成"
            fi
        fi
        # ui 模块跑的是 package.json 里的 build:prod，它硬编码了 `--base-href /thingsboard/`，
        # 而本仓库的入口网关把界面挂在根路径 —— 不纠正的话页面能打开但所有资源 404
        INDEX_HTML="${ROOT}/ui/target/generated-resources/public/index.html"
        if [ -f "${INDEX_HTML}" ] && ! grep -q "base href=\"${UI_BASE_HREF}\"" "${INDEX_HTML}"; then
            if [ "${DRY_RUN}" = false ]; then
                sed -i.bak "s|<base href=\"[^\"]*\"|<base href=\"${UI_BASE_HREF}\"|" "${INDEX_HTML}" \
                    && rm -f "${INDEX_HTML}.bak" \
                    && ok "已把前端 base href 纠正为 ${UI_BASE_HREF}"
            else
                warn "dry-run：本会把前端 base href 纠正为 ${UI_BASE_HREF}"
            fi
        fi
        run bash "${DOCKER_DIR}/build-images.sh" --with-node-images || die "镜像构建失败"
    else
        warn "跳过前端与 js-executor：没有管理界面（后端 API 仍可用）"
        run bash "${DOCKER_DIR}/build-images.sh" || die "镜像构建失败"
    fi
    ok "镜像构建完成"
else
    # shellcheck disable=SC1091
    source "${DOCKER_DIR}/.env"
    repo="${DOCKER_REPO:-thingsboard}"; tag="${TB_VERSION:-latest}"
    for svc in tb-core tb-rule-engine tb-http-transport tb-mqtt-transport tb-coap-transport \
               tb-lwm2m-transport tb-snmp-transport tb-tcp-transport tb-udp-transport; do
        img="${repo}/${svc}:${tag}"
        docker image inspect "${img}" >/dev/null 2>&1 || docker pull "${img}" || die "拉取 ${img} 失败"
    done
    if [ "${BUILD_UI}" = true ]; then
        img="${repo}/tb-web-ui:${tag}"
        docker image inspect "${img}" >/dev/null 2>&1 || docker pull "${img}" || die "拉取 ${img} 失败"
    fi
    ok "所需镜像已就绪"
fi

# Kafka 镜像是自建的（docker/kafka-image/，见 DEPLOY.md）——确保它在
if [ "${QUEUE_TYPE}" = "kafka" ]; then
    KAFKA_IMG="${KAFKA_IMAGE:-thingsboard/tb-kafka:3.7.1}"
    if docker image inspect "${KAFKA_IMG}" >/dev/null 2>&1; then
        ok "已有 Kafka 镜像 ${KAFKA_IMG}"
    else
        run bash "${DOCKER_DIR}/kafka-image/build.sh" \
            || die "Kafka 镜像构建失败 —— 它需要一个 Kafka 发行版：
    KAFKA_DIST=/path/to/kafka_2.13-3.7.1 bash docker/deploy.sh ...
  （发行版从 Apache 官方或国内镜像下 kafka_2.13-3.7.1.tgz 解压即可）"
        ok "Kafka 镜像已构建"
    fi
fi

# ---------------------------------------------------------------- 4. 写配置

step "写入部署配置"

ENV_FILE="${DOCKER_DIR}/.env"
PG_ENV_FILE="${DOCKER_DIR}/tb-node.postgres.env"

if [ "${DRY_RUN}" = true ]; then
    warn "dry-run：跳过写配置（本来会写 ${ENV_FILE} 与 ${PG_ENV_FILE}）"
else
    [ -f "${PG_ENV_FILE}.deploy-bak" ] || cp "${PG_ENV_FILE}" "${PG_ENV_FILE}.deploy-bak"

    sed -i "s|^TB_VERSION=.*|TB_VERSION=${TB_VERSION}|"      "${ENV_FILE}"
    sed -i "s|^TB_QUEUE_TYPE=.*|TB_QUEUE_TYPE=${QUEUE_TYPE}|" "${ENV_FILE}"
    sed -i "s|^CACHE=.*|CACHE=${CACHE_TYPE}|"                 "${ENV_FILE}"

    # 用 printf 写，避免口令里的 $ / 反引号被展开
    {
        printf '# ThingsBoard server configuration for PostgreSQL database\n'
        printf '# 由 docker/deploy.sh 生成（原文件备份在 %s.deploy-bak）\n\n' "${PG_ENV_FILE##*/}"
        printf 'DATABASE_TS_TYPE=sql\n'
        printf 'SPRING_DRIVER_CLASS_NAME=org.postgresql.Driver\n'
        printf 'SPRING_DATASOURCE_URL=%s\n' "${DB_URL}"
        printf 'SPRING_DATASOURCE_USERNAME=%s\n' "${DB_USER}"
        printf 'SPRING_DATASOURCE_PASSWORD=%s\n' "${DB_PASSWORD}"
    } > "${PG_ENV_FILE}"
fi

ok ".env：tag=${TB_VERSION} queue=${QUEUE_TYPE} cache=${CACHE_TYPE}"
ok "数据库连接写入 docker/tb-node.postgres.env"
warn "队列/缓存用的是 compose 自带的 bitnami 镜像；网络拉不动就改用外部服务"

# ---------------------------------------------------------------- 5. 证书

step "入口网关证书"

if [ -f "${DOCKER_DIR}/nginx/certs/tls.pem" ] && [ -f "${DOCKER_DIR}/nginx/certs/tls.key" ]; then
    ok "已存在 docker/nginx/certs/tls.pem"
elif [ "${DRY_RUN}" = true ]; then
    run bash "${DOCKER_DIR}/nginx/gen-self-signed-cert.sh"
else
    bash "${DOCKER_DIR}/nginx/gen-self-signed-cert.sh"
    warn "当前是自签证书，浏览器会提示不受信任；生产请换成正式证书"
fi

# ---------------------------------------------------------------- 6. 起栈

step "起栈"

if command -v sudo >/dev/null 2>&1 && [ "${SKIP_CHOWN}" = false ]; then
    if [ "${DRY_RUN}" = true ]; then
        run bash -c "cd '${DOCKER_DIR}' && ./docker-create-log-folders.sh"
    else
        ( cd "${DOCKER_DIR}" && ./docker-create-log-folders.sh ) \
            || die "创建日志目录失败（需要 sudo：日志目录属主必须是镜像里的 thingsboard 用户 uid=999）"
        ok "日志/数据目录已就绪"
    fi
else
    warn "跳过 create-log-folders（--skip-chown 或没有 sudo）。容器写不了日志时手工："
    warn "  sudo chown -R 999:999 docker/tb-*/log docker/tb-transports/*/log"
fi

cd "${DOCKER_DIR}"
# shellcheck disable=SC1091
source compose-utils.sh
COMPOSE_ARGS="$(additionalComposeCacheArgs) $(additionalComposeArgs) $(additionalComposeQueueArgs) $(additionalComposeMonitoringArgs) $(additionalComposeEdqsArgs)"
COMPOSE=(docker compose -f docker-compose.yml)
# COMPOSE_ARGS 本身就是 "-f a.yml -f b.yml" 形式，直接按空白切开追加
# shellcheck disable=SC2206
COMPOSE+=(${COMPOSE_ARGS})

# 只启动我们确实有镜像的服务。compose 里还有 tb-vc-executor（版本控制功能，可选）等，
# 直接 up -d 全量会因为缺镜像而整体失败，所以这里显式列举。
SERVICES="zookeeper tb-core1 tb-core2 tb-rule-engine1 tb-rule-engine2 \
tb-mqtt-transport1 tb-mqtt-transport2 tb-http-transport1 tb-http-transport2 \
tb-coap-transport tb-lwm2m-transport tb-snmp-transport \
tb-tcp-transport1 tb-tcp-transport2 tb-udp-transport1 tb-udp-transport2 nginx-gateway"

# 队列：compose 里只有 kafka 是自带容器（bitnami/kafka:3.7.0，TB 连 kafka:9092）；
# 其余 queue 类型对应的 compose 文件只改 tb 服务的环境变量，broker 在外部。
case "${QUEUE_TYPE}" in
    kafka) SERVICES="${SERVICES} kafka" ;;
    *)     warn "队列类型 ${QUEUE_TYPE}：compose 不带 broker 容器，假定它在外部" ;;
esac

# 缓存：redis 三种形态都是自带容器
case "${CACHE_TYPE}" in
    redis)          SERVICES="${SERVICES} redis" ;;
    redis-cluster)  SERVICES="${SERVICES} redis-node-0 redis-node-1 redis-node-2 redis-node-3 redis-node-4 redis-node-5" ;;
    redis-sentinel) SERVICES="${SERVICES} redis-master redis-slave redis-sentinel" ;;
    *)              warn "缓存类型 ${CACHE_TYPE}：假定缓存服务在外部" ;;
esac

SCALE=()
if [ "${BUILD_UI}" = true ]; then
    SERVICES="${SERVICES} tb-web-ui1 tb-web-ui2 tb-js-executor"
    SCALE=(--scale tb-js-executor=2)     # compose 里 js-executor 写的是 replicas: 10
fi

if [ "${DB_MODE}" = dump ]; then
    echo "  先起 postgres 并恢复 dump（首次要初始化数据目录，稍等）"
    run "${COMPOSE[@]}" up -d postgres || die "启动 postgres 失败"
    if [ "${DRY_RUN}" = false ]; then
        ready=false
        for _ in $(seq 1 60); do
            "${COMPOSE[@]}" exec -T postgres pg_isready -U "${DB_USER}" >/dev/null 2>&1 && { ready=true; break; }
            sleep 2
        done
        [ "${ready}" = true ] || die "postgres 未就绪"
        if [[ "${DB_DUMP}" == *.sql ]]; then
            "${COMPOSE[@]}" exec -T postgres psql -v ON_ERROR_STOP=1 -U "${DB_USER}" -d thingsboard < "${DB_DUMP}" \
                || die "导入 SQL 失败"
        else
            "${COMPOSE[@]}" cp "${DB_DUMP}" postgres:/tmp/tb.dump || die "拷贝 dump 失败"
            "${COMPOSE[@]}" exec -T postgres pg_restore -U "${DB_USER}" -d thingsboard --no-owner /tmp/tb.dump \
                || die "恢复 dump 失败"
        fi
        ok "dump 已恢复"
    fi
    SERVICES="${SERVICES} postgres"
fi

run "${COMPOSE[@]}" up -d ${SCALE[@]+"${SCALE[@]}"} ${SERVICES} || die "起栈失败"
[ "${DRY_RUN}" = true ] || ok "compose 已启动"

# ---------------------------------------------------------------- 7. 验证

step "验证"

if [ "${DRY_RUN}" = true ]; then
    warn "dry-run：跳过验证（本来会轮询 http://127.0.0.1/api/auth/login 直到 200）"
    code="(dry-run)"
else
    echo "  等待入口就绪（最多 5 分钟）"
    code=""
    for _ in $(seq 1 60); do
        code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1/api/auth/login \
            -H 'Content-Type: application/json' \
            -d '{"username":"sysadmin@thingsboard.org","password":"sysadmin"}' --max-time 5 || true)
        [ "${code}" = "200" ] && break
        sleep 5
    done
fi

if [ "${code}" = "200" ]; then
    ok "登录接口 200"
elif [ "${DRY_RUN}" = false ]; then
    warn "登录接口返回 ${code}（可能还没起完，或库不对）"
    warn "排查：cd docker && docker compose -f docker-compose.yml logs --tail=100 tb-core1"
fi

printf '\n%s=== 部署完成 ===%s\n' "${C_OK}" "${C_END}"
host_ip="$(hostname -I 2>/dev/null | awk '{print $1}')"; host_ip="${host_ip:-<本机IP>}"
cat <<EOF

访问地址（把 ${host_ip} 换成对外地址/域名）：
  管理界面      http://${host_ip}/            默认账号 sysadmin@thingsboard.org / sysadmin
  管理界面 TLS  https://${host_ip}/          当前自签证书，浏览器会提示不受信任
  REST API      http://${host_ip}/api/**
  设备 HTTP     http://${host_ip}/api/v1/**
  设备 MQTT     ${host_ip}:1883
  设备 TCP      ${host_ip}:5683
  设备 UDP      ${host_ip}:5684/udp

防火墙要放开：80, 443, 1883, 5683/tcp, 5684/udp

常用操作（都在 docker/ 目录下执行）：
  状态      docker compose -f docker-compose.yml ps
  日志      docker compose -f docker-compose.yml logs -f tb-core1
  重启      ./docker-stop-services.sh && ./docker-start-services.sh
  改 nginx  docker exec tb-gateway nginx -s reload

要点：
  - 入口只有一个容器 tb-gateway（nginx），配置在 docker/nginx/config/，证书在 docker/nginx/certs/
  - 这套镜像**不能初始化数据库**；换库或升级 schema 要自己处理（docker/DEPLOY.md 第 1 节）
EOF
