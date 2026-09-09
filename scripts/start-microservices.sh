#!/usr/bin/env bash
# 本地顺序启动 ThingsBoard 微服务。
#
# 默认每种服务 2 个实例（同机端口自动错开）：
#   1) 基础设施已就绪（ZK / Kafka / Postgres / Redis）
#   2) tb-core × 2
#   3) tb-rule-engine × 2
#   4) HTTP / MQTT / TCP / UDP 传输各 × 2
#
# 用法（必须用 bash，不要用 sh）:
#   bash start-microservices.sh start
#   bash start-microservices.sh start tb-mqtt-transport2
#   bash start-microservices.sh stop tb-mqtt-transport2
#   ./start-microservices.sh stop
#   ./start-microservices.sh status
#   ./start-microservices.sh restart [name]

if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)"
RUN_DIR="${ROOT}/.run/microservices"
LOG_DIR="${RUN_DIR}/logs"
PID_DIR="${RUN_DIR}/pids"
VERSION="${TB_VERSION:-4.0.1}"

CORE_REPLICAS="${CORE_REPLICAS:-2}"
RULE_ENGINE_REPLICAS="${RULE_ENGINE_REPLICAS:-2}"
HTTP_TRANSPORT_REPLICAS="${HTTP_TRANSPORT_REPLICAS:-2}"
MQTT_TRANSPORT_REPLICAS="${MQTT_TRANSPORT_REPLICAS:-2}"
TCP_TRANSPORT_REPLICAS="${TCP_TRANSPORT_REPLICAS:-2}"
UDP_TRANSPORT_REPLICAS="${UDP_TRANSPORT_REPLICAS:-2}"
COAP_TRANSPORT_REPLICAS="${COAP_TRANSPORT_REPLICAS:-0}"
SNMP_TRANSPORT_REPLICAS="${SNMP_TRANSPORT_REPLICAS:-0}"
LWM2M_TRANSPORT_REPLICAS="${LWM2M_TRANSPORT_REPLICAS:-0}"

CORE_HTTP_BASE="${CORE_HTTP_BASE:-8080}"
RE_HTTP_BASE="${RE_HTTP_BASE:-8082}"
HTTP_TRANSPORT_HTTP_BASE="${HTTP_TRANSPORT_HTTP_BASE:-8081}"
MQTT_TRANSPORT_HTTP_BASE="${MQTT_TRANSPORT_HTTP_BASE:-8083}"
MQTT_BIND_BASE="${MQTT_BIND_BASE:-1883}"
TCP_TRANSPORT_HTTP_BASE="${TCP_TRANSPORT_HTTP_BASE:-8087}"
TCP_BIND_BASE="${TCP_BIND_BASE:-5683}"
UDP_TRANSPORT_HTTP_BASE="${UDP_TRANSPORT_HTTP_BASE:-8088}"
UDP_BIND_BASE="${UDP_BIND_BASE:-5684}"
COAP_TRANSPORT_HTTP_BASE="${COAP_TRANSPORT_HTTP_BASE:-8084}"
SNMP_TRANSPORT_HTTP_BASE="${SNMP_TRANSPORT_HTTP_BASE:-8086}"
LWM2M_TRANSPORT_HTTP_BASE="${LWM2M_TRANSPORT_HTTP_BASE:-8085}"

ZK_HOST="${ZK_HOST:-localhost}"
ZK_PORT="${ZK_PORT:-2181}"
KAFKA_HOST="${KAFKA_HOST:-localhost}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
POSTGRES_HOST="${POSTGRES_HOST:-10.2.0.120}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"

CORE_WAIT_SEC="${CORE_WAIT_SEC:-180}"
NODE_WAIT_SEC="${NODE_WAIT_SEC:-120}"
CORE_XMX="${CORE_XMX:-2g}"
RE_XMX="${RE_XMX:-2g}"
TRANSPORT_XMX="${TRANSPORT_XMX:-512m}"

if [[ -z "${JAVA_HOME:-}" && -d "/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home" ]]; then
  export JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home"
fi
if [[ -n "${JAVA_HOME:-}" ]]; then
  export PATH="${JAVA_HOME}/bin:${PATH}"
fi

maven_version() {
  local bin="$1"
  "$bin" -version 2>/dev/null \
    | sed $'s/\x1b\\[[0-9;]*[A-Za-z]//g' \
    | sed -n 's/.*Apache Maven \([0-9][0-9.]*\).*/\1/p' \
    | head -n 1
}

# maven-jar-plugin 3.4.0 要求 >= 3.6.3
maven_at_least_363() {
  local ver="${1%%[^0-9.]*}"
  [[ -n "${ver}" ]] || return 1
  local major="${ver%%.*}"
  local rest="${ver#*.}"
  local minor="${rest%%.*}"
  local patch="${rest#*.}"
  patch="${patch%%.*}"
  major="${major:-0}"
  minor="${minor:-0}"
  patch="${patch:-0}"
  [[ "${major}" =~ ^[0-9]+$ && "${minor}" =~ ^[0-9]+$ && "${patch}" =~ ^[0-9]+$ ]] || return 1
  if (( major > 3 )); then return 0; fi
  if (( major < 3 )); then return 1; fi
  if (( minor > 6 )); then return 0; fi
  if (( minor < 6 )); then return 1; fi
  (( patch >= 3 ))
}

resolve_mvn() {
  local candidate ver
  if [[ -n "${MVN:-}" ]]; then
    if [[ -x "${MVN}" ]]; then
      ver="$(maven_version "${MVN}")"
      if maven_at_least_363 "${ver}"; then
        echo "${MVN}"
        return
      fi
      echo "WARN: MVN=${MVN} is ${ver:-unknown}, need >= 3.6.3, ignoring" >&2
    fi
  fi
  if command -v mvn >/dev/null 2>&1; then
    candidate="$(command -v mvn)"
    ver="$(maven_version "${candidate}")"
    if maven_at_least_363 "${ver}"; then
      echo "${candidate}"
      return
    fi
    echo "WARN: PATH mvn is ${ver:-unknown}, need >= 3.6.3, looking for wrapper" >&2
  fi
  local best="" best_ver="0.0.0"
  local mvn_list
  mvn_list="$(find "${HOME}/.m2/wrapper/dists" -type f -path "*/bin/mvn" 2>/dev/null | sort || true)"
  while IFS= read -r candidate; do
    [ -z "${candidate}" ] && continue
    [ -x "${candidate}" ] || continue
    ver="$(maven_version "${candidate}")"
    maven_at_least_363 "${ver}" || continue
    if [ "${ver}" \> "${best_ver}" ]; then
      best="${candidate}"
      best_ver="${ver}"
    fi
  done <<EOF
${mvn_list}
EOF
  echo "${best}"
}

port_for_replica() {
  local base="$1"
  local idx="$2"
  echo $((base + (idx - 1) * 10000))
}

mqtt_bind_for_replica() {
  local idx="$1"
  echo $((MQTT_BIND_BASE + idx - 1))
}

tcp_bind_for_replica() {
  local idx="$1"
  echo $((TCP_BIND_BASE + (idx - 1) * 10))
}

udp_bind_for_replica() {
  local idx="$1"
  echo $((UDP_BIND_BASE + (idx - 1) * 10))
}

port_open() {
  local host="$1"
  local port="$2"
  if command -v nc >/dev/null 2>&1; then
    nc -z "${host}" "${port}" >/dev/null 2>&1
  else
    bash -c "echo >/dev/tcp/${host}/${port}" >/dev/null 2>&1
  fi
}

process_alive() {
  local pid_file="$1"
  [[ -n "${pid_file}" && -f "${pid_file}" ]] || return 0
  local pid
  pid="$(cat "${pid_file}")"
  kill -0 "${pid}" >/dev/null 2>&1
}

log_has_started() {
  local name="$1"
  local log_file="${LOG_DIR}/${name}.log"
  [[ -f "${log_file}" ]] && grep -q "Started .*Application" "${log_file}" 2>/dev/null
}

# MQTT/TCP/UDP 默认 web-application-type=none，不会监听 HTTP_BIND_PORT。
# 任一 TCP 端口可连，或日志出现 Started，即视为就绪。
wait_ready() {
  local name="$1"
  local pid_file="$2"
  local timeout="$3"
  shift 3
  local elapsed=0
  if (( $# > 0 )); then
    echo "    waiting ${name} on $* or Started log (timeout ${timeout}s)"
  else
    echo "    waiting ${name} Started log (timeout ${timeout}s)"
  fi
  while (( elapsed < timeout )); do
    if ! process_alive "${pid_file}"; then
      echo "ERROR: ${name} process exited. See ${LOG_DIR}/${name}.log" >&2
      tail -n 40 "${LOG_DIR}/${name}.log" >&2 || true
      return 1
    fi
    local p
    for p in "$@"; do
      if port_open localhost "${p}"; then
        echo "    ${name} is up (port ${p})"
        return 0
      fi
    done
    if log_has_started "${name}"; then
      echo "    ${name} is up (Started)"
      return 0
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
  echo "ERROR: ${name} did not become ready within ${timeout}s" >&2
  tail -n 40 "${LOG_DIR}/${name}.log" >&2 || true
  return 1
}

find_boot_jar() {
  local module_dir="$1"
  local pkg_name="$2"
  local jar="${ROOT}/${module_dir}/target/${pkg_name}-${VERSION}-boot.jar"
  if [[ -f "${jar}" ]]; then
    echo "${jar}"
    return 0
  fi
  local match
  match="$(ls -1 "${ROOT}/${module_dir}/target/${pkg_name}"-*-boot.jar 2>/dev/null | head -n 1 || true)"
  if [[ -n "${match}" ]]; then
    echo "${match}"
    return 0
  fi
  echo ""
}

ensure_boot_jars() {
  local missing=()
  local spec
  for spec in "$@"; do
    local module="${spec%%:*}"
    local pkg="${spec##*:}"
    if [[ -z "$(find_boot_jar "${module}" "${pkg}")" ]]; then
      missing+=("${module}")
    fi
  done
  if (( ${#missing[@]} == 0 )); then
    echo "Boot jars already present."
    return 0
  fi
  local mvn
  mvn="$(resolve_mvn)"
  if [[ -z "${mvn}" ]]; then
    echo "ERROR: need Maven >= 3.6.3 (maven-jar-plugin 3.4.0)." >&2
    echo "PATH mvn is too old. Set MVN to a 3.6.3+ binary, for example:" >&2
    echo "  MVN=\"\$HOME/.m2/wrapper/dists/apache-maven-3.6.3-bin/1iopthnavndlasol9gbrbg6bf2/apache-maven-3.6.3/bin/mvn\" $0 start" >&2
    echo "Missing modules: ${missing[*]}" >&2
    exit 1
  fi
  echo "Packaging boot jars with $(maven_version "${mvn}") (${mvn})"
  echo "First run may take a few minutes..."
  local pl
  pl="$(IFS=,; echo "${missing[*]}")"
  # maven.test.skip: 不编译测试（-DskipTests 仍会 testCompile）
  # pkg.package.phase=none: 跳过 gradle 打 deb/rpm（本机 gradle 路径是 Windows）
  (cd "${ROOT}" && "${mvn}" -pl "${pl}" -am package \
      -DskipTests -Dmaven.test.skip=true -Dpkg.package.phase=none)
}

start_java() {
  local name="$1"
  local jar="$2"
  local xmx="$3"
  shift 3
  mkdir -p "${LOG_DIR}" "${PID_DIR}"
  local pid_file="${PID_DIR}/${name}.pid"
  local log_file="${LOG_DIR}/${name}.log"
  if [[ -f "${pid_file}" ]]; then
    local old
    old="$(cat "${pid_file}")"
    if kill -0 "${old}" >/dev/null 2>&1; then
      echo "    skip ${name}, already running pid ${old}"
      return 0
    fi
    rm -f "${pid_file}"
  fi
  echo "  starting ${name}"
  (
    cd "${ROOT}"
    nohup env "$@" java \
      -Xms256m -Xmx"${xmx}" \
      -Dfile.encoding=UTF-8 \
      -jar "${jar}" \
      > "${log_file}" 2>&1 &
    echo $! > "${pid_file}"
  )
}

stop_one() {
  local name="$1"
  local pid_file="${PID_DIR}/${name}.pid"
  if [[ ! -f "${pid_file}" ]]; then
    return 0
  fi
  local pid
  pid="$(cat "${pid_file}")"
  if kill -0 "${pid}" >/dev/null 2>&1; then
    echo "  stopping ${name} (${pid})"
    kill "${pid}" >/dev/null 2>&1 || true
    local i=0
    while kill -0 "${pid}" >/dev/null 2>&1 && (( i < 20 )); do
      sleep 0.5
      i=$((i + 1))
    done
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill -9 "${pid}" >/dev/null 2>&1 || true
    fi
  fi
  rm -f "${pid_file}"
}

list_started_names() {
  [[ -d "${PID_DIR}" ]] || return 0
  ls -1 "${PID_DIR}"/*.pid 2>/dev/null | xargs -n1 basename | sed 's/\.pid$//' || true
}

check_infra() {
  echo "== checking infrastructure =="
  local failed=0
  if port_open "${ZK_HOST}" "${ZK_PORT}"; then
    echo "  ZooKeeper ${ZK_HOST}:${ZK_PORT} OK"
  else
    echo "  ERROR: ZooKeeper ${ZK_HOST}:${ZK_PORT} is down" >&2
    failed=1
  fi
  if port_open "${KAFKA_HOST}" "${KAFKA_PORT}"; then
    echo "  Kafka ${KAFKA_HOST}:${KAFKA_PORT} OK"
  else
    echo "  WARN: Kafka ${KAFKA_HOST}:${KAFKA_PORT} not reachable (queue may still work if you use another broker)"
  fi
  if port_open "${POSTGRES_HOST}" "${POSTGRES_PORT}"; then
    echo "  Postgres ${POSTGRES_HOST}:${POSTGRES_PORT} OK"
  else
    echo "  ERROR: Postgres ${POSTGRES_HOST}:${POSTGRES_PORT} is down" >&2
    failed=1
  fi
  if port_open "${REDIS_HOST}" "${REDIS_PORT}"; then
    echo "  Redis ${REDIS_HOST}:${REDIS_PORT} OK"
  else
    echo "  WARN: Redis ${REDIS_HOST}:${REDIS_PORT} not reachable"
  fi
  if (( failed != 0 )); then
    echo "Start ZooKeeper and Postgres first, then rerun." >&2
    exit 1
  fi
}

start_core_replicas() {
  local jar
  jar="$(find_boot_jar "apps/tb-core" "thingsboard-core")"
  local i http
  for (( i = 1; i <= CORE_REPLICAS; i++ )); do
    http="$(port_for_replica "${CORE_HTTP_BASE}" "${i}")"
    start_java "tb-core${i}" "${jar}" "${CORE_XMX}" \
      TB_SERVICE_ID="tb-core${i}" \
      TB_SERVICE_TYPE="tb-core" \
      HTTP_BIND_PORT="${http}"
    wait_ready "tb-core${i}" "${PID_DIR}/tb-core${i}.pid" "${CORE_WAIT_SEC}" "${http}"
  done
}

start_re_replicas() {
  local jar
  jar="$(find_boot_jar "apps/tb-rule-engine" "thingsboard-rule-engine")"
  local i http
  for (( i = 1; i <= RULE_ENGINE_REPLICAS; i++ )); do
    http="$(port_for_replica "${RE_HTTP_BASE}" "${i}")"
    start_java "tb-rule-engine${i}" "${jar}" "${RE_XMX}" \
      TB_SERVICE_ID="tb-rule-engine${i}" \
      TB_SERVICE_TYPE="tb-rule-engine" \
      HTTP_BIND_PORT="${http}"
    wait_ready "tb-rule-engine${i}" "${PID_DIR}/tb-rule-engine${i}.pid" "${NODE_WAIT_SEC}" "${http}"
  done
}

start_http_transport_replicas() {
  local jar
  jar="$(find_boot_jar "apps/tb-transport/tb-http-transport" "tb-http-transport")"
  local i http
  for (( i = 1; i <= HTTP_TRANSPORT_REPLICAS; i++ )); do
    http="$(port_for_replica "${HTTP_TRANSPORT_HTTP_BASE}" "${i}")"
    start_java "tb-http-transport${i}" "${jar}" "${TRANSPORT_XMX}" \
      TB_SERVICE_ID="tb-http-transport${i}" \
      HTTP_BIND_PORT="${http}"
    wait_ready "tb-http-transport${i}" "${PID_DIR}/tb-http-transport${i}.pid" "${NODE_WAIT_SEC}" "${http}"
  done
}

start_mqtt_transport_replicas() {
  local jar
  jar="$(find_boot_jar "apps/tb-transport/tb-mqtt-transport" "tb-mqtt-transport")"
  local i http mqtt
  for (( i = 1; i <= MQTT_TRANSPORT_REPLICAS; i++ )); do
    http="$(port_for_replica "${MQTT_TRANSPORT_HTTP_BASE}" "${i}")"
    mqtt="$(mqtt_bind_for_replica "${i}")"
    start_java "tb-mqtt-transport${i}" "${jar}" "${TRANSPORT_XMX}" \
      TB_SERVICE_ID="tb-mqtt-transport${i}" \
      HTTP_BIND_PORT="${http}" \
      MQTT_BIND_PORT="${mqtt}"
    wait_ready "tb-mqtt-transport${i}" "${PID_DIR}/tb-mqtt-transport${i}.pid" "${NODE_WAIT_SEC}" "${mqtt}"
  done
}

start_tcp_transport_replicas() {
  local jar
  jar="$(find_boot_jar "apps/tb-transport/tb-tcp-transport" "tb-tcp-transport")"
  local i http tcp
  for (( i = 1; i <= TCP_TRANSPORT_REPLICAS; i++ )); do
    http="$(port_for_replica "${TCP_TRANSPORT_HTTP_BASE}" "${i}")"
    tcp="$(tcp_bind_for_replica "${i}")"
    start_java "tb-tcp-transport${i}" "${jar}" "${TRANSPORT_XMX}" \
      TB_SERVICE_ID="tb-tcp-transport${i}" \
      HTTP_BIND_PORT="${http}" \
      TCP_BIND_PORT="${tcp}"
    wait_ready "tb-tcp-transport${i}" "${PID_DIR}/tb-tcp-transport${i}.pid" "${NODE_WAIT_SEC}" "${tcp}"
  done
}

start_udp_transport_replicas() {
  local jar
  jar="$(find_boot_jar "apps/tb-transport/tb-udp-transport" "tb-udp-transport")"
  local i http udp
  for (( i = 1; i <= UDP_TRANSPORT_REPLICAS; i++ )); do
    http="$(port_for_replica "${UDP_TRANSPORT_HTTP_BASE}" "${i}")"
    udp="$(udp_bind_for_replica "${i}")"
    start_java "tb-udp-transport${i}" "${jar}" "${TRANSPORT_XMX}" \
      TB_SERVICE_ID="tb-udp-transport${i}" \
      HTTP_BIND_PORT="${http}" \
      UDP_BIND_PORT="${udp}"
    wait_ready "tb-udp-transport${i}" "${PID_DIR}/tb-udp-transport${i}.pid" "${NODE_WAIT_SEC}"
  done
}

start_coap_transport_replicas() {
  local jar
  jar="$(find_boot_jar "apps/tb-transport/tb-coap-transport" "tb-coap-transport")"
  local i http
  for (( i = 1; i <= COAP_TRANSPORT_REPLICAS; i++ )); do
    http="$(port_for_replica "${COAP_TRANSPORT_HTTP_BASE}" "${i}")"
    start_java "tb-coap-transport${i}" "${jar}" "${TRANSPORT_XMX}" \
      TB_SERVICE_ID="tb-coap-transport${i}" \
      HTTP_BIND_PORT="${http}"
    wait_ready "tb-coap-transport${i}" "${PID_DIR}/tb-coap-transport${i}.pid" "${NODE_WAIT_SEC}" "${http}"
  done
}

start_snmp_transport_replicas() {
  local jar
  jar="$(find_boot_jar "apps/tb-transport/tb-snmp-transport" "tb-snmp-transport")"
  local i http
  for (( i = 1; i <= SNMP_TRANSPORT_REPLICAS; i++ )); do
    http="$(port_for_replica "${SNMP_TRANSPORT_HTTP_BASE}" "${i}")"
    start_java "tb-snmp-transport${i}" "${jar}" "${TRANSPORT_XMX}" \
      TB_SERVICE_ID="tb-snmp-transport${i}" \
      HTTP_BIND_PORT="${http}"
    wait_ready "tb-snmp-transport${i}" "${PID_DIR}/tb-snmp-transport${i}.pid" "${NODE_WAIT_SEC}" "${http}"
  done
}

start_lwm2m_transport_replicas() {
  local jar
  jar="$(find_boot_jar "apps/tb-transport/tb-lwm2m-transport" "tb-lwm2m-transport")"
  local i http
  for (( i = 1; i <= LWM2M_TRANSPORT_REPLICAS; i++ )); do
    http="$(port_for_replica "${LWM2M_TRANSPORT_HTTP_BASE}" "${i}")"
    start_java "tb-lwm2m-transport${i}" "${jar}" "${TRANSPORT_XMX}" \
      TB_SERVICE_ID="tb-lwm2m-transport${i}" \
      HTTP_BIND_PORT="${http}"
    wait_ready "tb-lwm2m-transport${i}" "${PID_DIR}/tb-lwm2m-transport${i}.pid" "${NODE_WAIT_SEC}" "${http}"
  done
}

start_named() {
  local name="$1"
  local idx jar http mqtt tcp udp
  if [[ "${name}" =~ ^tb-core([0-9]+)$ ]]; then
    idx="${BASH_REMATCH[1]}"
    jar="$(find_boot_jar "apps/tb-core" "thingsboard-core")"
    [[ -n "${jar}" ]] || { echo "ERROR: boot jar not found for ${name}" >&2; return 1; }
    http="$(port_for_replica "${CORE_HTTP_BASE}" "${idx}")"
    start_java "${name}" "${jar}" "${CORE_XMX}" \
      TB_SERVICE_ID="${name}" TB_SERVICE_TYPE="tb-core" HTTP_BIND_PORT="${http}"
    wait_ready "${name}" "${PID_DIR}/${name}.pid" "${CORE_WAIT_SEC}" "${http}"
  elif [[ "${name}" =~ ^tb-rule-engine([0-9]+)$ ]]; then
    idx="${BASH_REMATCH[1]}"
    jar="$(find_boot_jar "apps/tb-rule-engine" "thingsboard-rule-engine")"
    [[ -n "${jar}" ]] || { echo "ERROR: boot jar not found for ${name}" >&2; return 1; }
    http="$(port_for_replica "${RE_HTTP_BASE}" "${idx}")"
    start_java "${name}" "${jar}" "${RE_XMX}" \
      TB_SERVICE_ID="${name}" TB_SERVICE_TYPE="tb-rule-engine" HTTP_BIND_PORT="${http}"
    wait_ready "${name}" "${PID_DIR}/${name}.pid" "${NODE_WAIT_SEC}" "${http}"
  elif [[ "${name}" =~ ^tb-http-transport([0-9]+)$ ]]; then
    idx="${BASH_REMATCH[1]}"
    jar="$(find_boot_jar "apps/tb-transport/tb-http-transport" "tb-http-transport")"
    [[ -n "${jar}" ]] || { echo "ERROR: boot jar not found for ${name}" >&2; return 1; }
    http="$(port_for_replica "${HTTP_TRANSPORT_HTTP_BASE}" "${idx}")"
    start_java "${name}" "${jar}" "${TRANSPORT_XMX}" \
      TB_SERVICE_ID="${name}" HTTP_BIND_PORT="${http}"
    wait_ready "${name}" "${PID_DIR}/${name}.pid" "${NODE_WAIT_SEC}" "${http}"
  elif [[ "${name}" =~ ^tb-mqtt-transport([0-9]+)$ ]]; then
    idx="${BASH_REMATCH[1]}"
    jar="$(find_boot_jar "apps/tb-transport/tb-mqtt-transport" "tb-mqtt-transport")"
    [[ -n "${jar}" ]] || { echo "ERROR: boot jar not found for ${name}" >&2; return 1; }
    http="$(port_for_replica "${MQTT_TRANSPORT_HTTP_BASE}" "${idx}")"
    mqtt="$(mqtt_bind_for_replica "${idx}")"
    start_java "${name}" "${jar}" "${TRANSPORT_XMX}" \
      TB_SERVICE_ID="${name}" HTTP_BIND_PORT="${http}" MQTT_BIND_PORT="${mqtt}"
    wait_ready "${name}" "${PID_DIR}/${name}.pid" "${NODE_WAIT_SEC}" "${mqtt}"
  elif [[ "${name}" =~ ^tb-tcp-transport([0-9]+)$ ]]; then
    idx="${BASH_REMATCH[1]}"
    jar="$(find_boot_jar "apps/tb-transport/tb-tcp-transport" "tb-tcp-transport")"
    [[ -n "${jar}" ]] || { echo "ERROR: boot jar not found for ${name}" >&2; return 1; }
    http="$(port_for_replica "${TCP_TRANSPORT_HTTP_BASE}" "${idx}")"
    tcp="$(tcp_bind_for_replica "${idx}")"
    start_java "${name}" "${jar}" "${TRANSPORT_XMX}" \
      TB_SERVICE_ID="${name}" HTTP_BIND_PORT="${http}" TCP_BIND_PORT="${tcp}"
    wait_ready "${name}" "${PID_DIR}/${name}.pid" "${NODE_WAIT_SEC}" "${tcp}"
  elif [[ "${name}" =~ ^tb-udp-transport([0-9]+)$ ]]; then
    idx="${BASH_REMATCH[1]}"
    jar="$(find_boot_jar "apps/tb-transport/tb-udp-transport" "tb-udp-transport")"
    [[ -n "${jar}" ]] || { echo "ERROR: boot jar not found for ${name}" >&2; return 1; }
    http="$(port_for_replica "${UDP_TRANSPORT_HTTP_BASE}" "${idx}")"
    udp="$(udp_bind_for_replica "${idx}")"
    start_java "${name}" "${jar}" "${TRANSPORT_XMX}" \
      TB_SERVICE_ID="${name}" HTTP_BIND_PORT="${http}" UDP_BIND_PORT="${udp}"
    wait_ready "${name}" "${PID_DIR}/${name}.pid" "${NODE_WAIT_SEC}"
  else
    echo "ERROR: unknown node '${name}'" >&2
    usage
    return 1
  fi
}

collect_needed_jars() {
  local needed=()
  if (( CORE_REPLICAS > 0 )); then needed+=("apps/tb-core:thingsboard-core"); fi
  if (( RULE_ENGINE_REPLICAS > 0 )); then needed+=("apps/tb-rule-engine:thingsboard-rule-engine"); fi
  if (( HTTP_TRANSPORT_REPLICAS > 0 )); then needed+=("apps/tb-transport/tb-http-transport:tb-http-transport"); fi
  if (( MQTT_TRANSPORT_REPLICAS > 0 )); then needed+=("apps/tb-transport/tb-mqtt-transport:tb-mqtt-transport"); fi
  if (( TCP_TRANSPORT_REPLICAS > 0 )); then needed+=("apps/tb-transport/tb-tcp-transport:tb-tcp-transport"); fi
  if (( UDP_TRANSPORT_REPLICAS > 0 )); then needed+=("apps/tb-transport/tb-udp-transport:tb-udp-transport"); fi
  if (( COAP_TRANSPORT_REPLICAS > 0 )); then needed+=("apps/tb-transport/tb-coap-transport:tb-coap-transport"); fi
  if (( SNMP_TRANSPORT_REPLICAS > 0 )); then needed+=("apps/tb-transport/tb-snmp-transport:tb-snmp-transport"); fi
  if (( LWM2M_TRANSPORT_REPLICAS > 0 )); then needed+=("apps/tb-transport/tb-lwm2m-transport:tb-lwm2m-transport"); fi
  echo "${needed[*]}"
}

do_start() {
  mkdir -p "${LOG_DIR}" "${PID_DIR}"
  check_infra
  # shellcheck disable=SC2046
  ensure_boot_jars $(collect_needed_jars)

  echo
  echo "== start order: Core -> Rule Engine -> HTTP -> MQTT -> TCP -> UDP =="
  echo "  core=${CORE_REPLICAS}  re=${RULE_ENGINE_REPLICAS}  http=${HTTP_TRANSPORT_REPLICAS}  mqtt=${MQTT_TRANSPORT_REPLICAS}  tcp=${TCP_TRANSPORT_REPLICAS}  udp=${UDP_TRANSPORT_REPLICAS}"
  echo

  if (( CORE_REPLICAS > 0 )); then
    echo "== 1/6 tb-core =="
    start_core_replicas
  fi
  if (( RULE_ENGINE_REPLICAS > 0 )); then
    echo "== 2/6 tb-rule-engine =="
    start_re_replicas
  fi
  if (( HTTP_TRANSPORT_REPLICAS > 0 )); then
    echo "== 3/6 tb-http-transport =="
    start_http_transport_replicas
  fi
  if (( MQTT_TRANSPORT_REPLICAS > 0 )); then
    echo "== 4/6 tb-mqtt-transport =="
    start_mqtt_transport_replicas
  fi
  if (( TCP_TRANSPORT_REPLICAS > 0 )); then
    echo "== 5/6 tb-tcp-transport =="
    start_tcp_transport_replicas
  fi
  if (( UDP_TRANSPORT_REPLICAS > 0 )); then
    echo "== 6/6 tb-udp-transport =="
    start_udp_transport_replicas
  fi
  if (( COAP_TRANSPORT_REPLICAS > 0 )); then
    echo "== extra tb-coap-transport =="
    start_coap_transport_replicas
  fi
  if (( SNMP_TRANSPORT_REPLICAS > 0 )); then
    echo "== extra tb-snmp-transport =="
    start_snmp_transport_replicas
  fi
  if (( LWM2M_TRANSPORT_REPLICAS > 0 )); then
    echo "== extra tb-lwm2m-transport =="
    start_lwm2m_transport_replicas
  fi

  echo
  echo "All requested nodes started. Logs: ${LOG_DIR}"
  echo "HTTP pull split: grep 'HTTP pull devices loaded' ${LOG_DIR}/tb-http-transport*.log"
  echo "MQTT pull sessions: grep 'Established MQTT pull collector session' ${LOG_DIR}/tb-mqtt-transport*.log"
  do_status
}

do_stop() {
  echo "== stopping microservices (reverse order) =="
  local names
  names="$(list_started_names | sort -r || true)"
  if [[ -z "${names}" ]]; then
    echo "  nothing to stop"
    return 0
  fi
  while IFS= read -r name; do
    [[ -n "${name}" ]] || continue
    stop_one "${name}"
  done <<< "${names}"
  echo "Stopped."
}

do_status() {
  echo "== status =="
  if [[ ! -d "${PID_DIR}" ]] || ! ls "${PID_DIR}"/*.pid >/dev/null 2>&1; then
    echo "  no tracked processes"
    return 0
  fi
  local pid_file name pid
  for pid_file in "${PID_DIR}"/*.pid; do
    name="$(basename "${pid_file}" .pid)"
    pid="$(cat "${pid_file}")"
    if kill -0 "${pid}" >/dev/null 2>&1; then
      echo "  ${name}  pid=${pid}  RUNNING"
    else
      echo "  ${name}  pid=${pid}  DEAD (see ${LOG_DIR}/${name}.log)"
    fi
  done
}

usage() {
  cat <<EOF
Usage: $0 {start|stop|status|restart} [node]

Examples:
  $0 start
  $0 start tb-mqtt-transport2
  $0 stop tb-mqtt-transport2
  $0 status

Default local topology (2 of each):
  tb-core1 / tb-core2                      HTTP 8080 / 18080
  tb-rule-engine1 / tb-rule-engine2        HTTP 8082 / 18082
  tb-http-transport1 / tb-http-transport2  HTTP 8081 / 18081
  tb-mqtt-transport1 / tb-mqtt-transport2  HTTP 8083 / 18083  MQTT 1883 / 1884
  tb-tcp-transport1 / tb-tcp-transport2    HTTP 8087 / 18087  TCP 5683 / 5693
  tb-udp-transport1 / tb-udp-transport2    HTTP 8088 / 18088  UDP 5684 / 5694

Requires ZooKeeper + Postgres (and normally Kafka/Redis) already running.
EOF
}

cmd="${1:-}"
node="${2:-}"
case "${cmd}" in
  start)
    if [[ -n "${node}" ]]; then start_named "${node}"; else do_start; fi
    ;;
  stop)
    if [[ -n "${node}" ]]; then
      stop_one "${node}"
      echo "Stopped ${node}."
    else
      do_stop
    fi
    ;;
  status) do_status ;;
  restart)
    if [[ -n "${node}" ]]; then
      stop_one "${node}"
      start_named "${node}"
    else
      do_stop
      do_start
    fi
    ;;
  -h|--help|help) usage ;;
  *) usage; exit 1 ;;
esac
