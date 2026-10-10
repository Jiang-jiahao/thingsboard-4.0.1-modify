#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}/../services"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/compose-utils.sh"

# 起栈前先把 log/ 与 data/ 建好、并改成容器内用户的属主（uid 999）。
# 少了这步，Docker 会用 root 把目录建出来，以非 root 运行的 JVM 写不了 GC 日志、
# 直接 "Could not create the Java Virtual Machine" 退出 —— 表现是 core 起不来、网关 502。
# macOS 的 Docker Desktop 文件共享层不按 uid 校验，跳过。
if [ "$(uname -s)" = "Linux" ]; then
    checkFolders --create || exit $?
fi

COMPOSE_VERSION=$(composeVersion) || exit $?

ADDITIONAL_COMPOSE_QUEUE_ARGS=$(additionalComposeQueueArgs) || exit $?

ADDITIONAL_COMPOSE_ARGS=$(additionalComposeArgs) || exit $?

ADDITIONAL_CACHE_ARGS=$(additionalComposeCacheArgs) || exit $?

ADDITIONAL_COMPOSE_MONITORING_ARGS=$(additionalComposeMonitoringArgs) || exit $?

ADDITIONAL_COMPOSE_EDQS_ARGS=$(additionalComposeEdqsArgs) || exit $?

COMPOSE_ARGS="\
      -f docker-compose.yml ${ADDITIONAL_CACHE_ARGS} ${ADDITIONAL_COMPOSE_ARGS} ${ADDITIONAL_COMPOSE_QUEUE_ARGS} ${ADDITIONAL_COMPOSE_MONITORING_ARGS} ${ADDITIONAL_COMPOSE_EDQS_ARGS} \
      up -d"

case $COMPOSE_VERSION in
    V2)
        docker compose $COMPOSE_ARGS
    ;;
    V1)
        docker-compose --compatibility $COMPOSE_ARGS
    ;;
    *)
        # unknown option
    ;;
esac
