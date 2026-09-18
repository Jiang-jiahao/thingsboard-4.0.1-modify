#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}/../services"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/compose-utils.sh"
checkFolders --create "$@"
