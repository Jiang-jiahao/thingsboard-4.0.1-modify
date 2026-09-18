#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}/../services"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/compose-utils.sh"
if checkFolders "$@" ; then
    echo "------"
    echo "All checks have passed"
else
    CHECK_EXIT_CODE=$?
    echo "------"
    echo "Some checks did not pass - check the output"
    exit $CHECK_EXIT_CODE
fi