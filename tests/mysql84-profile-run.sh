#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
project="rivomesh-mysql84-profile-$$"
compose=(docker compose -p "$project" -f "$test_dir/compose.yml")
cleanup() {
    status=$?
    if [[ "$status" -ne 0 ]]; then "${compose[@]}" logs --tail 60 mysql; fi
    "${compose[@]}" down -v --remove-orphans
}
trap cleanup EXIT
"${compose[@]}" up --wait --wait-timeout 240 mysql
"${compose[@]}" run --rm --no-deps tests mvn -B -s build/settings.xml -Dsync.mysql84.profile=true \
    -DtestSuite=mysql84-profile -Dtest=Mysql84ProfileTest -Dsurefire.failIfNoSpecifiedTests=false verify
