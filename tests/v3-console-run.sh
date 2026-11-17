#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
project="rivomesh-v3-console-$$"
compose=(docker compose -p "$project" -f "$test_dir/compose.yml" -f "$test_dir/postgres-compose.yml")
cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then "${compose[@]}" logs --tail 50; fi
    "${compose[@]}" down -v --remove-orphans
}
trap cleanup EXIT
"${compose[@]}" run --rm tests mvn -B -s build/settings.xml -Dsync.v3.integration=true -DtestSuite=v3-console \
    '-Dtarget.url=jdbc:postgresql://postgres:5432/sync_test?connectTimeout=3&socketTimeout=10' \
    -Dtest=PlatformV3IntegrationTest -Dsurefire.failIfNoSpecifiedTests=false verify
