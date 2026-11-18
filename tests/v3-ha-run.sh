#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
project="rivomesh-v3-ha-$$"
compose=(docker compose -p "$project" -f "$test_dir/compose.yml" -f "$test_dir/postgres-compose.yml" -f "$test_dir/ha-compose.yml")
if [ "${SYNC_MYSQL57:-0}" = 1 ]; then compose+=(-f "$test_dir/mysql57-compose.yml" -f "$test_dir/mysql57-ha-compose.yml"); fi
cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then "${compose[@]}" logs --tail 80; fi
    "${compose[@]}" down -v --remove-orphans
}
trap cleanup EXIT
run_flags=(--rm)
profile_flags=()
if [ "${SYNC_V3_HA_POSTGRES_ONLY:-0}" = 1 ]; then
    "${compose[@]}" up --wait --wait-timeout 240 mysql mysql-replica postgres
    run_flags+=(--no-deps)
    profile_flags+=(-Dsync.v3.ha.postgresOnly=true)
fi
"${compose[@]}" run "${run_flags[@]}" tests mvn -B -s build/settings.xml -Dsync.v3.ha=true "${profile_flags[@]}" -DtestSuite=v3-ha \
    '-Dtarget.url=jdbc:postgresql://postgres:5432/sync_test?connectTimeout=3&socketTimeout=10' \
    -Dtest=PipelineClusterIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false verify
