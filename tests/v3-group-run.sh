#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
project="rivomesh-v3-group-$$"
compose=(docker compose -p "$project" -f "$test_dir/compose.yml" -f "$test_dir/postgres-compose.yml" -f "$test_dir/group-compose.yml")
if [ "${SYNC_MYSQL57:-0}" = 1 ]; then compose+=(-f "$test_dir/mysql57-compose.yml" -f "$test_dir/mysql57-group-compose.yml"); fi
cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then "${compose[@]}" logs --tail 80; fi
    "${compose[@]}" down -v --remove-orphans
}
if [ "${RIVOMESH_TEST_PREBUILT:-0}" = 1 ]; then
    python3 "$test_dir/messaging-prebuilt-check.py"
fi
trap cleanup EXIT
run_flags=(--rm)
profile_flags=(
    -Dsync.v3.group=true
    '-Dtarget.url=jdbc:postgresql://postgres:5432/sync_test?connectTimeout=3&socketTimeout=10'
)
if [ "${SYNC_V3_GROUP_SQL_TARGETS:-0}" = 1 ]; then
    "${compose[@]}" up --wait --wait-timeout 240 mysql mysql-2 mysql-3 mysql-target postgres
    run_flags+=(--no-deps)
    profile_flags+=(-Dsync.v3.group.sqlTargets=true)
fi
if [ "${RIVOMESH_TEST_PREBUILT:-0}" = 1 ]; then
    artifact="${RIVOMESH_TEST_ARTIFACT:-sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar}"
    classpath="$artifact:sync-app/target/test-classes:target/tools/junit-platform-console-standalone-1.11.0.jar"
    "${compose[@]}" run "${run_flags[@]}" tests java -Xmx512m "${profile_flags[@]}" -cp "$classpath" \
        org.junit.platform.console.ConsoleLauncher execute --select-class io.github.rivomesh.PipelineGroupIntegrationTest \
        --fail-if-no-tests --disable-ansi-colors
else
    "${compose[@]}" run "${run_flags[@]}" tests mvn -B -s build/settings.xml "${profile_flags[@]}" -DtestSuite=v3-group \
        -Dtest=PipelineGroupIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false verify
fi
