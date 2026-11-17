#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
project="rivomesh-v3-test-$$"
compose=(docker compose -p "$project" -f "$test_dir/compose.yml" -f "$test_dir/postgres-compose.yml")
if [ "${SYNC_MYSQL57:-0}" = 1 ]; then compose+=(-f "$test_dir/mysql57-compose.yml"); fi
cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then "${compose[@]}" logs --tail 80; fi
    "${compose[@]}" down -v --remove-orphans
}
trap cleanup EXIT
"${compose[@]}" run --rm tests mvn -B -s build/settings.xml -Dsync.integration=true -Dsync.v3.integration=true \
    -DtestSuite=v3 -Dtarget.type=POSTGRESQL -Dtarget.driver=org.postgresql.Driver \
    '-Dtarget.url=jdbc:postgresql://postgres:5432/sync_test?connectTimeout=3&socketTimeout=10' \
    -Dtest=PipelineIntegrationTest,PlatformV3IntegrationTest,MysqlCdcSourceIntegrationTest,PipelineConfigTest,JournalIntegrationTest,JdbcSinkIntegrationTest,JournalTest,NormalizationTest,FanoutDispatcherTest,JdbcSinkTest,JsonValuesTest,MessageSinkTest \
    -Dsurefire.failIfNoSpecifiedTests=false verify
