#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
export RIVOMESH_TEST_PROJECT="rivomesh-clickhouse-pipeline-$$"
compose=(docker compose -p "$RIVOMESH_TEST_PROJECT" -f "$test_dir/clickhouse-protocol-compose.yml" -f "$test_dir/clickhouse-pipeline-compose.yml")
marker_dir="$test_dir/../target/broker-tests/$RIVOMESH_TEST_PROJECT"
mkdir -p "$marker_dir"
cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then
        "${compose[@]}" logs --no-color --tail 100 > "$marker_dir/containers.log" 2>&1 || true
        "${compose[@]}" cp clickhouse:/var/log/clickhouse-server "$marker_dir/server-logs" || true
    fi
    "${compose[@]}" down -v --remove-orphans
}
trap cleanup EXIT
"${compose[@]}" run --rm -T tests bash tests/clickhouse-pipeline-in-container.sh &
test_pid=$!
while kill -0 "$test_pid" 2>/dev/null; do
    if [ -f "$marker_dir/stop.request" ] && [ ! -f "$marker_dir/stop.done" ]; then
        "${compose[@]}" stop clickhouse
        touch "$marker_dir/stop.done"
    fi
    if [ -f "$marker_dir/start.request" ] && [ ! -f "$marker_dir/start.done" ]; then
        "${compose[@]}" start --wait --wait-timeout 180 clickhouse
        touch "$marker_dir/start.done"
    fi
    sleep 1
done
wait "$test_pid"
