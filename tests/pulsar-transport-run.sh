#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
export RIVOMESH_TEST_PROJECT="rivomesh-pulsar-test-$$"
compose=(docker compose -p "$RIVOMESH_TEST_PROJECT" -f "$test_dir/pulsar-transport-compose.yml")
marker_dir="$test_dir/../target/broker-tests/$RIVOMESH_TEST_PROJECT"
mkdir -p "$marker_dir"
cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then "${compose[@]}" logs --tail 80; fi
    "${compose[@]}" down -v --remove-orphans
}
trap cleanup EXIT
"${compose[@]}" run --rm -T tests bash tests/pulsar-transport-in-container.sh &
test_pid=$!
while kill -0 "$test_pid" 2>/dev/null; do
    if [ -f "$marker_dir/restart.request" ] && [ ! -f "$marker_dir/restart.done" ]; then
        "${compose[@]}" restart pulsar
        touch "$marker_dir/restart.done"
    fi
    sleep 1
done
wait "$test_pid"
