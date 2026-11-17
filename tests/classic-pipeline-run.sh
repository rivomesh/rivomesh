#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
export RIVOMESH_TEST_PROJECT="rivomesh-classic-pipeline-$$"
compose=(docker compose -p "$RIVOMESH_TEST_PROJECT" -f "$test_dir/classic-transport-compose.yml" -f "$test_dir/classic-pipeline-compose.yml")
marker_dir="$test_dir/../target/broker-tests/$RIVOMESH_TEST_PROJECT"
mkdir -p "$marker_dir"
if [ "${RIVOMESH_TEST_PREBUILT:-0}" = "1" ]; then python3 "$test_dir/messaging-prebuilt-check.py"; fi
cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then
        "${compose[@]}" logs --no-color --tail 100 > "$marker_dir/containers.log" 2>&1 || true
    fi
    "${compose[@]}" down -v --remove-orphans
}
trap cleanup EXIT
"${compose[@]}" run --rm -T tests bash tests/classic-pipeline-in-container.sh &
test_pid=$!
while kill -0 "$test_pid" 2>/dev/null; do
    if [ -f "$marker_dir/stop.request" ] && [ ! -f "$marker_dir/stop.done" ]; then
        "${compose[@]}" stop classic
        touch "$marker_dir/stop.done"
    fi
    if [ -f "$marker_dir/start.request" ] && [ ! -f "$marker_dir/start.done" ]; then
        "${compose[@]}" start --wait --wait-timeout 180 classic
        touch "$marker_dir/start.done"
    fi
    sleep 1
done
wait "$test_pid"
python3 "$test_dir/verify-artifact.py"
