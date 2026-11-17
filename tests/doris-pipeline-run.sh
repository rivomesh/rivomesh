#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
export RIVOMESH_TEST_PROJECT="rivomesh-doris-pipeline-$$"
compose=(docker compose -p "$RIVOMESH_TEST_PROJECT" -f "$test_dir/doris-compose.yml")
marker_dir="$test_dir/../target/broker-tests/$RIVOMESH_TEST_PROJECT/acceptance"
export RIVOMESH_DORIS_DIR="/project/target/broker-tests/$RIVOMESH_TEST_PROJECT/acceptance"
mkdir -p "$marker_dir"
python3 "$test_dir/messaging-prebuilt-check.py"
cleanup(){
    status=$?
    if [ "$status" -ne 0 ]; then
        "${compose[@]}" logs --no-color --tail 150 > "$marker_dir/containers.log" 2>&1 || true
        "${compose[@]}" exec -T doris tail -n 500 /opt/apache-doris/be/log/be.out > "$marker_dir/be.log" 2>&1 || true
    fi
    "${compose[@]}" down -v --remove-orphans
}
trap cleanup EXIT
"${compose[@]}" up -d --wait --wait-timeout 180 doris mysql postgres
"${compose[@]}" run --rm --no-deps -T tests bash tests/doris-pipeline-in-container.sh &
test_pid=$!
while kill -0 "$test_pid" 2>/dev/null; do
    if [ -f "$marker_dir/stop.request" ] && [ ! -f "$marker_dir/stop.done" ]; then
        "${compose[@]}" stop doris
        touch "$marker_dir/stop.done"
    fi
    if [ -f "$marker_dir/start.request" ] && [ ! -f "$marker_dir/start.done" ]; then
        "${compose[@]}" start --wait --wait-timeout 180 doris
        touch "$marker_dir/start.done"
    fi
    sleep 1
done
wait "$test_pid"
python3 "$test_dir/verify-artifact.py"
