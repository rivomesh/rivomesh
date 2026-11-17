#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
export RIVOMESH_TEST_PROJECT="${RIVOMESH_DORIS_REUSE_PROJECT:-rivomesh-doris-native-$$}"
if [[ ! "$RIVOMESH_TEST_PROJECT" =~ ^rivomesh-doris-(native|dev)-[0-9]+$ ]]; then exit 2; fi
compose=(docker compose -p "$RIVOMESH_TEST_PROJECT" -f "$test_dir/doris-compose.yml")
marker_dir="$test_dir/../target/broker-tests/$RIVOMESH_TEST_PROJECT/native-$$"
export RIVOMESH_DORIS_DIR="/project/target/broker-tests/$RIVOMESH_TEST_PROJECT/native-$$"
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
"${compose[@]}" up -d --wait --wait-timeout 180 doris
"${compose[@]}" run --rm --no-deps -T tests bash tests/doris-materializer-in-container.sh &
test_pid=$!
while kill -0 "$test_pid" 2>/dev/null; do
    if [ -f "$marker_dir/restart.request" ] && [ ! -f "$marker_dir/restart.done" ]; then
        "${compose[@]}" stop doris
        "${compose[@]}" run --rm --no-deps -T store 'mkdir -p /backup/fe /backup/be; cp -a /fe/. /backup/fe/; cp -a /be/. /backup/be/'
        "${compose[@]}" start --wait --wait-timeout 180 doris
        touch "$marker_dir/restart.done"
    fi
    if [ -f "$marker_dir/restore.request" ] && [ ! -f "$marker_dir/restore.done" ]; then
        "${compose[@]}" stop doris
        # These paths are only this isolated project's named test volumes.
        "${compose[@]}" run --rm --no-deps -T store 'find /fe -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +; find /be -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +; cp -a /backup/fe/. /fe/; cp -a /backup/be/. /be/'
        "${compose[@]}" start --wait --wait-timeout 180 doris
        touch "$marker_dir/restore.done"
    fi
    sleep 1
done
wait "$test_pid"
python3 "$test_dir/verify-artifact.py"
