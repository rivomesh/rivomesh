#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
project="${RIVOMESH_HIVE_REUSE_PROJECT:-rivomesh-hive-native-$$}"
if [[ ! "$project" =~ ^rivomesh-hive-(native|dev)-[0-9]+$ ]]; then exit 2; fi
compose=(docker compose -p "$project" -f "$test_dir/hive-compose.yml")
marker_dir="$test_dir/../target/broker-tests/$project/native-$$"
export RIVOMESH_HIVE_DIR="/project/target/broker-tests/$project/native-$$"
mkdir -p "$marker_dir"
python3 "$test_dir/messaging-prebuilt-check.py"
cleanup(){
    status=$?
    if [ "$status" -ne 0 ]; then "${compose[@]}" logs --no-color --tail 150 > "$marker_dir/containers.log" 2>&1 || true; fi
    "${compose[@]}" down -v --remove-orphans
}
trap cleanup EXIT
# Never start a second embedded Derby writer beside a live server.
if ! "${compose[@]}" ps --status running --services | grep -qx hive; then "${compose[@]}" run --rm --no-deps -T init; fi
"${compose[@]}" up -d --wait --wait-timeout 480 hive
"${compose[@]}" run --rm --no-deps -T tests bash tests/hive-materializer-in-container.sh &
test_pid=$!
while kill -0 "$test_pid" 2>/dev/null; do
    if [ -f "$marker_dir/restart.request" ] && [ ! -f "$marker_dir/restart.done" ]; then
        "${compose[@]}" stop hive
        "${compose[@]}" run --rm --no-deps -T store 'mkdir -p /backup/metadata /backup/warehouse; cp -a /metadata/. /backup/metadata/; cp -a /warehouse/. /backup/warehouse/'
        "${compose[@]}" start --wait --wait-timeout 480 hive
        touch "$marker_dir/restart.done"
    fi
    if [ -f "$marker_dir/restore.request" ] && [ ! -f "$marker_dir/restore.done" ]; then
        "${compose[@]}" stop hive
        # Only this isolated project's named volumes are restored, with the embedded metastore closed.
        "${compose[@]}" run --rm --no-deps -T store 'find /metadata -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +; find /warehouse -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +; cp -a /backup/metadata/. /metadata/; cp -a /backup/warehouse/. /warehouse/'
        "${compose[@]}" start --wait --wait-timeout 480 hive
        touch "$marker_dir/restore.done"
    fi
    sleep 1
done
wait "$test_pid"
python3 "$test_dir/verify-artifact.py"
