#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
export RIVOMESH_TEST_PROJECT="rivomesh-rocket-test-$$"
compose=(docker compose -p "$RIVOMESH_TEST_PROJECT" -f "$test_dir/rocketmq-transport-compose.yml")
marker_dir="$test_dir/../target/broker-tests/$RIVOMESH_TEST_PROJECT"
mkdir -p "$marker_dir"
cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then
        "${compose[@]}" logs --tail 80
        "${compose[@]}" cp rocketbroker:/home/rocketmq/logs/rocketmqlogs "$marker_dir/broker-logs" || true
    fi
    "${compose[@]}" down -v --remove-orphans
}
trap cleanup EXIT
"${compose[@]}" run --rm -T tests bash tests/rocketmq-transport-in-container.sh &
test_pid=$!
while kill -0 "$test_pid" 2>/dev/null; do
    if [ -f "$marker_dir/restart.request" ] && [ ! -f "$marker_dir/restart.done" ]; then
        "${compose[@]}" stop rocketbroker
        # Snapshot only this stopped fixture's volume, retaining an older nonempty topic.
        "${compose[@]}" run --rm --no-deps -T rocket-store -c 'cp -a /store/. /backup/'
        "${compose[@]}" start --wait --wait-timeout 180 rocketbroker
        touch "$marker_dir/restart.done"
    fi
    if [ -f "$marker_dir/restore.request" ] && [ ! -f "$marker_dir/restore.done" ]; then
        "${compose[@]}" stop rocketbroker
        # Both paths are isolated named volumes declared in this test's Compose project.
        "${compose[@]}" run --rm --no-deps -T rocket-store -c 'find /store -mindepth 1 -maxdepth 1 -exec rm -rf -- {} + && cp -a /backup/. /store/'
        "${compose[@]}" start --wait --wait-timeout 180 rocketbroker
        touch "$marker_dir/restore.done"
    fi
    sleep 1
done
wait "$test_pid"
