#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
export RIVOMESH_TEST_PROJECT="rivomesh-artemis-test-$$"
compose=(docker compose -p "$RIVOMESH_TEST_PROJECT" -f "$test_dir/artemis-transport-compose.yml")
marker_dir="$test_dir/../target/broker-tests/$RIVOMESH_TEST_PROJECT"
mkdir -p "$marker_dir"
cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then "${compose[@]}" logs --tail 100; fi
    "${compose[@]}" down -v --remove-orphans
}
trap cleanup EXIT
"${compose[@]}" run --rm -T tests bash tests/artemis-transport-in-container.sh &
test_pid=$!
while kill -0 "$test_pid" 2>/dev/null; do
    if [ -f "$marker_dir/restart.request" ] && [ ! -f "$marker_dir/restart.done" ]; then
        "${compose[@]}" stop artemis
        "${compose[@]}" run --rm --no-deps -T artemis-store -c 'cp -a /store/. /backup/'
        "${compose[@]}" start --wait --wait-timeout 180 artemis
        touch "$marker_dir/restart.done"
    fi
    if [ -f "$marker_dir/restore.request" ] && [ ! -f "$marker_dir/restore.done" ]; then
        "${compose[@]}" stop artemis
        # Restore only this isolated fixture's named volume, never host/user data.
        "${compose[@]}" run --rm --no-deps -T artemis-store -c 'find /store -mindepth 1 -maxdepth 1 -exec rm -rf -- {} + && cp -a /backup/. /store/'
        "${compose[@]}" start --wait --wait-timeout 180 artemis
        touch "$marker_dir/restore.done"
    fi
    sleep 1
done
wait "$test_pid"
