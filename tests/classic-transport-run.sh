#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
export RIVOMESH_TEST_PROJECT="rivomesh-classic-test-$$"
compose=(docker compose -p "$RIVOMESH_TEST_PROJECT" -f "$test_dir/classic-transport-compose.yml")
marker_dir="$test_dir/../target/broker-tests/$RIVOMESH_TEST_PROJECT"
mkdir -p "$marker_dir"
cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then "${compose[@]}" logs --no-color --tail 200 > "$marker_dir/containers.log" 2>&1 || true; fi
    "${compose[@]}" down -v --remove-orphans
}
trap cleanup EXIT
"${compose[@]}" run --rm -T tests bash tests/classic-transport-in-container.sh &
test_pid=$!
while kill -0 "$test_pid" 2>/dev/null; do
    if [ -f "$marker_dir/restart.request" ] && [ ! -f "$marker_dir/restart.done" ]; then
        "${compose[@]}" stop classic
        "${compose[@]}" run --rm --no-deps -T classic-store -c 'cp -a /store/. /backup/'
        "${compose[@]}" start --wait --wait-timeout 180 classic
        touch "$marker_dir/restart.done"
    fi
    if [ -f "$marker_dir/restore.request" ] && [ ! -f "$marker_dir/restore.done" ]; then
        "${compose[@]}" stop classic
        # Restore only this isolated fixture's named volume, never host/user data.
        "${compose[@]}" run --rm --no-deps -T classic-store -c 'find /store -mindepth 1 -maxdepth 1 -exec rm -rf -- {} + && cp -a /backup/. /store/'
        "${compose[@]}" start --wait --wait-timeout 180 classic
        touch "$marker_dir/restore.done"
    fi
    sleep 1
done
wait "$test_pid"
