#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
project="rivomesh-validation-barrier-$$"
compose=(docker compose -p "$project" -f "$test_dir/validation-barrier-compose.yml")
private_dir="$test_dir/../target/broker-tests/$project"
export RIVOMESH_VALIDATION_DIR="/project/target/broker-tests/$project"
mkdir -p "$private_dir"
python3 "$test_dir/messaging-prebuilt-check.py"
cleanup(){
    status=$?
    if [ "$status" -ne 0 ]; then "${compose[@]}" logs --no-color --tail 100 > "$private_dir/containers.log" 2>&1 || true; fi
    "${compose[@]}" down -v --remove-orphans
}
trap cleanup EXIT
services=(mysql mysql-target postgres)
case "${RIVOMESH_VALIDATION_JOURNAL:-LOCAL}" in
    MYSQL) services+=(mysql-journal);;
    GAUSS) services+=(gaussdb);;
esac
"${compose[@]}" up -d --wait --wait-timeout 240 "${services[@]}"
"${compose[@]}" run --rm --no-deps -T tests bash tests/validation-barrier-in-container.sh &
test_pid=$!
while kill -0 "$test_pid" 2>/dev/null; do
    if [ -f "$private_dir/stop.request" ] && [ ! -f "$private_dir/stop.done" ]; then
        "${compose[@]}" stop mysql-target
        touch "$private_dir/stop.done"
    fi
    if [ -f "$private_dir/start.request" ] && [ ! -f "$private_dir/start.done" ]; then
        "${compose[@]}" start --wait --wait-timeout 120 mysql-target
        touch "$private_dir/start.done"
    fi
    if [ -f "$private_dir/stop-source.request" ] && [ ! -f "$private_dir/stop-source.done" ]; then
        "${compose[@]}" stop mysql
        touch "$private_dir/stop-source.done"
    fi
    if [ -f "$private_dir/start-source.request" ] && [ ! -f "$private_dir/start-source.done" ]; then
        "${compose[@]}" start --wait --wait-timeout 120 mysql
        touch "$private_dir/start-source.done"
    fi
    if [ -f "$private_dir/stop-repair-source.request" ] && [ ! -f "$private_dir/stop-repair-source.done" ]; then
        "${compose[@]}" stop mysql
        touch "$private_dir/stop-repair-source.done"
    fi
    if [ -f "$private_dir/start-repair-source.request" ] && [ ! -f "$private_dir/start-repair-source.done" ]; then
        "${compose[@]}" start --wait --wait-timeout 120 mysql
        touch "$private_dir/start-repair-source.done"
    fi
    if [ -f "$private_dir/stop-managed-repair-source.request" ] && [ ! -f "$private_dir/stop-managed-repair-source.done" ]; then
        "${compose[@]}" stop mysql
        touch "$private_dir/stop-managed-repair-source.done"
    fi
    if [ -f "$private_dir/start-managed-repair-source.request" ] && [ ! -f "$private_dir/start-managed-repair-source.done" ]; then
        "${compose[@]}" start --wait --wait-timeout 120 mysql
        touch "$private_dir/start-managed-repair-source.done"
    fi
    sleep 1
done
wait "$test_pid"
python3 "$test_dir/verify-artifact.py" "${RIVOMESH_TEST_ARTIFACT:-$test_dir/../sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar}"
