#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
export RIVOMESH_TEST_PROJECT="rivomesh-message-test-$$"
compose=(docker compose -p "$RIVOMESH_TEST_PROJECT" -f "$test_dir/messaging-compose.yml")
marker_dir="$test_dir/../target/broker-tests/$RIVOMESH_TEST_PROJECT"
mkdir -p "$marker_dir"
cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then
        "${compose[@]}" logs --no-color --tail 200 > "$marker_dir/containers.log" 2>&1 || true
        # Keep the original server exception, rather than only a client's HTTP 500.
        "${compose[@]}" logs --no-color pulsar > "$marker_dir/pulsar.log" 2>&1 || true
        tail -n 60 "$marker_dir/containers.log"
    fi
    "${compose[@]}" down -v --remove-orphans
}
trap cleanup EXIT
test_script=tests/messaging-in-container.sh
if [ "${RIVOMESH_TEST_PREBUILT:-0}" = 1 ]; then
    python3 "$test_dir/messaging-prebuilt-check.py"
    test_script=tests/messaging-prebuilt-in-container.sh
fi
"${compose[@]}" run --rm -T tests bash "$test_script" &
test_pid=$!
restart_round=1
while kill -0 "$test_pid" 2>/dev/null; do
    if [ -f "$marker_dir/restart.$restart_round.request" ] && [ ! -f "$marker_dir/restart.$restart_round.done" ]; then
        "${compose[@]}" stop rabbit nats kafka redis pulsar rocketbroker artemis
        touch "$marker_dir/restart.$restart_round.stopped"
        while [ ! -f "$marker_dir/restart.$restart_round.continue" ] && kill -0 "$test_pid" 2>/dev/null; do sleep 1; done
        if ! kill -0 "$test_pid" 2>/dev/null; then break; fi
        # Record startup readiness separately from the subsequent 60s catch-up assertion.
        "${compose[@]}" start --wait --wait-timeout 180 rabbit nats kafka redis pulsar rocketbroker artemis
        touch "$marker_dir/restart.$restart_round.done"
        restart_round=$((restart_round + 1))
    fi
    sleep 1
done
wait "$test_pid"
