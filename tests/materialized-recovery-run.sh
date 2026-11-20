#!/usr/bin/env bash
set -euo pipefail

tests="$(cd "$(dirname "$0")" && pwd)"
artifact=sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar
python3 "$tests/verify-artifact.py" "$tests/../$artifact"

run_profile() (
    set -euo pipefail
    local engine=$1 manifest
    case "$engine" in
        clickhouse) manifest=clickhouse-protocol-compose.yml ;;
        doris|hive) manifest="$engine-compose.yml" ;;
        artemis|classic|rocketmq|pulsar) manifest="$engine-transport-compose.yml" ;;
        redis|nats) manifest=stream-receipt-compose.yml ;;
        rabbit) manifest=rabbit-receipt-compose.yml ;;
        *) return 2 ;;
    esac
    local project="rivomesh-preceding-receipt-$engine-$$" local_override=
    local -a compose=(docker compose -p "$project" -f "$tests/$manifest")
    # This test reads native records after a client reopen. Optional temporary storage does not attest a broker restart.
    local service=$engine data_path=
    case "$engine" in
        artemis) data_path=/var/lib/artemis-instance/data ;;
        rocketmq) service=rocketbroker; data_path=/home/rocketmq/store ;;
    esac
    if [[ -n "$data_path" && "${RIVOMESH_RECEIPT_TEST_TMPFS:-0}" == 1 ]]; then
        local_override=$(mktemp)
        cat > "$local_override" <<YAML
services:
  $service:
    volumes:
      - type: tmpfs
        target: $data_path
        tmpfs:
          size: 268435456
          mode: 0777
YAML
        compose+=(-f "$local_override")
    fi
    cleanup() {
        local status=$?
        if (( status != 0 )); then
            "${compose[@]}" logs --no-color --tail 30 "$service" || true
        fi
        "${compose[@]}" down -v --remove-orphans
        if [[ -n "$local_override" ]]; then rm -f -- "$local_override"; fi
        return "$status"
    }
    trap cleanup EXIT
    if [[ "$engine" == hive ]]; then
        "${compose[@]}" run --rm --no-deps -T init
    fi
    "${compose[@]}" up -d --wait --wait-timeout 420 "$service"
    local product
    case "$engine" in
        artemis) product=ACTIVEMQ_ARTEMIS ;;
        classic) product=ACTIVEMQ_CLASSIC ;;
        redis) product=REDIS_STREAMS ;;
        rabbit) product=RABBITMQ ;;
        nats) product=NATS_JETSTREAM ;;
        *) product="$(printf '%s' "$engine" | tr '[:lower:]' '[:upper:]')" ;;
    esac
    "${compose[@]}" run --rm --no-deps -T tests bash -euc '
        jar=$1
        classes=$(mktemp -d)
        javac -cp "$jar" -d "$classes" tests/analytics/MaterializedRecoveryAcceptance.java
        timeout 180s java -Xmx384m -cp "$jar:$classes" io.github.rivomesh.cdc.MaterializedRecoveryAcceptance "$2"
    ' -- "$artifact" "$product"
)

if (( $# == 0 )); then
    set -- clickhouse doris hive
fi
for engine in "$@"; do
    run_profile "$engine"
done
