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
        *) return 2 ;;
    esac
    local project="rivomesh-preceding-receipt-$engine-$$"
    local -a compose=(docker compose -p "$project" -f "$tests/$manifest")
    cleanup() {
        local status=$?
        if (( status != 0 )); then
            "${compose[@]}" logs --no-color --tail 30 "$engine" || true
        fi
        "${compose[@]}" down -v --remove-orphans
    }
    trap cleanup EXIT
    if [[ "$engine" == hive ]]; then
        "${compose[@]}" run --rm --no-deps -T init
    fi
    "${compose[@]}" up -d --wait --wait-timeout 420 "$engine"
    "${compose[@]}" run --rm --no-deps -T tests bash -euc '
        jar=$1
        classes=$(mktemp -d)
        javac -cp "$jar" -d "$classes" tests/analytics/MaterializedRecoveryAcceptance.java
        java -Xmx384m -cp "$jar:$classes" io.github.rivomesh.cdc.MaterializedRecoveryAcceptance "$2"
    ' -- "$artifact" "$(printf '%s' "$engine" | tr '[:lower:]' '[:upper:]')"
)

if (( $# == 0 )); then
    set -- clickhouse doris hive
fi
for engine in "$@"; do
    run_profile "$engine"
done
