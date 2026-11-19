#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
postgres_metastore=false
if [ "$#" -ne 0 ]; then
    if [ "$#" -ne 2 ] || [ "$1" != "--postgres-metastore" ]; then
        printf '%s\n' "Usage: $0 [--postgres-metastore /path/to/postgresql-jdbc.jar]" >&2
        exit 2
    fi
    postgres_metastore=true
    export COMPOSE_DISABLE_ENV_FILE=1
    unset COMPOSE_ENV_FILES
fi
if "$postgres_metastore"; then
    export RIVOMESH_TEST_PROJECT="rivomesh-hive-postgres-$$"
else
    export RIVOMESH_TEST_PROJECT="rivomesh-hive-pipeline-$$"
fi
compose=(docker compose -p "$RIVOMESH_TEST_PROJECT" -f "$test_dir/hive-compose.yml")
marker_dir="$test_dir/../target/broker-tests/$RIVOMESH_TEST_PROJECT/acceptance"
export RIVOMESH_HIVE_DIR="/project/target/broker-tests/$RIVOMESH_TEST_PROJECT/acceptance"
mkdir -p "$marker_dir"
python3 "$test_dir/messaging-prebuilt-check.py"
private_metastore_config=
cleanup(){
    status=$?
    if [ "$status" -ne 0 ]; then
        "${compose[@]}" logs --no-color --tail 150 > "$marker_dir/containers.log" 2>&1 || true
    fi
    down_status=0
    "${compose[@]}" down -v --remove-orphans || down_status=$?
    if [ -n "$private_metastore_config" ]; then
        rm -f -- "$private_metastore_config/server2.xml" "$private_metastore_config/metastore.xml"
        rmdir -- "$private_metastore_config"
    fi
    if [ "$status" -ne 0 ]; then exit "$status"; fi
    exit "$down_status"
}
trap cleanup EXIT
if "$postgres_metastore"; then
    source "$test_dir/hive/postgres-metastore.sh"
    prepare_postgres_metastore "$2"
    "${compose[@]}" up -d --wait --wait-timeout 180 metastore-db mysql postgres
    "${compose[@]}" run --rm --no-deps -T metastore-init
    "${compose[@]}" up -d --wait --wait-timeout 240 metastore
    "${compose[@]}" exec -T metastore /opt/hive/bin/schematool -dbType postgres -validate
    "${compose[@]}" up -d --wait --wait-timeout 480 hive
else
    "${compose[@]}" run --rm --no-deps -T init
    "${compose[@]}" up -d --wait --wait-timeout 480 hive mysql postgres
fi
"${compose[@]}" run --rm --no-deps -T tests bash tests/hive-pipeline-in-container.sh &
test_pid=$!
while kill -0 "$test_pid" 2>/dev/null; do
    if [ -f "$marker_dir/stop.request" ] && [ ! -f "$marker_dir/stop.done" ]; then
        if "$postgres_metastore"; then
            observe_postgres_catalog before
            "${compose[@]}" stop hive metastore
            "${compose[@]}" stop metastore-db
        else
            "${compose[@]}" stop hive
        fi
        touch "$marker_dir/stop.done"
    fi
    if [ -f "$marker_dir/start.request" ] && [ ! -f "$marker_dir/start.done" ]; then
        if "$postgres_metastore"; then
            "${compose[@]}" start --wait --wait-timeout 180 metastore-db
            "${compose[@]}" start --wait --wait-timeout 240 metastore
            observe_postgres_catalog after
            cmp "$marker_dir/catalog-before.tsv" "$marker_dir/catalog-after.tsv"
            if cmp -s "$marker_dir/catalog-started-before.txt" "$marker_dir/catalog-started-after.txt"; then
                printf '%s\n' "PostgreSQL Metastore catalog did not restart" >&2
                exit 1
            fi
            printf '%s\n' "HIVE_POSTGRES_METASTORE_CATALOG_ACCEPTED: PostgreSQL catalog restarted with its original system, table, resource, owner and warehouse location"
        fi
        "${compose[@]}" start --wait --wait-timeout 480 hive
        touch "$marker_dir/start.done"
    fi
    sleep 1
done
wait "$test_pid"
python3 "$test_dir/verify-artifact.py"
