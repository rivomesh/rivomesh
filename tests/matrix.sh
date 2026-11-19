#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
project_dir="$(cd "$test_dir/.." && pwd)"
registry="${SYNC_MYSQL_REGISTRY:-mysql}"
if [[ "${1:-}" == --v3 ]]; then
    shift
fi
if [[ "$#" -eq 0 ]]; then
    set -- 5.7.44 8.0.46 8.2.0 8.4.13
fi
for version in "$@"; do
    if [[ ! "$version" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]]; then
        printf 'Usage: %s [--v3] [MySQL image version ...]\n' "$0" >&2
        exit 2
    fi
    for suite in integration ha group; do
        printf 'Running V3 %s tests against MySQL %s\n' "$suite" "$version"
        case "$suite" in
            integration) script=v3-run.sh; report_suite=v3 ;;
            ha) script=v3-ha-run.sh; report_suite=v3-ha ;;
            group) script=v3-group-run.sh; report_suite=v3-group ;;
        esac
        report_dir="$project_dir/target/matrix-v3/$version/$suite"
        if [[ "$version" == 5.7* ]]; then
            SYNC_MYSQL57=1 SYNC_MYSQL57_IMAGE="$registry:$version" "$test_dir/$script"
        else
            SYNC_MYSQL_IMAGE="$registry:$version" "$test_dir/$script"
        fi
        mkdir -p "$report_dir"
        cp "$project_dir"/sync-app/target/surefire-reports/"$report_suite"/TEST-*.xml "$report_dir/"
    done
done
