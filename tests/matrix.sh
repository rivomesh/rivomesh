#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
project_dir="$(cd "$test_dir/.." && pwd)"
registry="${SYNC_MYSQL_REGISTRY:-mysql}"
profile=legacy
if [[ "${1:-}" == --v3 ]]; then
    profile=v3
    shift
fi
if [[ "$#" -eq 0 ]]; then
    if [[ "$profile" == v3 ]]; then
        set -- 8.0 8.2.0 8.4
    else
        set -- 8.0 8.4
    fi
fi
for version in "$@"; do
    if [[ ! "$version" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]]; then
        printf 'Usage: %s [--v3] [MySQL image version ...]\n' "$0" >&2
        exit 2
    fi
    for suite in integration ha group; do
        printf 'Running %s %s tests against MySQL %s\n' "$profile" "$suite" "$version"
        report_suite="$suite"
        if [[ "$profile" == v3 ]]; then
            case "$suite" in
                integration) script=v3-run.sh; report_suite=v3 ;;
                ha) script=v3-ha-run.sh; report_suite=v3-ha ;;
                group) script=v3-group-run.sh; report_suite=v3-group ;;
            esac
            report_dir="$project_dir/target/matrix-v3/$version/$suite"
        else
            case "$suite" in
                integration) script=run.sh ;;
                ha) script=ha-run.sh ;;
                group) script=group-run.sh ;;
            esac
            report_dir="$project_dir/target/matrix-ha/$version/$suite"
        fi
        SYNC_MYSQL_IMAGE="$registry:$version" "$test_dir/$script"
        mkdir -p "$report_dir"
        cp "$project_dir"/sync-app/target/surefire-reports/"$report_suite"/TEST-*.xml "$report_dir/"
    done
done
