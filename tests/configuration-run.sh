#!/usr/bin/env bash
set -euo pipefail
test_dir="$(cd "$(dirname "$0")" && pwd)"
export RIVOMESH_TEST_PROJECT="rivomesh-configuration-$$"
compose=(docker compose -p "$RIVOMESH_TEST_PROJECT" -f "$test_dir/configuration-compose.yml")
mkdir -p "$test_dir/../target/broker-tests/$RIVOMESH_TEST_PROJECT"
if [ "${RIVOMESH_TEST_PREBUILT:-0}" = "1" ]; then python3 "$test_dir/messaging-prebuilt-check.py"; fi
cleanup(){
  status=$?
  if [ "$status" -ne 0 ]; then "${compose[@]}" logs --no-color --tail 150 > "$test_dir/../target/broker-tests/$RIVOMESH_TEST_PROJECT/containers.log" 2>&1 || true; fi
  "${compose[@]}" down -v --remove-orphans
}
trap cleanup EXIT
"${compose[@]}" run --rm -T tests bash tests/configuration-in-container.sh
for audit_page in "$test_dir/../target/broker-tests/$RIVOMESH_TEST_PROJECT/acceptance"/audit-page-*.json; do
  python3 "$test_dir/../tools/audit-export.py" --verify-page "$audit_page"
done
python3 "$test_dir/verify-artifact.py"
