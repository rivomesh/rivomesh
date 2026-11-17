#!/usr/bin/env bash
set -euo pipefail
output="$(printf "SET HEADING OFF;\nSELECT 'RIVOMESH_DM_READY' AS READY;\nEXIT;\n" | /opt/dmdbms/bin/disql -S "SYSDBA/\"${SYSDBA_PWD}\"@127.0.0.1:5236")"
printf '%s\n' "$output" | grep -Eq '^[[:space:]]*1[[:space:]]+RIVOMESH_DM_READY[[:space:]]*$'
