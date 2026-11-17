#!/usr/bin/env bash
set -euo pipefail
test -f /var/lib/gbase8c/bootstrap.ready
gsql -U omm -d postgres -p 5432 -Atc "SELECT 'RIVOMESH_GBASE_READY' FROM pg_roles WHERE rolname='rivomesh_writer'" 2>/dev/null | grep -qx RIVOMESH_GBASE_READY
