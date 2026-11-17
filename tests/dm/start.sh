#!/usr/bin/env bash
set -euo pipefail
# Vendor initialization runs inside this isolated container; no privileged mode or host changes.
/opt/startup.sh &
launcher=$!
shutdown(){
    trap - TERM INT
    /opt/dmdbms/bin/DmService stop
    kill -KILL "$launcher" 2>/dev/null || true
    wait "$launcher" 2>/dev/null || true
    exit 0
}
trap shutdown TERM INT
wait "$launcher"
