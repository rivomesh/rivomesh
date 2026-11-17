#!/usr/bin/env bash
set -euo pipefail
export GAUSSHOME=/opt/gbase8c
export GBASEHOME=/opt/gbase8c
export PATH="$GAUSSHOME/bin:$PATH"
export LD_LIBRARY_PATH="$GAUSSHOME/lib:$GAUSSHOME/lib/3rd${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export GAUSSLOG=/var/lib/gbase8c/log
mkdir -p "$GAUSSLOG"
data=/var/lib/gbase8c/data
mkdir -p "$data"
if [ ! -f "$data/PG_VERSION" ]; then
    secret_file="$(mktemp)"
    trap 'rm -f "$secret_file"' EXIT
    chmod 600 "$secret_file"
    printf '%s\n' "$DB_PASSWORD" > "$secret_file"
    gs_initdb -D "$data" --nodename=rivomesh --dbcompatibility=PG -E UTF8 --locale=C -U omm --pwfile="$secret_file" --auth-host=sha256 --auth-local=trust
    rm -f "$secret_file"
    trap - EXIT
    printf '\nhost all all 0.0.0.0/0 sha256\n' >> "$data/pg_hba.conf"
    cat >> "$data/postgresql.conf" <<'CONF'
listen_addresses = '*'
port = 5432
shared_buffers = 128MB
max_connections = 80
unix_socket_directory = '/tmp'
log_min_messages = warning
CONF
fi
# The initialization owner is intentionally local-only. Create a distinct fixture
# login rather than weakening GBase's remote-initial-user restriction.
if [[ ! "$DB_PASSWORD" =~ ^[A-Za-z0-9@_-]{12,80}$ ]]; then exit 2; fi
rm -f /var/lib/gbase8c/bootstrap.ready
gbased -D "$data" -M primary &
server_pid=$!
shutdown(){ kill -INT "$server_pid" 2>/dev/null || true; wait "$server_pid" || true; exit 0; }
trap shutdown INT TERM
ready=false
for attempt in $(seq 1 180); do
    if gsql -U omm -d postgres -p 5432 -Atc "SELECT 'RIVOMESH_LOCAL_READY'" 2>/dev/null | grep -qx RIVOMESH_LOCAL_READY; then ready=true; break; fi
    kill -0 "$server_pid" 2>/dev/null || { wait "$server_pid"; exit 1; }
    sleep 1
done
[ "$ready" = true ] || exit 1
if ! gsql -U omm -d postgres -p 5432 -Atc "SELECT rolname FROM pg_roles WHERE rolname='rivomesh_writer'" | grep -qx rivomesh_writer; then
    gsql -U omm -d postgres -p 5432 -v ON_ERROR_STOP=1 -c "CREATE USER rivomesh_writer WITH LOGIN CREATEDB PASSWORD '$DB_PASSWORD'"
fi
gsql -U omm -d postgres -p 5432 -v ON_ERROR_STOP=1 -c "GRANT CREATE,CONNECT ON DATABASE postgres TO rivomesh_writer"
touch /var/lib/gbase8c/bootstrap.ready
wait "$server_pid"
