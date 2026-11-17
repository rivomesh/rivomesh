#!/usr/bin/env bash
set -euo pipefail
# Isolated native fixture, run as the image's kingbase user, without sudo/systemd/privileged.
server=/home/kingbase/install/kingbase/bin
data=/home/kingbase/userdata/data
if [ ! -f "$data/SYS_VERSION" ]; then
    secret_file="$(mktemp)"
    trap 'rm -f "$secret_file"' EXIT
    chmod 600 "$secret_file"
    printf '%s\n' "$DB_PASSWORD" > "$secret_file"
    "$server/initdb" -D "$data" -U system --pwfile="$secret_file" -m pg -E UTF8 --locale=C --auth-host=scram-sha-256 --auth-local=trust
    rm -f "$secret_file"
    trap - EXIT
    printf '\nhost all all 0.0.0.0/0 scram-sha-256\n' >> "$data/sys_hba.conf"
fi
exec "$server/kingbase" -D "$data" -p 54321 -c 'listen_addresses=*'
