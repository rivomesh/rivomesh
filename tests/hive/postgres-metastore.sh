#!/usr/bin/env bash
# Optional PostgreSQL Metastore setup for the disposable Hive pipeline fixture.

prepare_postgres_metastore(){
    private_metastore_config=$(mktemp -d "$marker_dir/metastore-private.XXXXXX")
    export RIVOMESH_HIVE_META_CONF="$private_metastore_config/server2.xml"
    export RIVOMESH_HIVE_META_SERVER_CONF="$private_metastore_config/metastore.xml"
    export RIVOMESH_HIVE_META_JDBC
    RIVOMESH_HIVE_META_JDBC=$(python3 - "$1" <<'PY'
from pathlib import Path
import sys
import zipfile

driver = Path(sys.argv[1]).resolve(strict=True)
with zipfile.ZipFile(driver) as jar:
    jar.getinfo("org/postgresql/Driver.class")
print(driver)
PY
)
    export RIVOMESH_HIVE_META_PASSWORD
    RIVOMESH_HIVE_META_PASSWORD=$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')
    python3 - "$test_dir/hive/hive-site.xml" <<'PY'
import os
import sys
from xml.etree import ElementTree as ET


def write_hive_config(path, additions):
    config = ET.parse(sys.argv[1])
    root = config.getroot()
    for child in list(root):
        name = child.findtext("name", "")
        if name.startswith("javax.jdo.option.Connection") or name in additions:
            root.remove(child)
    for name, value in additions.items():
        prop = ET.SubElement(root, "property")
        ET.SubElement(prop, "name").text = name
        ET.SubElement(prop, "value").text = value
    config.write(path, encoding="utf-8", xml_declaration=True)
    os.chmod(path, 0o644)  # The private host directory is accessed by the container through file mounts.


write_hive_config(os.environ["RIVOMESH_HIVE_META_CONF"], {
    "hive.metastore.uris": "thrift://metastore:9083",
    "hive.compactor.initiator.on": "false",
    "hive.compactor.worker.threads": "0",
})
write_hive_config(os.environ["RIVOMESH_HIVE_META_SERVER_CONF"], {
    "javax.jdo.option.ConnectionURL": "jdbc:postgresql://metastore-db:5432/hive_metastore",
    "javax.jdo.option.ConnectionDriverName": "org.postgresql.Driver",
    "javax.jdo.option.ConnectionUserName": "hive_catalog",
    "javax.jdo.option.ConnectionPassword": os.environ["RIVOMESH_HIVE_META_PASSWORD"],
    "hive.metastore.schema.verification": "true",
})
PY
    compose+=(-f "$test_dir/hive-postgres-metastore-compose.yml")
}

observe_postgres_catalog(){
    local phase="$1"
    "${compose[@]}" exec -T metastore-db psql -X -qAt -v ON_ERROR_STOP=1 -U hive_catalog -d hive_metastore \
        -c 'SELECT pg_postmaster_start_time()' > "$marker_dir/catalog-started-$phase.txt"
    test -s "$marker_dir/catalog-started-$phase.txt"
    "${compose[@]}" exec -T metastore-db psql -X -qAt -F '|' -v ON_ERROR_STOP=1 -U hive_catalog -d hive_metastore \
        > "$marker_dir/catalog-$phase.tsv" <<'SQL'
SELECT c.system_identifier, t."TBL_ID", r."PARAM_VALUE", o."PARAM_VALUE", s."LOCATION"
FROM "TBLS" t
JOIN "DBS" d ON d."DB_ID" = t."DB_ID"
JOIN "SDS" s ON s."SD_ID" = t."SD_ID"
JOIN "TABLE_PARAMS" r ON r."TBL_ID" = t."TBL_ID" AND r."PARAM_KEY" = 'rivomesh.resource'
JOIN "TABLE_PARAMS" o ON o."TBL_ID" = t."TBL_ID" AND o."PARAM_KEY" = 'rivomesh.owner'
CROSS JOIN pg_control_system() c
WHERE t."TBL_NAME" ~ '^rm3_hive_[a-f0-9]{48}$'
AND d."NAME" ~ '^hive_[a-f0-9]{20}_meta$';
SQL
    test "$(wc -l < "$marker_dir/catalog-$phase.tsv")" -eq 1
    grep -Eq '^[0-9]+\|[0-9]+\|[a-f0-9]{32}\|rivomesh:[a-f0-9]{64}:[a-f0-9]{64}\|file:' "$marker_dir/catalog-$phase.tsv"
}
