#!/usr/bin/env python3
"""Check the distributable, not just Maven's intermediate class directories."""
import io
import hashlib
import json
import os
import re
import struct
import sys
import zipfile
from pathlib import Path

root = Path(__file__).resolve().parents[1]
artifact = Path(sys.argv[1]) if len(sys.argv) > 1 else root / "sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar"
snapshot_sources = None
if os.environ.get('RIVOMESH_RELEASE_MANIFEST'):
    manifest_path=(root/os.environ['RIVOMESH_RELEASE_MANIFEST']).resolve();manifest_path.relative_to(root)
    manifest=json.loads(manifest_path.read_text());assert artifact.resolve()==(root/manifest['artifact']).resolve(), 'Frozen artifact path differs'
    assert hashlib.sha256(artifact.read_bytes()).hexdigest()==manifest['artifactSha256'], 'Frozen artifact hash differs'
    source_path=(root/manifest['sourceManifest']).resolve();source_path.relative_to(root)
    assert hashlib.sha256(source_path.read_bytes()).hexdigest()==manifest['sourceManifestSha256'], 'Frozen source manifest differs'
    snapshot_sources=json.loads(source_path.read_text())
with zipfile.ZipFile(artifact) as jar:
    names = jar.namelist()
    namespace = 'io/github/rivomesh/'
    legacy_namespace = 'io/' + 'rivomesh/'
    assert len(names) == len(set(names)), "Duplicate ZIP entries"
    assert not any("/cscec/" in name for name in names), "Old application package present"
    assert not any(name.startswith(legacy_namespace) for name in names), "Previous application namespace present"
    assert not any(name.startswith(namespace) and "/internal/" not in name
                   and re.search(r"(?:Test|Acceptance)(?:\$[^/]*)?\.class$", name)
                   for name in names), "Test class included in release"
    resources = ["index.html", "app.js", "app.css", "configuration-editor.js", "audit.js", "accounts.js", "validation.js"]
    resources.append('recovery.js')
    for resource in resources:
        if snapshot_sources is None:
            assert jar.read("console/" + resource) == (root / "sync-app/src/main/resources/console" / resource).read_bytes(), "Stale console asset: " + resource
        else:
            expected=snapshot_sources.get('sync-app/src/main/resources/console/'+resource)
            assert expected and hashlib.sha256(jar.read('console/'+resource)).hexdigest()==expected, 'Frozen console asset differs: '+resource
    for name in names:
        if name.startswith(namespace) and "/internal/" not in name and name.endswith(".class"):
            assert struct.unpack(">H", jar.read(name)[6:8])[0] <= 52, "Application requires newer Java: " + name
    for name in (
        "io/github/rivomesh/Main.class",
        "io/github/rivomesh/cdc/RowValues.class",
        "io/github/rivomesh/validation/ValidationStore.class",
        "io/github/rivomesh/PipelineReconciliation.class",
        "io/github/rivomesh/PipelineRepair.class",
        "io/github/rivomesh/repair/RepairPlan.class",
        "io/github/rivomesh/repair/RepairPlans.class",
        "io/github/rivomesh/repair/RepairProbe.class",
        "io/github/rivomesh/cdc/PausedRepairFence.class",
        "io/github/rivomesh/platform/ValidationRequest.class",
        "io/github/rivomesh/platform/RepairRequest.class",
        "io/github/rivomesh/cdc/RepairDependencies.class",
        "io/github/rivomesh/cdc/RepairConnectionGuard.class",
        "io/github/rivomesh/cdc/TargetRead.class",
        "io/github/rivomesh/cdc/JdbcTargetRead.class",
        "io/github/rivomesh/MysqlConsistentRead.class",
        "io/github/rivomesh/PipelineValidationBarrier.class",
        "io/github/rivomesh/cdc/SourceAdapter$ConsistentRead.class",
        "io/github/rivomesh/cdc/KingbaseProfile.class",
        "io/github/rivomesh/cdc/Gbase8cProfile.class",
        "com/gbase8c/Driver.class",
        "META-INF/rivomesh-licenses/gbase8c-jdbc.txt",
        "io/github/rivomesh/cdc/DmProfile.class",
        "io/github/rivomesh/cdc/DmStorage.class",
        "dm/jdbc/driver/DmDriver.class",
        "com/kingbase8/Driver.class",
        "io/github/rivomesh/platform/AccountDirectory.class",
        "io/github/rivomesh/transport/hive/HiveTransport.class",
        "io/github/rivomesh/cdc/HiveSink.class",
        "io/github/rivomesh/gaussdb/jdbc/Driver.class",
        "io/github/rivomesh/transport/pulsar/PulsarTransport.class",
        "io/github/rivomesh/transport/rocketmq/RocketTransport.class",
        "io/github/rivomesh/transport/artemis/ArtemisTransport.class",
        "io/github/rivomesh/cdc/ClassicMessagePublisher.class",
        "io/github/rivomesh/transport/classic/ClassicTransport.class",
        "io/github/rivomesh/classic/internal/jms/Message.class",
        "io/github/rivomesh/cdc/ClickHouseSink.class",
        "io/github/rivomesh/cdc/ClickHouseMaterializer.class",
        "io/github/rivomesh/cdc/ClickHouseHttpClient.class",
        "io/github/rivomesh/cdc/ClickHouseRowBinary.class",
        "io/github/rivomesh/cdc/ClickHouseIndexPlan.class",
        "io/github/rivomesh/artemis/internal/netty/channel/Channel.class",
        "io/github/rivomesh/artemis/internal/json/Json.class",
        "io/github/rivomesh/pulsar/internal/slf4j/Logger.class",
        "io/github/rivomesh/rocketmq/internal/netty/channel/Channel.class",
        "io/github/rivomesh/rocketmq/internal/fastjson2/JSON.class",
    ):
        assert name in names, "Missing runtime class: " + name
    for name in names:
        if (name.startswith("com/gbase8c/") or name.startswith("com/kingbase8/") or name.startswith("dm/jdbc/")) and name.endswith(".class"):
            assert struct.unpack(">H",jar.read(name)[6:8])[0]<=52, "Vendor JDBC runtime requires newer Java: "+name
    gbase_vendor=root / "gbase8c-driver/target/vendor/gbase8c-jdbc-3.0.0B02.jar"
    assert hashlib.sha256(gbase_vendor.read_bytes()).hexdigest()=="fe115f05124f6b9f37e49c829a0def28f7d09a6d871dd62a0c9f856b0aaa7e91", "Wrong GBase 8c SDK"
    with zipfile.ZipFile(gbase_vendor) as vendor:
        for entry in vendor.namelist():
            if entry.startswith("com/gbase8c/") and entry.endswith(".class"):
                assert jar.read(entry)==vendor.read(entry), "Changed GBase vendor class: "+entry
        assert jar.read("META-INF/rivomesh-licenses/gbase8c-jdbc.txt")==vendor.read("META-INF/LICENSE"), "Lost GBase license"
    sdk=jar.read("rivomesh-sdk/hive-jdbc-4.0.1-standalone.jar")
    assert hashlib.sha256(sdk).hexdigest()=="5d74bb0130aae6a8a2fad9cf5ffa1608974a55f1013464334490f7e06311959e", "Wrong private Hive SDK"
    assert "org/apache/hive/jdbc/HiveDriver.class" not in names, "Hive SDK leaked into the core loader"
    with zipfile.ZipFile(io.BytesIO(sdk)) as private:
        assert struct.unpack(">H", private.read("org/apache/hive/jdbc/HiveDriver.class")[6:8])[0]<=52, "Hive driver requires newer Java"
    assert b"atDebug" not in jar.read("org/slf4j/Logger.class"), "Core SLF4J 1.7 API replaced"
    assert "Main-Class: io.github.rivomesh.Main" in jar.read("META-INF/MANIFEST.MF").decode(), "Wrong executable entry point"
    drivers = jar.read("META-INF/services/java.sql.Driver").decode().splitlines()
    for driver in ("com.mysql.cj.jdbc.Driver", "org.postgresql.Driver", "org.h2.Driver", "io.github.rivomesh.gaussdb.jdbc.Driver", "com.kingbase8.Driver", "com.gbase8c.Driver", "dm.jdbc.driver.DmDriver"):
        assert driver in drivers, "Missing JDBC service: " + driver
print("ARTIFACT_ACCEPTED:", artifact.name, artifact.stat().st_size, "bytes; SHA-256", hashlib.sha256(artifact.read_bytes()).hexdigest())
