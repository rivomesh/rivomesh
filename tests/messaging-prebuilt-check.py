#!/usr/bin/env python3
"""Refuse stale compiled inputs before a focused release-JAR fault-test rerun."""
import hashlib
import json
import os
import sys
from pathlib import Path
import runpy
import struct
from urllib.request import urlretrieve
import zipfile

root = Path(__file__).resolve().parents[1]
jar = (root / os.environ.get("RIVOMESH_TEST_ARTIFACT", "sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar")).resolve()
jar.relative_to(root)
arguments=sys.argv
try:
    sys.argv=[str(root / "tests/verify-artifact.py"),str(jar)]
    runpy.run_path(str(root / "tests/verify-artifact.py"), run_name="__main__")
finally:
    sys.argv=arguments
frozen=os.environ.get("RIVOMESH_RELEASE_MANIFEST")
if frozen:
    manifest_path=(root / frozen).resolve();manifest_path.relative_to(root);manifest=json.loads(manifest_path.read_text())
    assert hashlib.sha256(jar.read_bytes()).hexdigest()==manifest["artifactSha256"], "Frozen release differs"
    artifact=(root / manifest["artifact"]).resolve();artifact.relative_to(root)
    assert artifact.is_file() and hashlib.sha256(artifact.read_bytes()).hexdigest()==manifest["artifactSha256"], "Frozen artifact differs"
    classes=(root / manifest["testClasses"]).resolve();classes.relative_to(root);digest=hashlib.sha256()
    assert classes.is_dir(), "Frozen test classes missing"
    for entry in sorted(classes.rglob("*")):
        if entry.is_file():digest.update(str(entry.relative_to(classes)).encode()+b"\0"+entry.read_bytes())
    assert digest.hexdigest()==manifest["testClassesSha256"], "Frozen test classes differ"
    source_manifest=(root / manifest["sourceManifest"]).resolve();source_manifest.relative_to(root)
    assert hashlib.sha256(source_manifest.read_bytes()).hexdigest()==manifest["sourceManifestSha256"], "Historical source manifest differs"
    print("FROZEN_RELEASE_INPUTS_ACCEPTED:",manifest["artifactSha256"],"; current work-in-progress Java is outside this acceptance scope")
else:
    for module in ("sync-app", "gaussdb-driver", "gbase8c-driver", "pulsar-transport", "rocketmq-transport", "artemis-transport", "classic-transport", "hive-transport"):
        for source in (root / module / "src/main/java").rglob("*.java"):
            assert source.stat().st_mtime <= jar.stat().st_mtime, "Rebuild after changed source: " + str(source)
    for source in (root / "sync-app/src/test/java").rglob("*.java"):
        compiled = root / "sync-app/target/test-classes" / source.relative_to(root / "sync-app/src/test/java").with_suffix(".class")
        assert compiled.is_file() and compiled.stat().st_mtime >= source.stat().st_mtime, "Compile changed test: " + str(source)
version = "1.11.0"  # Same JUnit Platform release as the project's Jupiter 5.11.0.
tools = root / "target/tools"
tools.mkdir(parents=True, exist_ok=True)
console = tools / ("junit-platform-console-standalone-" + version + ".jar")
url = "https://repo.maven.apache.org/maven2/org/junit/platform/junit-platform-console-standalone/" + version + "/" + console.name
checksum = console.with_suffix(".jar.sha512")
if not checksum.is_file():
    urlretrieve(url + ".sha512", checksum)
if not console.is_file():
    temporary = console.with_suffix(".jar.download")
    urlretrieve(url, temporary)
    temporary.replace(console)
assert hashlib.sha512(console.read_bytes()).hexdigest() == checksum.read_text().strip().split()[0], "JUnit checksum differs"
with zipfile.ZipFile(console) as archive:
    assert struct.unpack(">H", archive.read("org/junit/platform/console/ConsoleLauncher.class")[6:8])[0] <= 52, "JUnit requires newer Java"
print("PREBUILT_INPUTS_ACCEPTED: focused pipeline rerun; full Maven regression remains separate")
