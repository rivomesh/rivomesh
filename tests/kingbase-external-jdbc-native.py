#!/usr/bin/env python3
"""Verify the external Kingbase JDBC driver, native target state and reconnect using Java 21."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import socket
import subprocess
import tempfile
import time
import uuid


ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / "tests"
CHECKS = (
    "KINGBASE_EXTERNAL_ORIGIN_AND_AUTH_ACCEPTED", "KINGBASE_EXTERNAL_TYPES_AND_ATOMICITY_ACCEPTED",
    "KINGBASE_EXTERNAL_OUTAGE_ACCEPTED", "KINGBASE_EXTERNAL_RECONNECT_REPLAY_ACCEPTED",
)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", required=True, type=Path)
    parser.add_argument("--kingbase-driver", required=True, type=Path)
    parser.add_argument("--java", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--image", default="kingbase_v009r001c001b0025_single_arm:v1")
    args = parser.parse_args()
    artifact, driver, java = (path.resolve(strict=True) for path in (args.artifact, args.kingbase_driver, args.java))
    version = subprocess.run([java, "-version"], capture_output=True, check=True, text=True, timeout=15).stderr
    javac = java.with_name("javac")
    if not re.search(r'version "21(?:[."]|$)', version) or not javac.is_file():
        parser.error("--java must point to a Java 21 JDK")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    project = "rivomesh-kingbase-external-" + uuid.uuid4().hex[:10]
    password = "aA2!" + secrets.token_hex(16)
    env = dict(os.environ, RIVOMESH_KINGBASE_IMAGE=args.image, RIVOMESH_TEST_KINGBASE_PASSWORD=password)
    fixture = TESTS / "kingbase/KingbaseExternalJdbcAcceptance.java"
    compose_spec = TESTS / "kingbase-compose.yml"
    proof = {
        "status": "FAIL", "project": project, "runtime": version.strip().splitlines(), "source": "synthetic row inputs",
        "artifactSha256": digest(artifact), "driverSha256": digest(driver), "fixtureSha256": digest(fixture),
        "profileSha256": digest(Path(__file__)), "composeSha256": digest(compose_spec), "checks": [],
    }

    with tempfile.TemporaryDirectory(prefix="kingbase-external-", dir=output.parent) as directory:
        state = Path(directory).resolve()
        classes = state / "classes"
        classes.mkdir()
        subprocess.run([javac, "--release", "21", "-cp", artifact, "-d", classes, fixture], check=True, timeout=30)
        test_class = "io.github.rivomesh.acceptance.KingbaseExternalJdbcAcceptance"
        no_driver = os.pathsep.join(map(str, (artifact, classes)))
        missing = subprocess.run([java, "-cp", no_driver, test_class, "absent"],
                                 capture_output=True, text=True, check=True, timeout=20)
        assert missing.stdout.strip() == "KINGBASE_EXTERNAL_MISSING_DRIVER_ACCEPTED"
        proof["checks"].append(missing.stdout.strip())

        with socket.socket() as hold:
            hold.bind(("127.0.0.1", 0))
            port = hold.getsockname()[1]
        overlay = state / "compose.json"
        overlay.write_text(json.dumps({"services": {"kingbase": {
            "ports": [f"127.0.0.1:{port}:54321"],
            "environment": {"DB_PASSWORD": "${RIVOMESH_TEST_KINGBASE_PASSWORD}"},
        }}}))
        base = ["docker", "compose", "--env-file", os.devnull, "--project-directory", str(TESTS),
                "-p", project, "-f", str(compose_spec), "-f", str(overlay)]

        def compose(*command, timeout=45, check=True):
            result = subprocess.run([*base, *command], cwd=state, env=env, text=True,
                                    capture_output=True, timeout=timeout)
            if check and result.returncode:
                try:
                    result.check_returncode()
                except subprocess.CalledProcessError as failure:
                    failure.add_note(result.stderr.replace(password, "[REDACTED]").strip()[:1400])
                    raise
            return result

        worker = None
        output_log = output / "native.log"
        try:
            compose("up", "--pull", "never", "-d", "--wait", "--wait-timeout", "200", "kingbase", timeout=220)
            containers = compose("ps", "-q").stdout.splitlines()
            assert len(containers) == 1
            inspection = json.loads(subprocess.check_output(["docker", "inspect", containers[0]], text=True, timeout=15))[0]
            assert inspection["Config"]["Labels"]["com.docker.compose.project"] == project
            assert inspection["Config"]["Labels"]["com.docker.compose.service"] == "kingbase"
            mounts = inspection["Mounts"]
            assert {(m["Type"], m["Destination"]) for m in mounts} == {
                ("bind", "/fixture/start.sh"), ("volume", "/home/kingbase/userdata"),
            }
            assert len(mounts) == 2
            bind = next(m for m in mounts if m["Type"] == "bind")
            configured = bind["Source"]
            for prefix in ("/host_mnt", "/run/desktop/mnt/host"):
                if configured.startswith(prefix + "/"):
                    configured = configured.removeprefix(prefix)
                    break
            expected_script = (TESTS / "kingbase/start.sh").resolve()
            assert not bind["RW"] and Path(configured).resolve() == expected_script
            loaded_script = compose("exec", "-T", "kingbase", "cat", "/fixture/start.sh").stdout.encode()
            assert hashlib.sha256(loaded_script).hexdigest() == digest(expected_script)
            proof["imageId"] = inspection["Image"]
            proof["startScriptSha256"] = digest(expected_script)
            host = f"127.0.0.1:{port}"
            assert compose("port", "kingbase", "54321").stdout.strip() == host
            url = f"jdbc:kingbase8://{host}/test?connectTimeout=3&socketTimeout=10"
            classpath = os.pathsep.join(map(str, (artifact, driver, classes)))
            with output_log.open("w") as log:
                worker = subprocess.Popen([java, "-cp", classpath, test_class, "native", driver, url, state],
                                          cwd=state, env=env, stdout=log, stderr=subprocess.STDOUT)
                deadline = time.monotonic() + 480
                while worker.poll() is None and time.monotonic() < deadline:
                    for phase in ("stop", "start"):
                        if (state / (phase + ".request")).exists() and not (state / (phase + ".done")).exists():
                            if phase == "stop":
                                compose("stop", "kingbase", timeout=45)
                            else:
                                compose("start", "--wait", "--wait-timeout", "150", "kingbase", timeout=170)
                                assert compose("port", "kingbase", "54321").stdout.strip() == host
                                proof["sameEndpointAcrossRestart"] = True
                            (state / (phase + ".done")).touch()
                    time.sleep(.25)
                assert worker.poll() == 0, "Native Kingbase fixture failed or timed out"
            lines = output_log.read_text().splitlines()
            assert all(check in lines for check in CHECKS)
            proof["checks"].extend(CHECKS)
            profiles = [line.removeprefix("KINGBASE_EXTERNAL_PROFILE: ") for line in lines
                        if line.startswith("KINGBASE_EXTERNAL_PROFILE: ")]
            receipts = [line.removeprefix("KINGBASE_EXTERNAL_RECEIPTS: ") for line in lines
                        if line.startswith("KINGBASE_EXTERNAL_RECEIPTS: ")]
            assert len(profiles) == len(receipts) == 1
            proof["nativeProfile"] = json.loads(profiles[0])
            proof["nativeReceipts"] = json.loads(receipts[0])
            assert digest(artifact) == proof["artifactSha256"] and digest(driver) == proof["driverSha256"]
            proof["status"] = "PASS"
        finally:
            if worker is not None and worker.poll() is None:
                worker.terminate()
                try:
                    worker.wait(timeout=12)
                except subprocess.TimeoutExpired:
                    worker.kill()
                    worker.wait(timeout=10)
            cleanup = compose("down", "-v", "--remove-orphans", timeout=75, check=False)
            proof["cleanupExitCode"] = cleanup.returncode
            if cleanup.returncode:
                proof["status"] = "FAIL"
            (output / "manifest.json").write_text(json.dumps(proof, indent=2) + "\n")
            if cleanup.returncode:
                raise RuntimeError("Native Kingbase fixture cleanup failed")
    print("KINGBASE_EXTERNAL_JDBC_NATIVE_ACCEPTED artifact=" + proof["artifactSha256"])


if __name__ == "__main__":
    main()
