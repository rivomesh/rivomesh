#!/usr/bin/env python3
"""Run the native Dameng acceptance with a frozen external-driver candidate and Java 21."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import tempfile
import time
import uuid


ROOT = Path(__file__).resolve().parents[1]
EXPECTED = (
    "DM_NATIVE_CONTRACT_ACCEPTED", "DM_EXTERNAL_ORIGIN_AND_NATIVE_ERROR_ACCEPTED",
    "DM_EXISTING_LAYOUTS_ACCEPTED", "DM_EXTERNAL_MISSING_DRIVER_ACCEPTED",
    "DM_NATIVE_TYPES_ACCEPTED", "DM_SNAPSHOT_ACCEPTED", "DM_CDC_KEYLESS_ACCEPTED",
    "DM_OUTAGE_CATCHUP_ACCEPTED", "DM_WORKER_RESTART_ACCEPTED", "DM_POSTGRES_REPAIR_ACCEPTED",
    "DM_NATIVE_SQL_REPAIR_ACCEPTED", "DM_PIPELINE_ACCEPTED",
)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--mysql-driver", type=Path, required=True)
    parser.add_argument("--dm-driver", type=Path, required=True)
    parser.add_argument("--java", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    artifact, mysql, dm, java = (
        path.resolve(strict=True) for path in (args.artifact, args.mysql_driver, args.dm_driver, args.java)
    )
    version = subprocess.run([java, "-version"], capture_output=True, check=True, text=True, timeout=15).stderr
    if not re.search(r'version "21(?:[."]|$)', version) or not java.with_name("javac").is_file():
        parser.error("--java must point to a Java 21 JDK")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    project = "rivomesh-dm-external-" + uuid.uuid4().hex[:10]
    tests = ROOT / "tests"
    fixture = tests / "dm" / "DmPipelineAcceptance.java"
    image_file = tests / "dm-compose.yml"
    recorded = {
        "status": "FAIL", "project": project, "runtime": version.strip().splitlines(),
        "fixtureSha256": digest(fixture), "profileSha256": digest(Path(__file__)),
        "composeSha256": digest(image_file),
        "artifactSha256": digest(artifact), "mysqlDriverSha256": digest(mysql), "dmDriverSha256": digest(dm),
    }
    env = dict(os.environ, RIVOMESH_DM_ARTIFACT=str(artifact), RIVOMESH_DM_MYSQL_DRIVER=str(mysql),
               RIVOMESH_DM_JDBC_DRIVER=str(dm))
    with tempfile.TemporaryDirectory(prefix="dm-external-private-", dir=output.parent) as temp:
        temporary = Path(temp).resolve()
        env["RIVOMESH_DM_DIR"] = str(temporary / "state")
        state = Path(env["RIVOMESH_DM_DIR"])
        classes = temporary / "classes"
        classes.mkdir()
        drivers = os.pathsep.join(map(str, (artifact, mysql, dm)))
        subprocess.run([java.with_name("javac"), "--release", "21", "-cp", drivers, "-d", classes, fixture],
                       check=True, timeout=45)
        overlay = temporary / "compose.json"
        ports = {"mysql": 3306, "postgres": 5432, "dm": 5236}
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            dm_host_port = reservation.getsockname()[1]
        overlay.write_text(json.dumps({"services": {
            service: {"ports": [f"127.0.0.1:{dm_host_port if service == 'dm' else ''}:{port}"]}
            for service, port in ports.items()
        }}))
        base = ["docker", "compose", "--env-file", os.devnull, "--project-directory", str(tests),
                "-p", project, "-f", str(image_file), "-f", str(overlay)]

        def compose(*command, timeout=50, check=True):
            return subprocess.run([*base, *command], cwd=temporary, env=env, text=True,
                                  capture_output=True, check=check, timeout=timeout)

        def verify_mounts():
            containers = compose("ps", "-q", *ports).stdout.splitlines()
            assert len(containers) == 3
            output_mounts = {}
            for details in json.loads(subprocess.check_output(["docker", "inspect", *containers], text=True, timeout=15)):
                service = details["Config"]["Labels"]["com.docker.compose.service"]
                assert details["Config"]["Labels"]["com.docker.compose.project"] == project
                expected_path = {"mysql": "/var/lib/mysql", "postgres": "/var/lib/postgresql/data",
                                 "dm": "/opt/dmdbms/data"}[service]
                mounts = details["Mounts"]
                volume_paths = {mount["Destination"] for mount in mounts if mount["Type"] == "volume"}
                assert volume_paths == {expected_path}
                found = {}
                for mount in mounts:
                    if mount["Type"] == "bind":
                        assert service == "dm" and not mount["RW"]
                        source = mount["Source"]
                        for prefix in ("/host_mnt", "/run/desktop/mnt/host"):
                            if source.startswith(prefix + "/"):
                                source = source.removeprefix(prefix)
                                break
                        found[mount["Destination"]] = Path(source).resolve()
                    else:
                        assert mount["Type"] == "volume"
                expected = {"/fixture/" + name: (tests / "dm" / name).resolve()
                            for name in ("start.sh", "health.sh")} if service == "dm" else {}
                assert found == expected, (found, expected)
                mounted_hashes = {}
                for destination, source in expected.items():
                    contents = compose("exec", "-T", service, "cat", destination).stdout.encode()
                    mounted = hashlib.sha256(contents).hexdigest()
                    assert mounted == digest(source)
                    mounted_hashes[destination] = mounted
                output_mounts[service] = {"imageId": details["Image"], "volume": expected_path,
                                           "readOnlyBindSha256": mounted_hashes}
            assert set(output_mounts) == set(ports)
            return output_mounts

        worker = None
        log_path = output / "native.log"
        try:
            try:
                compose("up", "--pull", "never", "-d", "--wait", "--wait-timeout", "300", *ports, timeout=320)
            except subprocess.CalledProcessError as error:
                error.add_note(error.stderr.strip()[:1800])
                raise
            recorded["services"] = verify_mounts()
            for service, port in ports.items():
                endpoint = compose("port", service, str(port)).stdout.strip()
                assert re.fullmatch(r"127\.0\.0\.1:[0-9]+", endpoint)
                name = "TARGET" if service == "dm" else service.upper()
                env[f"RIVOMESH_DM_{name}_ENDPOINT"] = endpoint
            with log_path.open("wb") as log:
                worker = subprocess.Popen([java, "-Xmx512m", "-Dh2.bindAddress=127.0.0.1", "-cp",
                                           drivers + os.pathsep + str(classes),
                                           "io.github.rivomesh.acceptance.DmPipelineAcceptance"],
                                          cwd=temporary, env=env, stdout=log, stderr=subprocess.STDOUT,
                                          start_new_session=True)
                deadline = time.monotonic() + 720
                while worker.poll() is None and time.monotonic() < deadline:
                    for phase in ("stop", "start"):
                        if (state / (phase + ".request")).exists() and not (state / (phase + ".done")).exists():
                            if phase == "stop":
                                compose("stop", "dm", timeout=85)
                            else:
                                compose("start", "--wait", "--wait-timeout", "210", "dm", timeout=225)
                                resumed_endpoint = compose("port", "dm", str(ports["dm"])).stdout.strip()
                                assert resumed_endpoint == env["RIVOMESH_DM_TARGET_ENDPOINT"]
                                recorded["sameEndpointAcrossRestart"] = True
                            (state / (phase + ".done")).touch()
                    time.sleep(.25)
                assert worker.poll() == 0, "Native Dameng acceptance failed or timed out"
            statements = log_path.read_text(errors="replace").splitlines()
            recorded["checks"] = [item for item in EXPECTED if any(line.startswith(item) for line in statements)]
            assert recorded["checks"] == list(EXPECTED)
            recorded["versionAndProfile"] = [
                line for line in statements if line.startswith(("DM_FIXTURE_MYSQL_VERSION:", "DM_FIXTURE_POSTGRES_VERSION:",
                                                                 "DM_NATIVE_PROFILE:"))
            ]
            assert len(recorded["versionAndProfile"]) == 3
            if (digest(artifact), digest(mysql), digest(dm)) != (
                    recorded["artifactSha256"], recorded["mysqlDriverSha256"], recorded["dmDriverSha256"]):
                raise AssertionError("JDBC input changed during the run")
            recorded["status"] = "PASS"
        finally:
            if worker is not None and worker.poll() is None:
                os.killpg(worker.pid, signal.SIGTERM)
                try:
                    worker.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    os.killpg(worker.pid, signal.SIGKILL)
                    worker.wait(timeout=10)
            removal = compose("down", "-v", "--remove-orphans", timeout=90, check=False)
            recorded["cleanupExitCode"] = removal.returncode
            if removal.returncode:
                recorded["status"] = "FAIL"
            (output / "manifest.json").write_text(json.dumps(recorded, indent=2) + "\n")
            if removal.returncode:
                raise RuntimeError("Fixture cleanup failed")
    print("DAMENG_EXTERNAL_JDBC_NATIVE_ACCEPTED artifact=" + recorded["artifactSha256"])


if __name__ == "__main__":
    main()
