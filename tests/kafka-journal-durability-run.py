#!/usr/bin/env python3
"""Verify Kafka physical-receipt journal admission against disposable SQL servers."""
import argparse
import hashlib
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import tempfile
import time


IMAGES = {"postgres": "postgres:17", "mysql": "mysql:8.4"}
HELPER = Path(__file__).resolve().parent / "kafka" / "KafkaExternalJournalAcceptance.java"


def run(*arguments, **settings):
    return subprocess.run(arguments, check=True, text=True, **settings)


def verify_inputs(artifact, expected_sha256, java_home):
    if expected_sha256:
        digest = hashlib.sha256()
        with artifact.open("rb") as jar:
            for chunk in iter(lambda: jar.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != expected_sha256:
            raise RuntimeError("App digest differs from --artifact-sha256")
    version = run(str(java_home / "bin/java"), "-version", capture_output=True, timeout=10)
    if "Temurin" not in version.stderr or not re.search(r'version "21(?:[.+"]|$)', version.stderr):
        raise RuntimeError("Set JAVA_HOME or --java-home to Eclipse Temurin 21 LTS")


def compile_external(artifact, classes, java_home):
    run(str(java_home / "bin/javac"), "-cp", str(artifact), "-d", str(classes), str(HELPER), timeout=30)


def await_database(case, name):
    readiness = (
        ("pg_isready", "-U", "fixture_admin", "-d", "journal", "-h", "127.0.0.1", "-t", "2")
        if case == "postgres" else ("mysqladmin", "ping", "--protocol=tcp", "-h", "127.0.0.1", "--silent")
    )
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        check = subprocess.run(("docker", "exec", name, *readiness), capture_output=True, timeout=8)
        if check.returncode == 0:
            return
        time.sleep(2)
    raise RuntimeError(f"{case} initialization timed out")


def require_absent(name):
    container = run("docker", "ps", "-a", "--filter", f"name=^{name}$", "--format", "{{.Names}}", capture_output=True, timeout=12)
    volume = run("docker", "volume", "ls", "--filter", f"name={name}", "--format", "{{.Name}}", capture_output=True, timeout=12)
    network = run("docker", "network", "ls", "--filter", f"name={name}", "--format", "{{.Name}}", capture_output=True, timeout=12)
    if any(resource.stdout.strip() for resource in (container, volume, network)):
        raise RuntimeError(f"Fixture remains after teardown: {name}")
    print(f"Scoped cleanup verified {name}.", flush=True)


def database_case(case, artifact, classes, java_home, image):
    postgres = case == "postgres"
    name = f"rivomesh-receipt-{case}-{secrets.token_hex(5)}"
    password = secrets.token_hex(24)
    reader = secrets.token_hex(24)
    env = dict(os.environ, RIVOMESH_NATIVE_JOURNAL_SECRET=password, RIVOMESH_NATIVE_JOURNAL_READER_SECRET=reader)
    options = (
        ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")
        if postgres else ("MYSQL_ROOT_HOST", "MYSQL_ROOT_PASSWORD", "MYSQL_DATABASE")
    )
    env.update(zip(options, ("fixture_admin" if postgres else "%", password, "journal")))
    docker_env = tuple(item for option in options for item in ("-e", option))
    port, data, memory, admin = (
        ("5432", "/var/lib/postgresql/data", "640m", "fixture_admin")
        if postgres else ("3306", "/var/lib/mysql", "1100m", "root")
    )
    print(f"Starting {case} {name}.", flush=True)
    try:
        run("docker", "volume", "create", "--label", "io.github.rivomesh.fixture=physical-journal", name,
            capture_output=True, timeout=12)
        run("docker", "run", "--detach", "--name", name, "--cpus=1.0", f"--memory={memory}",
            "--publish", f"127.0.0.1::{port}", "--volume", f"{name}:{data}", *docker_env,
            image, capture_output=True, env=env, timeout=25)
        await_database(case, name)
        address = run("docker", "port", name, f"{port}/tcp", capture_output=True, timeout=10).stdout.strip()
        if not address.startswith("127.0.0.1:"):
            raise RuntimeError("Fixture address is outside loopback")
        url = f"jdbc:postgresql://{address}/journal?connectTimeout=5" if postgres else (
            f"jdbc:mysql://{address}/journal?sslMode=DISABLED&allowPublicKeyRetrieval=true&connectTimeout=5000")
        java = str(java_home / "bin/java")
        run(java, "-Xmx192m", "-cp", os.pathsep.join((str(artifact), str(classes))),
            "io.github.rivomesh.cdc.KafkaExternalJournalAcceptance", case, url, admin, env=env, timeout=95)
    except BaseException:
        result = subprocess.run(("docker", "logs", "--tail", "28", name), capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print((result.stdout + result.stderr).replace(password, "[synthetic-secret]").replace(reader, "[synthetic-secret]"),
                  file=sys.stderr, flush=True)
        raise
    finally:
        subprocess.run(("docker", "rm", "-f", name), capture_output=True, timeout=18)
        subprocess.run(("docker", "volume", "rm", name), capture_output=True, timeout=18)
        require_absent(name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--artifact-sha256", help="Require the supplied SHA-256 of the built app")
    parser.add_argument("--java-home", type=Path, default=os.environ.get("JAVA_HOME"), help="Eclipse Temurin 21 JDK")
    parser.add_argument("--only", choices=tuple(IMAGES))
    parser.add_argument("--postgres-image", default=IMAGES["postgres"])
    parser.add_argument("--mysql-image", default=IMAGES["mysql"])
    parser.add_argument("--preflight", action="store_true", help="Compile the helper without starting Docker")
    args = parser.parse_args()
    if args.java_home is None:
        parser.error("Set JAVA_HOME or --java-home to Eclipse Temurin 21 LTS")
    if args.artifact_sha256 and not re.fullmatch("[0-9a-f]{64}", args.artifact_sha256):
        parser.error("--artifact-sha256 must contain 64 lowercase hexadecimal characters")
    artifact = args.artifact.resolve(strict=True)
    cases = (args.only,) if args.only else tuple(IMAGES)
    images = {"postgres": args.postgres_image, "mysql": args.mysql_image}
    verify_inputs(artifact, args.artifact_sha256, args.java_home)
    with tempfile.TemporaryDirectory(prefix="rivomesh-external-journal-native-") as location:
        classes = Path(location)
        compile_external(artifact, classes, args.java_home)
        if args.preflight:
            print("Native helper compiled. No fixture started.", flush=True)
            return
        failures = []
        for case in cases:
            try:
                database_case(case, artifact, classes, args.java_home, images[case])
            except (RuntimeError, subprocess.SubprocessError) as error:
                print(f"Failed {case}: {type(error).__name__} {error}", file=sys.stderr, flush=True)
                failures.append(case)
        if failures:
            raise SystemExit("Native scenarios failed: " + ", ".join(failures))


if __name__ == "__main__":
    main()
