#!/usr/bin/env python3
"""Measure complete snapshots and restart against many explicitly mapped schemas."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import uuid


ROOT = Path(__file__).resolve().parents[1]


def run_profile(args, directory, report):
    names = [(f"source_{n:04d}", f"copy_{n:04d}") for n in range(1, args.schemas + 1)]
    pad = "x" * args.payload_bytes
    shutil.copyfile(args.artifact, directory / "app.jar")
    with (directory / "app.jar").open("rb") as app:
        report["artifactSha256"] = hashlib.file_digest(app, "sha256").hexdigest()
    environment = dict(os.environ, RIVOMESH_BENCH_DIR=str(directory), RIVOMESH_BENCH_CPUS=str(args.cpus),
                       RIVOMESH_BENCH_DB_MEMORY="1g", RIVOMESH_BENCH_WORKER_MEMORY="1g",
                       RIVOMESH_BENCH_WORKER_HEAP_MIB="256", RIVOMESH_BENCH_WORKER_IMAGE="eclipse-temurin:21-jre-jammy")
    compose = ["docker", "compose", "--env-file", os.devnull, "-p", "rivomesh-catalog-" + uuid.uuid4().hex[:12],
               "-f", str(ROOT / "tests/benchmark-compose.yml")]
    config = {
        "formatVersion": 3, "pipelineId": "catalog", "nodeId": "catalog-worker",
        "source": {"clusterId": "catalog-source", "host": "mysql", "port": 3306,
                   "username": "root", "password": "Bench@Test123", "serverId": 911310,
                   "topology": "AUTO", "expectedVersion": "8.4"},
        "databases": [source for source, _ in names],
        "targets": [{"id": "postgres", "product": "POSTGRESQL", "username": "postgres",
                     "password": "Bench@Test123", "metadataSchema": "catalog_checkpoints",
                     "url": "jdbc:postgresql://postgres:5432/bench?connectTimeout=5&socketTimeout=30",
                     "mappings": dict(names)}],
        "journal": {"mode": "LOCAL", "path": "/bench/journal"}, "chunkSize": 50,
        "leaseMillis": 8000, "heartbeatMillis": 500, "pollIntervalMillis": 100,
    }
    (directory / "pipeline.json").write_text(json.dumps(config))

    def execute(*arguments, sql=None, check=True, timeout=180):
        return subprocess.run([*compose, *arguments], env=environment, input=sql,
                              text=True, capture_output=True, check=check, timeout=timeout)

    def mysql(sql):
        return execute("exec", "-T", "-e", "MYSQL_PWD=Bench@Test123", "mysql", "mysql",
                       "-uroot", "--batch", "--raw", "--skip-column-names", sql=sql).stdout.strip()

    def postgres(sql):
        return execute("exec", "-T", "postgres", "psql", "-X", "-v", "ON_ERROR_STOP=1",
                       "-U", "postgres", "-d", "bench", "-At", "-F", "\t", sql=sql).stdout.strip()

    def wait(condition, label):
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            if result := condition():
                return result
            services = execute("ps", "--status", "running", "--services").stdout.splitlines()
            if "worker" not in services:
                raise RuntimeError("CATALOG_WORKER_EXITED")
            time.sleep(0.5)
        raise RuntimeError(label)

    def finished_snapshots():
        if postgres("SELECT to_regclass('catalog_checkpoints.rm3_sink_table');") == "":
            return None
        return postgres("SELECT count(*) FROM catalog_checkpoints.rm3_sink_table "
                        "WHERE pipeline_id='catalog' AND snapshot_state='COMPLETE' AND row_count=2;") == str(args.schemas)

    def snapshot_ids():
        return postgres("SELECT table_id,snapshot_id FROM catalog_checkpoints.rm3_sink_table "
                        "WHERE pipeline_id='catalog' ORDER BY table_id;")

    def change_sequence():
        return int(postgres("SELECT stream_seq FROM catalog_checkpoints.rm3_sink_job WHERE pipeline_id='catalog';"))

    def binlog_position():
        coordinates = mysql("SHOW BINARY LOG STATUS;").split("\t")
        return coordinates[0], int(coordinates[1])

    def verify_content(phase, ids):
        statements = [[], []]
        expected = []
        for source, target in names:
            for slot, schema in enumerate((source, target)):
                statements[slot].append(f"SELECT '{source}' AS namespace_id,id,version,label,pad FROM {schema}.items")
            expected.extend(f"{source}\t{identity}\t{row_version}\t{source}-{label}\t{pad}"
                            for identity, row_version, label in ids)
        sql = [" UNION ALL ".join(items) + " ORDER BY namespace_id,id;" for items in statements]
        source, target = mysql(sql[0]), postgres(sql[1])
        content = "\n".join(expected)
        if source != content or target != content:
            raise RuntimeError("CATALOG_CONTENT_MISMATCH_" + phase.upper())
        report["phases"][phase].update(rowCount=len(expected),
                                       contentSha256=hashlib.sha256(content.encode()).hexdigest())

    try:
        docker_info = subprocess.check_output(["docker", "info", "--format", "{{json .}}"], text=True, timeout=30)
        docker = json.loads(docker_info)
        report["docker"] = {key: docker[key] for key in
                            ("NCPU", "MemTotal", "Architecture", "Driver", "OperatingSystem", "ServerVersion")}
        execute("up", "-d", "--wait", "--wait-timeout", "180", "mysql", "postgres")
        report["mysqlVersion"] = mysql("SELECT @@version;")
        report["postgreSQLVersion"] = postgres("SHOW server_version;")
        seed = []
        for source, _ in names:
            seed.extend((f"CREATE DATABASE {source} CHARACTER SET utf8mb4;",
                         f"CREATE TABLE {source}.items (id BIGINT PRIMARY KEY,version INT NOT NULL,"
                         f"label VARCHAR(100) NOT NULL,pad VARCHAR({args.payload_bytes}) NOT NULL) ENGINE=InnoDB;",
                         f"INSERT INTO {source}.items VALUES (1,0,'{source}-first','{pad}'),"
                         f"(2,0,'{source}-second','{pad}');"))
        mysql("\n".join(seed))
        snapshot_start = time.monotonic()
        execute("up", "-d", "--no-deps", "worker")
        wait(finished_snapshots, "CATALOG_SNAPSHOT_TIMED_OUT")
        snapshots = snapshot_ids()
        if len(snapshots.splitlines()) != args.schemas:
            raise RuntimeError("CATALOG_SNAPSHOTS_MISSING")
        report["phases"]["snapshot"] = {"seconds": time.monotonic() - snapshot_start}
        verify_content("snapshot", ((1, 0, "first"), (2, 0, "second")))
        report["snapshotIdentifiersSha256"] = hashlib.sha256(snapshots.encode()).hexdigest()
        before = change_sequence()

        binlog_before = binlog_position()
        started = time.monotonic()
        statements = ["START TRANSACTION;"]
        for source, _ in names:
            statements.extend((f"UPDATE {source}.items SET version=1,label='{source}-updated' WHERE id=1;",
                               f"DELETE FROM {source}.items WHERE id=2;",
                               f"INSERT INTO {source}.items VALUES (3,1,'{source}-added','{pad}');"))
        mysql("\n".join([*statements, "COMMIT;"]))
        wait(lambda: change_sequence() > before, "CATALOG_COMMIT_TIMED_OUT")
        report["phases"]["cdc"] = {"seconds": time.monotonic() - started, "changes": args.schemas * 3}
        binlog_after = binlog_position()
        if binlog_before[0] == binlog_after[0]:
            report["phases"]["cdc"]["binlogBytes"] = binlog_after[1] - binlog_before[1]
        verify_content("cdc", ((1, 1, "updated"), (3, 1, "added")))

        execute("kill", "-s", "SIGKILL", "worker")
        before = change_sequence()
        mysql("\n".join(["START TRANSACTION;", *(f"UPDATE {source}.items SET version=2 WHERE id=1;"
                                                  for source, _ in names), "COMMIT;"]))
        started = time.monotonic()
        execute("up", "-d", "worker")
        wait(lambda: change_sequence() > before, "CATALOG_RESTART_TIMED_OUT")
        if snapshot_ids() != snapshots:
            raise RuntimeError("CATALOG_SNAPSHOT_IDENTIFIERS_CHANGED")
        report["phases"]["restart"] = {"seconds": time.monotonic() - started, "changes": args.schemas}
        verify_content("restart", ((1, 2, "updated"), (3, 1, "added")))
        ids = execute("ps", "-q", "mysql", "postgres", "worker").stdout.split()
        inspection = json.loads(subprocess.check_output(["docker", "inspect", *ids], text=True, timeout=30))
        report["containers"] = [{"service": item["Config"]["Labels"]["com.docker.compose.service"],
                                 "image": item["Image"], "nanoCpus": item["HostConfig"]["NanoCpus"],
                                 "memoryBytes": item["HostConfig"]["Memory"]} for item in inspection]
        if len(inspection) != 3 or any(i["nanoCpus"] != args.cpus * 10**9 or i["memoryBytes"] != 2**30
                                        for i in report["containers"]):
            raise RuntimeError("CATALOG_RESOURCE_LIMITS_UNVERIFIED")
        report["workerJvm"] = execute("exec", "-T", "worker", "java", "-version").stderr.splitlines()[0]
        report["status"] = "PASSED"
    except Exception:
        logs = execute("logs", "--no-color", "--tail", "60", "worker", check=False, timeout=30)
        (args.output / "worker.log").write_text(logs.stdout.replace("Bench@Test123", "[REDACTED]"))
        raise
    finally:
        cleanup = execute("down", "-v", "--remove-orphans", check=False)
        report["cleanupExitCode"] = cleanup.returncode
        if cleanup.returncode:
            report["status"] = "FAILED"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--schemas", type=int, default=256)
    parser.add_argument("--payload-bytes", type=int, default=8192)
    parser.add_argument("--cpus", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=360)
    args = parser.parse_args()
    if not args.artifact.is_file() or not (1 <= args.schemas <= 512 and 1 <= args.cpus <= 8
                                          and 32 <= args.payload_bytes <= 8192 and 30 <= args.timeout <= 1800):
        parser.error("an artifact, 1..512 schemas, 1..8 CPUs, 32..8192 payload bytes and timeout 30..1800 are required")
    args.output.mkdir(parents=True, exist_ok=True)
    (ROOT / "target").mkdir(exist_ok=True)
    report = {"status": "FAILED", "schemaCount": args.schemas, "payloadBytesPerRow": args.payload_bytes,
              "cpusPerContainer": args.cpus,
              "workerHeapMiB": 256, "journalMode": "LOCAL", "phases": {}}
    try:
        with tempfile.TemporaryDirectory(prefix="rivomesh-catalog-", dir=ROOT / "target") as scratch:
            run_profile(args, Path(scratch), report)
    except Exception as failure:
        report["failureClass"] = type(failure).__name__
        if isinstance(failure, RuntimeError):
            report["failureCode"] = str(failure)
    finally:
        (args.output / "manifest.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if report["status"] == "PASSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
