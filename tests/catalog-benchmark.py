#!/usr/bin/env python3
"""Measure catalog snapshots, restart or a separately journaled destination outage."""

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
    database_services = ["mysql", "postgres"]
    if args.slow_target:
        journal_override = directory / "logstore.json"
        journal_override.write_text(json.dumps({"services": {"logstore": {
            "extends": {"file": str(ROOT / "tests/benchmark-compose.yml"), "service": "postgres"}
        }}}))
        compose.extend(("-f", str(journal_override)))
        database_services.append("logstore")
        config["journal"] = {"mode": "EXTERNAL", "product": "POSTGRESQL", "namespace": "catalog_journal",
                             "url": "jdbc:postgresql://logstore:5432/bench?connectTimeout=5&socketTimeout=30",
                             "username": "postgres", "password": "Bench@Test123",
                             "maxBytes": 12 * args.schemas * (args.payload_bytes + 1024)}
    (directory / "pipeline.json").write_text(json.dumps(config))

    def execute(*arguments, sql=None, check=True, timeout=180):
        return subprocess.run([*compose, *arguments], env=environment, input=sql,
                              text=True, capture_output=True, check=check, timeout=timeout)

    def mysql(sql):
        return execute("exec", "-T", "-e", "MYSQL_PWD=Bench@Test123", "mysql", "mysql",
                       "-uroot", "--batch", "--raw", "--skip-column-names", sql=sql).stdout.strip()

    def postgres(sql, service="postgres"):
        return execute("exec", "-T", service, "psql", "-X", "-v", "ON_ERROR_STOP=1",
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

    def run_slow_target(snapshots, acknowledged):
        transactions, hold_seconds = 24, 5
        quota = config["journal"]["maxBytes"]
        measurement = report["phases"]["backpressure"] = {
            "logicalQuotaBytes": quota, "transactions": transactions,
            "changes": transactions * args.schemas, "maxObservedLogicalBytes": 0,
        }

        def state():
            point = json.loads(postgres(
                "SELECT json_build_object('capture',j.captured::json,'sequence',j.next_seq-1,"
                "'epoch',j.epoch,'generation',j.generation,'state',j.run_state,"
                "'pending',j.pending_bytes,'snapshots',j.snapshot_bytes,'identity',j.identity_bytes,"
                "'acknowledged',s.acknowledged) "
                "FROM catalog_journal.rm3_job j JOIN catalog_journal.rm3_subscription s "
                "ON j.pipeline_id=s.pipeline_id WHERE j.pipeline_id='catalog' AND s.subscription_id='postgres';",
                "logstore"))
            logical = point["pending"] + point["snapshots"] + point["identity"]
            if min(point["pending"], point["snapshots"], point["identity"]) < 0 or logical > quota:
                raise RuntimeError("CATALOG_LOGICAL_JOURNAL_QUOTA_EXCEEDED")
            measurement["maxObservedLogicalBytes"] = max(measurement["maxObservedLogicalBytes"], logical)
            return point

        def cut(point):
            position = point["capture"]["payload"]
            return position["file"], int(position["offset"])

        def drained():
            point = state()
            return point if point["pending"] == point["snapshots"] == 0 and point["sequence"] == acknowledged else None

        original = wait(drained, "CATALOG_INITIAL_EXTERNAL_JOURNAL_NOT_DRAINED")
        if original["acknowledged"] != acknowledged or original["state"] != "RUNNING":
            raise RuntimeError("CATALOG_INITIAL_EXTERNAL_JOURNAL_UNEXPECTED")
        target = execute("ps", "-q", "postgres").stdout.strip()
        journal = execute("ps", "-q", "logstore").stdout.strip()
        if not target or not journal or target == journal:
            raise RuntimeError("CATALOG_EXTERNAL_JOURNAL_NOT_INDEPENDENT")
        ready_to_restart = False
        try:
            execute("pause", "postgres")
            if subprocess.check_output(["docker", "inspect", "--format", "{{.State.Paused}}", target],
                                       text=True, timeout=30).strip() != "true":
                raise RuntimeError("CATALOG_TARGET_PAUSE_UNVERIFIED")
            measurement["targetContainerPaused"] = True
            started = time.monotonic()
            statements = []
            for version in range(1, transactions + 1):
                statements.append("START TRANSACTION;")
                statements.extend(f"UPDATE {source}.items SET version={version},label='{source}-delayed' WHERE id=1;"
                                  for source, _ in names)
                statements.append("COMMIT;")
            mysql("\n".join(statements))
            measurement["sourceWriteSeconds"] = time.monotonic() - started
            final_cut = binlog_position()
            measurement["sourceFinalCut"] = {"file": final_cut[0], "offset": final_cut[1]}
            if cut(original)[0] == final_cut[0]:
                measurement["binlogBytes"] = final_cut[1] - cut(original)[1]

            def backpressure():
                point = state()
                logs = execute("logs", "--no-color", "--no-log-prefix", "--tail", "100", "worker")
                if "JOURNAL_BACKPRESSURE" not in (logs.stdout + "\n" + logs.stderr).splitlines():
                    return None
                if not (point["state"] == "RUNNING" and point["acknowledged"] == acknowledged
                        and point["pending"] > 0 and original["sequence"] < point["sequence"]
                        < original["sequence"] + transactions and cut(point)[0] == final_cut[0]
                        and cut(point)[1] < final_cut[1]):
                    raise RuntimeError("CATALOG_BACKPRESSURE_WITHOUT_DURABLE_STALL")
                return point

            stalled = wait(backpressure, "CATALOG_JOURNAL_BACKPRESSURE_NOT_OBSERVED")
            measurement.update(pressureStartedAfterSeconds=time.monotonic() - started,
                               durableCapture={"file": cut(stalled)[0], "offset": cut(stalled)[1]},
                               frozenCaptureSequence=stalled["sequence"], frozenAcknowledged=stalled["acknowledged"],
                               sourceTransactionsBeyondCapture=transactions - (stalled["sequence"] - original["sequence"]),
                               frozenPendingBytes=stalled["pending"], frozenSnapshotBytes=stalled["snapshots"],
                               frozenIdentityBytes=stalled["identity"])
            observed_from = time.monotonic()
            samples = 1
            while time.monotonic() - observed_from < hold_seconds:
                time.sleep(0.5)
                current = state()
                if (current["capture"], current["sequence"], current["acknowledged"],
                        current["epoch"], current["state"]) != (stalled["capture"], stalled["sequence"],
                        stalled["acknowledged"], stalled["epoch"], "RUNNING"):
                    raise RuntimeError("CATALOG_DURABLE_CUT_MOVED_DURING_PRESSURE")
                samples += 1
            measurement.update(frozenSeconds=time.monotonic() - observed_from, frozenSamples=samples)
            measurement["journalRelationBytes"] = int(postgres(
                "SELECT COALESCE(SUM(pg_total_relation_size(c.oid)),0) FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='catalog_journal' AND c.relkind='r';",
                "logstore"))
            execute("kill", "-s", "SIGKILL", "worker")
            after_kill = state()
            if (cut(after_kill), after_kill["sequence"], after_kill["acknowledged"]) != (
                    cut(stalled), stalled["sequence"], acknowledged):
                raise RuntimeError("CATALOG_DURABLE_CUT_CHANGED_ON_WORKER_LOSS")
            ready_to_restart = True
        finally:
            unpause = execute("unpause", "postgres", check=False)
            measurement["targetUnpauseExitCode"] = unpause.returncode
            if unpause.returncode and ready_to_restart:
                raise RuntimeError("CATALOG_TARGET_UNPAUSE_FAILED")

        def recovered():
            point = state()
            return point if (point["state"] == "RUNNING" and point["epoch"] > stalled["epoch"]
                             and point["generation"] == original["generation"] and cut(point) == final_cut
                             and point["sequence"] == point["acknowledged"] == acknowledged + transactions
                             and change_sequence() == point["acknowledged"]) else None

        started = time.monotonic()
        execute("up", "-d", "worker")
        final = wait(recovered, "CATALOG_BACKPRESSURE_RESTART_TIMED_OUT")
        if snapshot_ids() != snapshots:
            raise RuntimeError("CATALOG_SNAPSHOT_IDENTIFIERS_CHANGED")
        report["phases"]["restart"] = {"seconds": time.monotonic() - started, "changes": transactions * args.schemas,
                                       "oldEpoch": stalled["epoch"], "newEpoch": final["epoch"],
                                       "finalCapture": {"file": cut(final)[0], "offset": cut(final)[1]},
                                       "finalAcknowledged": final["acknowledged"], "snapshotIdentifiersUnchanged": True}
        verify_content("restart", ((1, transactions, "delayed"), (2, 0, "second")))
        wait(lambda: state()["pending"] == 0, "CATALOG_JOURNAL_BACKLOG_NOT_RECLAIMED")
        report["phases"]["restart"]["pendingLogicalBytesAfterCollection"] = 0

    try:
        docker_info = subprocess.check_output(["docker", "info", "--format", "{{json .}}"], text=True, timeout=30)
        docker = json.loads(docker_info)
        report["docker"] = {key: docker[key] for key in
                            ("NCPU", "MemTotal", "Architecture", "Driver", "OperatingSystem", "ServerVersion")}
        execute("up", "-d", "--wait", "--wait-timeout", "180", *database_services)
        report["mysqlVersion"] = mysql("SELECT @@version;")
        report["postgreSQLVersion"] = postgres("SHOW server_version;")
        if args.slow_target:
            report["journalPostgreSQLVersion"] = postgres("SHOW server_version;", "logstore")
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

        if args.slow_target:
            run_slow_target(snapshots, before)
        else:
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
        ids = execute("ps", "-q", *database_services, "worker").stdout.split()
        inspection = json.loads(subprocess.check_output(["docker", "inspect", *ids], text=True, timeout=30))
        report["containers"] = [{"service": item["Config"]["Labels"]["com.docker.compose.service"],
                                 "image": item["Image"], "nanoCpus": item["HostConfig"]["NanoCpus"],
                                 "memoryBytes": item["HostConfig"]["Memory"]} for item in inspection]
        if ({i["service"] for i in report["containers"]} != {*database_services, "worker"}
                or any(i["nanoCpus"] != args.cpus * 10**9 or i["memoryBytes"] != 2**30
                       for i in report["containers"])):
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
    parser.add_argument("--schemas", type=int, help="Default: 256 for the catalog, 32 for --slow-target")
    parser.add_argument("--slow-target", action="store_true", help="Pause the destination independently of its external journal")
    parser.add_argument("--payload-bytes", type=int, default=8192)
    parser.add_argument("--cpus", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=360)
    args = parser.parse_args()
    if args.schemas is None:
        args.schemas = 32 if args.slow_target else 256
    if not args.artifact.is_file() or not (1 <= args.schemas <= 512 and 1 <= args.cpus <= 8
                                          and 32 <= args.payload_bytes <= 8192 and 30 <= args.timeout <= 1800):
        parser.error("an artifact, 1..512 schemas, 1..8 CPUs, 32..8192 payload bytes and timeout 30..1800 are required")
    args.output.mkdir(parents=True, exist_ok=True)
    (ROOT / "target").mkdir(exist_ok=True)
    report = {"status": "FAILED", "schemaCount": args.schemas, "payloadBytesPerRow": args.payload_bytes,
              "cpusPerContainer": args.cpus,
              "workerHeapMiB": 256, "journalMode": "EXTERNAL" if args.slow_target else "LOCAL", "phases": {}}
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
