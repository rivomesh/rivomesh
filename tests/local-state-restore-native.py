#!/usr/bin/env python3
"""Restore a matched LOCAL journal and native PostgreSQL destination on a disposable MySQL pipeline."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import tempfile
import time
import uuid


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def checked_gtid(value):
    if not re.fullmatch(r"[a-f0-9:,-]+", value):
        raise AssertionError("Unexpected GTID profile")
    return "'" + value + "'"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--java", type=Path, required=True, help="Executable from a Java 21 JDK")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-image", default="mysql:8.4")
    parser.add_argument("--target-image", default="postgres:17")
    args = parser.parse_args()
    artifact, java = (path.resolve(strict=True) for path in (args.artifact, args.java))
    archive_sha = digest(artifact)
    javac = java.with_name("javac")
    if not javac.is_file():
        parser.error("--java must come from a JDK with javac")
    supervisor_source = Path(__file__).resolve().parent / "roles/LocalRestoreWorker.java"
    java_version = subprocess.run([java, "-version"], capture_output=True, text=True, check=True, timeout=15).stderr
    if not re.search(r'version "21(?:[."]|$)', java_version):
        parser.error("--java must be a Java 21 runtime")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    project = "rivomesh-local-restore-" + uuid.uuid4().hex[:10]
    manifest = {
        "status": "FAIL", "checks": [], "artifactSha256": archive_sha,
        "fixtureSha256": digest(Path(__file__)), "workerLauncherSha256": digest(supervisor_source),
        "runtime": java_version.strip().splitlines(),
        "journal": "LOCAL H2", "sourceImage": args.source_image, "targetImage": args.target_image,
    }
    environment = dict(os.environ, RM_RESTORE_MYSQL=secrets.token_urlsafe(24),
                       RM_RESTORE_POSTGRES=secrets.token_urlsafe(24))
    worker = None

    def passed(label):
        manifest["checks"].append(label)
        print("PASS " + label, flush=True)

    with tempfile.TemporaryDirectory(prefix="rivomesh-native-local-restore-") as temporary:
        root = Path(temporary)
        classes = root / "classes"
        classes.mkdir()
        subprocess.run([javac, "--release", "21", "-cp", artifact, "-d", classes, supervisor_source],
                       check=True, timeout=40)
        services = {
            "mysql": {
                "image": args.source_image, "ports": ["127.0.0.1::3306"],
                "command": ["--server-id=111", "--log-bin=mysql-bin", "--binlog-format=ROW",
                            "--binlog-row-image=FULL", "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
                            "--log-replica-updates=ON", "--binlog-transaction-compression=OFF"],
                "environment": {"MYSQL_ROOT_PASSWORD": "${RM_RESTORE_MYSQL}", "MYSQL_ROOT_HOST": "%"},
                "healthcheck": {"test": ["CMD-SHELL", 'MYSQL_PWD="$$MYSQL_ROOT_PASSWORD" '
                                         'mysql -h127.0.0.1 -uroot -e "SELECT 1"'],
                                "interval": "2s", "timeout": "5s", "retries": 60},
            },
            "postgres": {
                "image": args.target_image, "ports": ["127.0.0.1::5432"],
                "environment": {"POSTGRES_PASSWORD": "${RM_RESTORE_POSTGRES}", "POSTGRES_DB": "restore_demo"},
                "healthcheck": {"test": ["CMD", "pg_isready", "-h", "127.0.0.1", "-U", "postgres", "-d", "restore_demo"],
                                "interval": "2s", "timeout": "5s", "retries": 60},
            },
        }
        compose_path = root / "compose.json"
        compose_path.write_text(json.dumps({"services": services}))
        base = ["docker", "compose", "-p", project, "--project-directory", str(root), "-f", str(compose_path)]

        def compose(*command, data=None, check=True, text=True, timeout=45):
            return subprocess.run([*base, *command], cwd=root, env=environment, input=data, text=text,
                                  capture_output=True, timeout=timeout, check=check)

        def mysql(statement):
            return compose("exec", "-T", "mysql", "sh", "-ec",
                           'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -NB', data=statement).stdout.strip()

        def postgres(statement, database="restore_demo"):
            return compose("exec", "-T", "postgres", "psql", "-XqAt", "-v", "ON_ERROR_STOP=1",
                           "-U", "postgres", "-d", database, data=statement).stdout.strip()

        def target_identity(database="restore_demo"):
            system, oid = postgres("SELECT system_identifier::text, (SELECT oid FROM pg_database "
                                   "WHERE datname=current_database()) FROM pg_control_system()", database).split("|")
            return {"systemIdentifier": system, "databaseOid": int(oid)}

        def config(directory, database="restore_demo"):
            directory.mkdir(parents=True, exist_ok=True)
            path = directory / "task.yml"
            path.write_text(json.dumps({
                "formatVersion": 3, "pipelineId": "local-restore-native", "nodeId": "local-restore-fixture",
                "databases": ["sales"], "chunkSize": 1, "leaseMillis": 5000, "heartbeatMillis": 500,
                "pollIntervalMillis": 200, "retryDelayMillis": 200,
                "source": {"type": "MYSQL", "expectedVersion": "8.4", "clusterId": "local-restore-source",
                           "host": "127.0.0.1", "port": mysql_port, "username": "root",
                           "password": "${RM_RESTORE_MYSQL}", "serverId": 811071},
                "journal": {"mode": "LOCAL", "path": "journal"},
                "targets": [{"id": "pg", "product": "POSTGRESQL", "metadataSchema": "restore_meta",
                             "url": f"jdbc:postgresql://127.0.0.1:{pg_port}/{database}"
                                    "?ApplicationName=rivomesh-native-restore&connectTimeout=4&socketTimeout=15",
                             "username": "postgres", "password": "${RM_RESTORE_POSTGRES}",
                             "mappings": {"sales": "replica"}}],
            }))
            path.chmod(0o600)
            return path

        java_options = [java, "-Xmx384m", "-Dh2.bindAddress=127.0.0.1"]
        java_command = [*java_options, "-jar", artifact]
        supervisor = [*java_options, "-cp", os.pathsep.join(map(str, (artifact, classes))),
                      "io.github.rivomesh.cdc.LocalRestoreWorker"]

        def cli(action, task, *extra, check=True):
            return subprocess.run([*java_command, action, task, *map(str, extra)], cwd=root, env=environment,
                                  text=True, capture_output=True, check=check, timeout=40)

        def start(task):
            nonlocal worker
            assert worker is None
            with (root / "worker.log").open("ab") as log:
                worker = subprocess.Popen([*supervisor, "run", task], cwd=root, env=environment, stdin=subprocess.PIPE,
                                          stdout=log, stderr=subprocess.STDOUT)

        def stop(verify=True):
            nonlocal worker
            if worker is None:
                return
            try:
                if worker.poll() is None:
                    try:
                        worker.stdin.write(b"stop\n")
                        worker.stdin.flush()
                    except BrokenPipeError:
                        pass
                exit_code = worker.wait(timeout=30)
                if verify:
                    log = (root / "worker.log").read_text(errors="replace")
                    assert exit_code == 0 and log.rstrip().endswith("LOCAL_RESTORE_WORKER_CLOSED")
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait(timeout=10)
                raise AssertionError("Worker did not stop gracefully")
            finally:
                try:
                    worker.stdin.close()
                except BrokenPipeError:
                    pass
                worker = None

        def wait(check, label, seconds=90):
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline:
                if worker is not None and worker.poll() is not None:
                    log = (root / "worker.log").read_text(errors="replace")
                    codes = re.findall(r"PIPELINE_DIAGNOSTIC ([A-Z_]+ [A-Z0-9_]+)", log)
                    raise AssertionError(f"Worker stopped while {label}, diagnostics: {codes[-5:]}")
                try:
                    value = check()
                except subprocess.CalledProcessError:
                    value = None
                if value:
                    return value
                time.sleep(0.35)
            raise AssertionError("Timed out while " + label)

        def pause_and_stop(task):
            pause = json.loads(cli("pause", task).stdout)
            assert pause["targetFences"] == {"pg": "FENCED"} and pause["pipeline"]["state"] == "PAUSED"
            stop()
            wait(lambda: postgres("SELECT count(*) FROM pg_stat_activity "
                                  "WHERE application_name='rivomesh-native-restore'") == "0",
                 "waiting for the stopped Worker to release PostgreSQL sessions", 20)
            clean = subprocess.run([*supervisor, "shutdown-h2", task], cwd=root, env=environment, text=True,
                                   capture_output=True, check=True, timeout=20)
            assert clean.stdout.strip() == "LOCAL_RESTORE_H2_CLOSED"
            state = inspect_journal(task)
            assert state["state"] == "PAUSED" and state["leaseOwner"] is None
            assert not (task.parent / "journal/rivomesh.lock.db").exists()
            return pause["pipeline"]["errorGeneration"]

        def inspect_journal(task):
            endpoint = f'jdbc:h2:file:{task.parent / "journal/rivomesh"};IFEXISTS=TRUE;ACCESS_MODE_DATA=r'
            sql = '''SELECT 'RM_CUT=' || CAST(JSON_OBJECT(
                'generation': j.generation, 'fingerprint': j.fingerprint, 'epoch': j.epoch,
                'position': j.captured FORMAT JSON, 'sequence': (j.next_seq-1),
                'normalized': j.normalized_seq, 'state': j.run_state, 'errorGeneration': j.error_generation,
                'acknowledged': sub.acknowledged, 'leaseOwner': j.owner,
                'schemaHashes': (SELECT JSON_ARRAYAGG(sc.payload_hash ORDER BY sc.table_id,sc.schema_version)
                    FROM "rivomesh_v3"."rm3_schema" sc WHERE sc.pipeline_id=j.pipeline_id) FORMAT JSON,
                'snapshots': (SELECT JSON_ARRAYAGG(JSON_OBJECT(
                    'tableId': s.table_id, 'snapshotId': s.snapshot_id, 'rows': s.row_count, 'state': s.run_state)
                    ORDER BY s.table_id) FROM "rivomesh_v3"."rm3_snapshot" s
                    WHERE s.pipeline_id=j.pipeline_id AND s.snapshot_scope='source') FORMAT JSON
                ) AS VARCHAR) AS proof FROM "rivomesh_v3"."rm3_job" j JOIN "rivomesh_v3"."rm3_subscription" sub
                  ON sub.pipeline_id=j.pipeline_id WHERE j.pipeline_id='local-restore-native' AND sub.subscription_id='pg';'''
            result = subprocess.run([*java_options, "-cp", artifact, "org.h2.tools.Shell", "-url", endpoint,
                                     "-user", "", "-password", ""], input="maxwidth 8192\n" + sql + "\nexit\n",
                                    cwd=root, text=True, capture_output=True, check=True, timeout=20)
            line = re.search(r"^RM_CUT=(\{.*\})$", result.stdout, re.M)
            if not line:
                raise AssertionError("Could not inspect the stopped LOCAL journal")
            return json.loads(line[1])

        def source_rows():
            return [mysql("SELECT id,note,derived,JSON_UNQUOTE(JSON_EXTRACT(payload,'$.n')) FROM sales.items ORDER BY id")
                    .replace("\t", "|"),
                    mysql("SELECT value,JSON_UNQUOTE(JSON_EXTRACT(payload,'$.n')),COUNT(*) FROM sales.duplicates "
                          "GROUP BY 1,2 ORDER BY 1,2").replace("\t", "|")]

        def target_rows(database="restore_demo"):
            return [postgres("SELECT id,note,derived,payload->>'n' FROM replica.items ORDER BY id", database),
                    postgres("SELECT value,payload->>'n',COUNT(*) FROM replica.duplicates GROUP BY 1,2 ORDER BY 1,2", database)]

        def target_cut(database="restore_demo"):
            generation, epoch, seq, transaction_id, transaction_hash = postgres(
                "SELECT generation,epoch,stream_seq,transaction_id,transaction_hash FROM restore_meta.rm3_sink_job "
                "WHERE pipeline_id='local-restore-native' AND subscription_id='pg'", database).split("|")
            snapshots = json.loads(postgres(
                "SELECT jsonb_agg(jsonb_build_object('tableId',table_id,'snapshotId',snapshot_id,"
                "'rows',row_count,'state',snapshot_state) ORDER BY table_id) FROM restore_meta.rm3_sink_table "
                "WHERE pipeline_id='local-restore-native'", database))
            return {"generation": generation, "epoch": int(epoch), "sequence": int(seq), "transactionId": transaction_id,
                    "transactionHash": transaction_hash, "snapshots": snapshots, "rows": target_rows(database)}

        def drain_observed(task, expected, gtid):
            target = target_cut()
            if target["rows"] != expected or target["transactionId"] != gtid:
                return False
            status = json.loads(cli("status", task).stdout)
            return (status["lastSequence"] == status["normalizedSequence"]
                    == status["subscriptions"][0]["acknowledged"] == target["sequence"])

        def gtid_after(previous):
            value = mysql(f"SELECT GTID_SUBTRACT(@@GLOBAL.gtid_executed,{checked_gtid(previous)})")
            assert re.fullmatch(re.escape(source_uuid) + r":\d+", value), value
            return value

        def copy_journal(task, archive):
            destination = task.parent / "journal/rivomesh.mv.db"
            destination.parent.mkdir()
            shutil.copyfile(archive, destination)

        def restore_target(backup, database, clean=False):
            options = ["--clean", "--if-exists"] if clean else []
            compose("exec", "-T", "postgres", "pg_restore", "--single-transaction", "--exit-on-error",
                    "--no-owner", "--no-privileges", *options, "-U", "postgres", "-d", database,
                    data=backup, text=False)

        try:
            compose("up", "--pull", "never", "-d", "--wait", "--wait-timeout", "145", timeout=165)
            mysql_endpoint = compose("port", "mysql", "3306").stdout.strip()
            pg_endpoint = compose("port", "postgres", "5432").stdout.strip()
            assert mysql_endpoint.startswith("127.0.0.1:") and pg_endpoint.startswith("127.0.0.1:")
            mysql_port, pg_port = (int(endpoint.rsplit(":", 1)[1]) for endpoint in (mysql_endpoint, pg_endpoint))
            versions = {"mysql": mysql("SELECT VERSION()"), "postgres": postgres("SHOW server_version")}
            assert versions["mysql"].startswith("8.4.") and versions["postgres"].startswith("17.")
            manifest["versions"] = versions
            for engine, image in (("mysql", args.source_image), ("postgres", args.target_image)):
                manifest[engine + "ImageId"] = subprocess.check_output(
                    ["docker", "image", "inspect", "--format", "{{.Id}}", image], text=True, timeout=10).strip()
            source_uuid = mysql("SELECT @@GLOBAL.server_uuid")
            mysql("""CREATE DATABASE sales;
                CREATE TABLE sales.items (id BIGINT PRIMARY KEY,note VARCHAR(50),payload JSON,
                  derived BIGINT GENERATED ALWAYS AS (id*2) VIRTUAL);
                INSERT INTO sales.items(id,note,payload) VALUES (1,'snapshot',JSON_OBJECT('n',1)),(2,'second',JSON_OBJECT('n',2));
                CREATE TABLE sales.duplicates(value INT,payload JSON);
                INSERT INTO sales.duplicates VALUES (7,JSON_OBJECT('n',7)),(7,JSON_OBJECT('n',7));""")
            live = config(root / "live")
            start(live)
            snapshot_rows = source_rows()
            wait(lambda: target_rows() == snapshot_rows, "finishing native snapshots")
            before_gtid = mysql("SELECT @@GLOBAL.gtid_executed")
            mysql("""START TRANSACTION;
                INSERT INTO sales.items(id,note,payload) VALUES (3,'before backup',JSON_OBJECT('n',3));
                UPDATE sales.duplicates SET value=8 WHERE value=7 LIMIT 1;
                COMMIT;""")
            saved_gtid = gtid_after(before_gtid)
            expected = source_rows()
            wait(lambda: drain_observed(live, expected, saved_gtid),
                 "delivering the exact pre-backup GTID")
            error_generation = pause_and_stop(live)
            saved = inspect_journal(live)
            native = target_cut()
            assert saved["state"] == "PAUSED" and saved["leaseOwner"] is None
            assert saved["errorGeneration"] == error_generation
            assert saved["sequence"] == saved["acknowledged"] == saved["normalized"] == native["sequence"] > 0
            assert saved["generation"] == native["generation"] and saved["snapshots"] == native["snapshots"]
            assert saved["epoch"] == native["epoch"]
            assert sorted(s["rows"] for s in saved["snapshots"]) == [2, 2]
            assert len(saved["schemaHashes"]) == 2 and len(native["transactionHash"]) == 64
            assert mysql(f"SELECT GTID_SUBSET({checked_gtid(saved_gtid)},"
                         f"{checked_gtid(saved['position']['payload']['gtidSet'])})") == "1"
            pg_identity = target_identity()
            journal_backup = root / "journal-backup.mv.db"
            live_path = live.parent / "journal/rivomesh.mv.db"
            shutil.copyfile(live_path, journal_backup)
            database_backup = compose("exec", "-T", "postgres", "pg_dump", "--format=custom", "--no-owner",
                                      "--no-privileges", "-U", "postgres", "-d", "restore_demo", text=False).stdout
            assert database_backup.startswith(b"PGDMP") and target_cut() == native
            manifest["backup"] = {"localSha256": digest(journal_backup),
                                  "pgDumpSha256": hashlib.sha256(database_backup).hexdigest(),
                                  "journal": saved, "target": native, "postgresIdentity": pg_identity,
                                  "workerExitedOrderly": True, "offlineH2Shutdown": True}
            passed("Closed the production engine and H2 cleanly, then took the matched LOCAL/pg_dump backup")

            postgres("UPDATE replica.items SET note='fixture damaged' WHERE id=1; "
                     "UPDATE restore_meta.rm3_sink_job SET stream_seq=stream_seq+99")
            assert target_cut() != native
            restore_target(database_backup, "restore_demo", clean=True)
            assert target_cut() == native and target_identity() == pg_identity
            restored = config(root / "restored")
            assert restored.read_bytes() == live.read_bytes()
            copy_journal(restored, journal_backup)
            assert inspect_journal(restored) == saved
            passed("Transactional pg_restore and cold journal copy reconstitute rows, receipts and original identity")

            postgres("CREATE DATABASE relocated WITH TEMPLATE template0", database="postgres")
            restore_target(database_backup, "relocated")
            relocated_identity = target_identity("relocated")
            assert relocated_identity["systemIdentifier"] == pg_identity["systemIdentifier"]
            assert relocated_identity["databaseOid"] != pg_identity["databaseOid"]
            wrong = config(root / "wrong", "relocated")
            copy_journal(wrong, journal_backup)
            refusal = cli("run", wrong, check=False)
            assert refusal.returncode != 0 and "JOURNAL_IDENTITY_MISMATCH" in refusal.stderr
            assert inspect_journal(wrong) == saved and target_cut("relocated") == native
            manifest["relocatedTargetRefusal"] = {"code": "JOURNAL_IDENTITY_MISMATCH", "identity": relocated_identity}
            passed("A changed target database URL is refused with its restored business rows and target receipts intact")

            before_outage_gtid = mysql("SELECT @@GLOBAL.gtid_executed")
            mysql("""START TRANSACTION;
                UPDATE sales.items SET note='after restore',payload=JSON_OBJECT('n',18446744073709551615) WHERE id=1;
                DELETE FROM sales.items WHERE id=3;
                INSERT INTO sales.items(id,note,payload) VALUES (51,'during outage',JSON_OBJECT('n',51));
                UPDATE sales.duplicates SET value=9 WHERE value=7 LIMIT 1;
                COMMIT;""")
            offline_gtid = gtid_after(before_outage_gtid)
            expected = source_rows()
            assert target_cut() == native
            cli("resume-pipeline", restored, error_generation)
            start(restored)
            wait(lambda: drain_observed(restored, expected, offline_gtid),
                 "replaying the offline MySQL GTID into the same PostgreSQL identity")
            pause_and_stop(restored)
            after = inspect_journal(restored)
            delivered = target_cut()
            assert after["generation"] == saved["generation"] == delivered["generation"]
            assert after["epoch"] == delivered["epoch"] > saved["epoch"]
            assert after["fingerprint"] == saved["fingerprint"] and after["schemaHashes"] == saved["schemaHashes"]
            assert after["snapshots"] == saved["snapshots"] == delivered["snapshots"]
            assert after["sequence"] == after["acknowledged"] == after["normalized"] == delivered["sequence"] == native["sequence"] + 1
            assert delivered["transactionId"] == offline_gtid and delivered["transactionHash"] != native["transactionHash"]
            captured = checked_gtid(after["position"]["payload"]["gtidSet"])
            assert mysql(f"SELECT GTID_SUBSET({checked_gtid(saved['position']['payload']['gtidSet'])},{captured}),"
                         f"GTID_SUBSET({checked_gtid(offline_gtid)},{captured})") == "1\t1"
            assert target_identity() == pg_identity and digest(live_path) == digest(journal_backup)
            assert digest(artifact) == archive_sha
            manifest["restored"] = {"journal": after, "target": delivered, "offlineGtid": offline_gtid,
                                    "postgresIdentity": pg_identity}
            passed("Exact saved MySQL GTID resumes once with original snapshot, generation and PostgreSQL identity")
            manifest["status"] = "PASS"
        finally:
            try:
                stop(verify=False)
            finally:
                result = compose("down", "-v", "--remove-orphans", check=False, timeout=60)
                manifest["cleanupExitCode"] = result.returncode
                if result.returncode:
                    manifest["status"] = "FAIL"
                (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
                if result.returncode:
                    raise RuntimeError("Fixture cleanup failed")

    print("LOCAL_STATE_RESTORE_NATIVE_ACCEPTED artifact=" + archive_sha)


if __name__ == "__main__":
    main()
