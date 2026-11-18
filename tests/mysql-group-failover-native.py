#!/usr/bin/env python3
"""Verify a released Java 21 application against native MySQL single-primary Group Replication."""

import argparse
from copy import deepcopy
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
MEMBERS = ("mysql", "mysql-2", "mysql-3")
GROUP_TABLE = "performance_schema.replication_group_members"


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def gtid_sql(value):
    if not re.fullmatch(r"[a-f0-9:,-]+", value):
        raise AssertionError("Unexpected GTID set")
    return "'" + value + "'"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--java", type=Path, required=True, help="Executable from a Java 21 JDK")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-image", default="mysql:8.4")
    parser.add_argument("--target-image", default="postgres:17")
    args = parser.parse_args()
    jar, java = (path.resolve(strict=True) for path in (args.artifact, args.java))
    runtime = subprocess.run([java, "-version"], capture_output=True, text=True, check=True, timeout=15).stderr
    if not re.search(r'version "21(?:[."]|$)', runtime) or not java.with_name("javac").is_file():
        parser.error("--java must point to a Java 21 JDK")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    project = "rivomesh-v3-group-exact-" + uuid.uuid4().hex[:10]
    group_uuid = str(uuid.uuid4())
    files = [ROOT / "tests" / name for name in ("compose.yml", "postgres-compose.yml", "group-compose.yml")]
    manifest = {
        "status": "FAIL", "checks": [], "artifactSha256": digest(jar), "fixtureSha256": digest(Path(__file__)),
        "runtime": runtime.strip().splitlines(), "sourceGroupUuid": group_uuid,
        "composeFixtureSha256": {path.name: digest(path) for path in files},
    }
    environment = dict(os.environ, SYNC_MYSQL_IMAGE=args.source_image, SYNC_POSTGRES_IMAGE=args.target_image,
                       RM_GRP_ADMIN=secrets.token_hex(24), RM_GRP_CAPTURE=secrets.token_hex(24),
                       RM_GRP_RECOVERY=secrets.token_hex(16), RM_GRP_TARGET=secrets.token_hex(24))
    worker = None

    def passed(label):
        manifest["checks"].append(label)
        print("PASS " + label, flush=True)

    with tempfile.TemporaryDirectory(prefix="rivomesh-group-exact-") as directory:
        root = Path(directory)
        overlay = root / "compose.json"
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            original_host_port = reservation.getsockname()[1]
        mysql_service = {
            "environment": {"MYSQL_ROOT_PASSWORD": "${RM_GRP_ADMIN}", "RM_GRP_CAPTURE": "${RM_GRP_CAPTURE}"},
            "healthcheck": {"test": ["CMD-SHELL", 'MYSQL_PWD="$$MYSQL_ROOT_PASSWORD" '
                                     'mysql -h127.0.0.1 -uroot -e "SELECT 1" >/dev/null 2>&1']},
        }
        overlay.write_text(json.dumps({"services": {
            **{member: dict(mysql_service, ports=[f"127.0.0.1:{original_host_port if member == 'mysql' else ''}:3306"])
               for member in MEMBERS},
            "postgres": {"ports": ["127.0.0.1::5432"], "environment": {"POSTGRES_PASSWORD": "${RM_GRP_TARGET}"},
                         "healthcheck": {"test": ["CMD", "pg_isready", "-h", "127.0.0.1", "-U", "sync_test", "-d", "sync_test"]}},
        }}))
        base = ["docker", "compose", "--env-file", os.devnull, "--project-directory", str(root), "-p", project]
        for file in (*files, overlay):
            base.extend(("-f", str(file)))

        def compose(*command, data=None, check=True, timeout=45):
            result = subprocess.run([*base, *command], cwd=root, env=environment, input=data, text=True,
                                    capture_output=True, timeout=timeout)
            if check and result.returncode:
                context = result.stderr
                for name in ("RM_GRP_ADMIN", "RM_GRP_CAPTURE", "RM_GRP_RECOVERY", "RM_GRP_TARGET"):
                    context = context.replace(environment[name], "[REDACTED]")
                try:
                    result.check_returncode()
                except subprocess.CalledProcessError as error:
                    error.add_note(context.strip()[:1800])
                    raise
            return result

        def mysql(member, statement, role="root", check=True):
            password = 'MYSQL_ROOT_PASSWORD' if role == 'root' else 'RM_GRP_CAPTURE'
            return compose("exec", "-T", member, "sh", "-ec",
                           f'MYSQL_PWD="${password}" mysql -h127.0.0.1 -u{role} -N -B',
                           data=statement, check=check)

        def my(member, statement):
            return mysql(member, statement).stdout.strip()

        def pg(statement):
            return compose("exec", "-T", "postgres", "psql", "-XqAt", "-v", "ON_ERROR_STOP=1",
                           "-U", "sync_test", "-d", "sync_test", data=statement).stdout.strip()

        def wait(check, label, seconds=90):
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline:
                if worker is not None and worker.poll() is not None:
                    diagnostics = re.findall(r"PIPELINE_DIAGNOSTIC ([A-Z_]+ [A-Z0-9_]+)",
                                             (root / "worker.log").read_text(errors="replace"))
                    raise AssertionError(f"Worker exited while {label}: {diagnostics[-5:]}")
                try:
                    result = check()
                except subprocess.CalledProcessError:
                    result = None
                if result:
                    return result
                time.sleep(.35)
            raise AssertionError("Timed out " + label)

        def endpoint(service, container_port):
            address = compose("port", service, str(container_port)).stdout.strip()
            assert address.startswith("127.0.0.1:")
            return {"host": "127.0.0.1", "port": int(address.rsplit(":", 1)[1])}

        def configure_member(member, initialize):
            if initialize:
                my(member, f"""SET SESSION sql_log_bin=0;
                    CREATE USER 'mgr_capture'@'%' IDENTIFIED BY '{environment["RM_GRP_CAPTURE"]}';
                    GRANT SELECT,RELOAD,REPLICATION SLAVE,REPLICATION CLIENT,LOCK TABLES ON *.* TO 'mgr_capture'@'%';
                    CREATE USER 'mgr_recovery'@'%' IDENTIFIED BY '{environment["RM_GRP_RECOVERY"]}';
                    GRANT REPLICATION SLAVE,CONNECTION_ADMIN,BACKUP_ADMIN ON *.* TO 'mgr_recovery'@'%';
                    RESET BINARY LOGS AND GTIDS;
                    CHANGE REPLICATION SOURCE TO SOURCE_USER='mgr_recovery',
                        SOURCE_PASSWORD='{environment["RM_GRP_RECOVERY"]}' FOR CHANNEL 'group_replication_recovery';""")
            seeds = ",".join(node + ":33061" for node in MEMBERS)
            my(member, f"""SET GLOBAL group_replication_group_name='{group_uuid}';
                SET GLOBAL group_replication_local_address='{member}:33061';
                SET GLOBAL group_replication_group_seeds='{seeds}';
                SET GLOBAL group_replication_single_primary_mode=ON;
                SET GLOBAL group_replication_ssl_mode='DISABLED';
                SET GLOBAL group_replication_recovery_use_ssl=OFF;
                SET GLOBAL group_replication_recovery_get_public_key=ON;
                SET GLOBAL group_replication_consistency='BEFORE_ON_PRIMARY_FAILOVER';""")

        def group_view(member):
            return [line.split("\t") for line in my(member, f"SELECT MEMBER_ID,MEMBER_ROLE,MEMBER_STATE FROM {GROUP_TABLE} "
                                                         "WHERE MEMBER_ID<>'' ORDER BY MEMBER_ID").splitlines()]

        def primary(members, expected_online):
            for member in members:
                view = group_view(member)
                admitted = [peer for peer, role, state in view if role == "PRIMARY" and state == "ONLINE"]
                if (len(admitted) == 1 and admitted[0] == server_ids[member]
                        and sum(state == "ONLINE" for _, _, state in view) == expected_online
                        and my(member, "SELECT @@global.read_only,@@global.super_read_only") == "0\t0"):
                    return member
            return None

        def source_rows(member):
            return [
                my(member, "SELECT id,note,derived,JSON_UNQUOTE(JSON_EXTRACT(payload,'$.n')) "
                           "FROM sales.items ORDER BY id").replace("\t", "|"),
                my(member, "SELECT id,amount FROM billing.ledger ORDER BY id").replace("\t", "|"),
            ]

        def target_rows():
            return [pg("SELECT id,note,derived,payload->>'n' FROM mgr_sales.items ORDER BY id"),
                    pg("SELECT id,amount FROM mgr_billing.ledger ORDER BY id")]

        def cut(member, transaction):
            data = pg("""SELECT jsonb_build_object(
                'generation',j.generation,'epoch',j.epoch,'capture',j.captured::jsonb->'payload',
                'sequence',j.next_seq-1,'normalized',j.normalized_seq,'ack',a.acknowledged,
                'targetGeneration',t.generation,'targetSequence',t.stream_seq,
                'targetTransaction',t.transaction_id,'targetHash',t.transaction_hash,
                'journalSnapshots',(SELECT jsonb_agg(jsonb_build_array(table_id,snapshot_id,row_count,run_state)
                    ORDER BY table_id) FROM mgr_journal.rm3_snapshot WHERE pipeline_id=j.pipeline_id),
                'targetSnapshots',(SELECT jsonb_agg(jsonb_build_array(table_id,snapshot_id,row_count,snapshot_state)
                    ORDER BY table_id) FROM mgr_meta.rm3_sink_table WHERE pipeline_id=j.pipeline_id)
              ) FROM mgr_journal.rm3_job j JOIN mgr_journal.rm3_subscription a USING (pipeline_id)
                JOIN mgr_meta.rm3_sink_job t ON t.pipeline_id=j.pipeline_id AND t.subscription_id=a.subscription_id
              WHERE j.pipeline_id='mgr_failover' AND a.subscription_id='pg'""")
            if not data:
                return None
            point = json.loads(data)
            if (point["capture"]["serverUuid"] != server_ids[member] or point["targetTransaction"] != transaction
                    or point["generation"] != point["targetGeneration"] or not point["targetHash"]
                    or point["sequence"] != point["normalized"] or point["sequence"] != point["ack"]
                    or point["sequence"] != point["targetSequence"] or target_rows() != source_rows(member)):
                return None
            assert point["journalSnapshots"] == point["targetSnapshots"]
            assert len(point["journalSnapshots"]) == 2
            assert all(snapshot[2:] == [2, "COMPLETE"] for snapshot in point["journalSnapshots"])
            return point

        def business_commit(member, statement):
            previous = my(member, "SELECT REPLACE(@@global.gtid_executed,CHAR(10),'')")
            my(member, statement)
            transaction = my(member, f"SELECT GTID_SUBTRACT(@@global.gtid_executed,{gtid_sql(previous)})")
            assert re.fullmatch(re.escape(group_uuid) + r":\d+", transaction), transaction
            return transaction

        def preserves(before, after, new_transaction):
            assert before["generation"] == after["generation"]
            assert before["journalSnapshots"] == after["journalSnapshots"]
            assert after["sequence"] > before["sequence"] and after["targetHash"] != before["targetHash"]
            old_gtids, new_gtids = (gtid_sql(point["capture"]["gtidSet"]) for point in (before, after))
            assert my(new_primary, f"SELECT GTID_SUBSET({old_gtids},{new_gtids}),"
                                   f"GTID_SUBSET({gtid_sql(new_transaction)},{new_gtids})") == "1\t1"

        def stop():
            if worker is None or worker.poll() is not None:
                return
            worker.terminate()
            try:
                worker.wait(timeout=20)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait(timeout=10)
                raise AssertionError("Worker failed to terminate")

        try:
            compose("up", "--pull", "never", "-d", "--wait", "--wait-timeout", "150", *MEMBERS, "postgres", timeout=170)
            server_ids = {name: my(name, "SELECT @@global.server_uuid") for name in MEMBERS}
            versions = {name: my(name, "SELECT VERSION()") for name in MEMBERS}
            assert len(set(server_ids.values())) == 3 and len(set(versions.values())) == 1
            mysql_version = versions["mysql"]
            assert mysql_version.startswith("8.4.")
            pg_version = pg("SHOW server_version")
            manifest.update(mysqlVersion=mysql_version, postgresVersion=pg_version, serverUuids=server_ids)
            for name, image in (("mysql", args.source_image), ("postgres", args.target_image)):
                manifest[name + "ImageId"] = subprocess.check_output(
                    ["docker", "image", "inspect", "--format", "{{.Id}}", image], text=True, timeout=10).strip()
            for member in MEMBERS:
                configure_member(member, True)
            my("mysql", "SET GLOBAL group_replication_bootstrap_group=ON; START GROUP_REPLICATION; "
                        "SET GLOBAL group_replication_bootstrap_group=OFF")
            for member in MEMBERS[1:]:
                my(member, "START GROUP_REPLICATION")
            assert wait(lambda: primary(MEMBERS, 3), "starting the three-member group") == "mysql"
            assert all(my(member, "SELECT @@global.read_only,@@global.super_read_only") == "1\t1" for member in MEMBERS[1:])
            denied = mysql("mysql", "SET GLOBAL group_replication_bootstrap_group=@@global.group_replication_bootstrap_group",
                           role="mgr_capture", check=False)
            assert denied.returncode != 0 and "ERROR 1227" in denied.stderr
            passed("Three MySQL group members are ONLINE with one writable primary; capture lacks group administration")
            my("mysql", """CREATE DATABASE sales; CREATE DATABASE billing;
                CREATE TABLE sales.items (id BIGINT PRIMARY KEY,note VARCHAR(50),payload JSON,
                  derived BIGINT GENERATED ALWAYS AS (id*2) VIRTUAL);
                INSERT INTO sales.items(id,note,payload) VALUES (1,'snapshot',JSON_OBJECT('n',1)),(2,'second',JSON_OBJECT('n',2));
                CREATE TABLE billing.ledger(id BIGINT PRIMARY KEY,amount DECIMAL(20,4));
                INSERT INTO billing.ledger VALUES (1,123.4567),(2,234.5678);""")

            target_endpoint = endpoint("postgres", 5432)
            url = f"jdbc:postgresql://127.0.0.1:{target_endpoint['port']}/sync_test?connectTimeout=4&socketTimeout=15"
            source_endpoints = [endpoint(member, 3306) for member in MEMBERS]
            config = {
                "formatVersion": 3, "pipelineId": "mgr_failover", "nodeId": "mgr-fixture",
                "databases": ["sales", "billing"], "chunkSize": 1, "leaseMillis": 5000, "heartbeatMillis": 500,
                "pollIntervalMillis": 200, "retryDelayMillis": 200,
                "source": {"type": "MYSQL", "expectedVersion": mysql_version, "topology": "GROUP_REPLICATION",
                           "clusterId": "mgr-fixture-source", "host": "127.0.0.1", "nodes": source_endpoints,
                           "serverId": 982014, "healthCheckMillis": 750,
                           "username": "mgr_capture", "password": "${RM_GRP_CAPTURE}"},
                "journal": {"mode": "EXTERNAL", "product": "POSTGRESQL", "namespace": "mgr_journal",
                            "url": url, "username": "sync_test", "password": "${RM_GRP_TARGET}"},
                "targets": [{"id": "pg", "product": "POSTGRESQL", "metadataSchema": "mgr_meta", "url": url,
                             "username": "sync_test", "password": "${RM_GRP_TARGET}",
                             "mappings": {"sales": "mgr_sales", "billing": "mgr_billing"}}],
            }
            task = root / "task.yml"
            task.write_text(json.dumps(config))
            task.chmod(0o600)
            config_digest = digest(task)
            with (root / "worker.log").open("wb") as log:
                worker = subprocess.Popen([java, "-Xmx384m", "-jar", jar, "run", task], cwd=root,
                                          env=environment, stdout=log, stderr=subprocess.STDOUT)
            snapshot_rows = source_rows("mysql")
            wait(lambda: target_rows() == snapshot_rows, "finishing both original SQL snapshots")
            original_tx = business_commit("mysql", """START TRANSACTION;
                INSERT INTO sales.items(id,note,payload) VALUES (3,'original primary',JSON_OBJECT('n',3));
                UPDATE billing.ledger SET amount=345.6789 WHERE id=1;
                COMMIT""")
            before = wait(lambda: cut("mysql", original_tx), "draining the original source GTID to PostgreSQL")
            assert all(my(node, f"SELECT WAIT_FOR_EXECUTED_GTID_SET({gtid_sql(before['capture']['gtidSet'])},20)") == "0"
                       for node in MEMBERS[1:])
            manifest["before"] = before
            passed("Original group GTID, source and target snapshots, journal ACK and native PostgreSQL rows agree")

            my("mysql", "SET PERSIST_ONLY read_only=ON; SET PERSIST_ONLY super_read_only=ON")
            compose("kill", "-s", "SIGKILL", "mysql")
            new_primary = wait(lambda: primary(MEMBERS[1:], 2), "waiting for Group Replication to elect a writable survivor")
            assert server_ids[new_primary] != before["capture"]["serverUuid"]
            secondary = next(node for node in MEMBERS[1:] if node != new_primary)
            assert my(secondary, "SELECT @@global.read_only,@@global.super_read_only") == "1\t1"
            manifest["electedMember"] = {"name": new_primary, "uuid": server_ids[new_primary], "view": group_view(new_primary)}
            passed("After primary SIGKILL the surviving majority elects one different writable member")
            elected_tx = business_commit(new_primary, """START TRANSACTION;
                UPDATE sales.items SET note='elected primary',payload=JSON_OBJECT('n',18446744073709551615) WHERE id=1;
                DELETE FROM sales.items WHERE id=3;
                INSERT INTO sales.items(id,note,payload) VALUES (51,'after election',JSON_OBJECT('n',51));
                INSERT INTO billing.ledger VALUES (3,456.0123);
                COMMIT""")
            after = wait(lambda: cut(new_primary, elected_tx), "resuming from the same journal on the elected server")
            preserves(before, after, elected_tx)
            manifest["afterElection"] = after
            passed("Same running Worker automatically delivers the new primary GTID and preserves the original snapshots")

            compose("start", "mysql")
            wait(lambda: my("mysql", "SELECT @@global.server_uuid") == server_ids["mysql"],
                 "bringing up the fenced old member with its original dataset")
            assert endpoint("mysql", 3306) == source_endpoints[0]
            assert my("mysql", "SELECT @@global.read_only,@@global.super_read_only") == "1\t1"
            assert not any(state == "ONLINE" for _, _, state in group_view("mysql"))
            assert my("mysql", f"SELECT GTID_SUBSET({gtid_sql(elected_tx)},@@global.gtid_executed)") == "0"
            blocked = mysql("mysql", "UPDATE sales.items SET note='stale writer' WHERE id=1", check=False)
            assert blocked.returncode != 0 and "ERROR 1290" in blocked.stderr
            stale_config = deepcopy(config)
            stale_config["source"]["nodes"] = [source_endpoints[0]]
            stale = root / "stale.yml"
            stale.write_text(json.dumps(stale_config))
            stale.chmod(0o600)
            probe = root / "VerifyGroupAdmission.java"
            probe.write_text("""import io.github.rivomesh.JdbcConnections;
import io.github.rivomesh.Main;
import io.github.rivomesh.PipelineConfig;
import io.github.rivomesh.SyncFailure;
import java.nio.file.Path;
class VerifyGroupAdmission {
    public static void main(String[] args) throws Exception {
        var config = PipelineConfig.load(Path.of(args[0]));
        var candidate = config.source.nodes.getFirst();
        String jdbcUrl = "jdbc:mysql://" + candidate.host + ":" + candidate.port + "/?" + config.source.jdbcOptions;
        try (var session = JdbcConnections.open("com.mysql.cj.jdbc.Driver", jdbcUrl,
                 config.source.username, config.source.password);
             var statement = session.createStatement();
             var row = statement.executeQuery("SELECT @@global.server_uuid,@@global.read_only,@@global.super_read_only")) {
            if (!row.next() || !args[1].equals(row.getString(1)) || !row.getBoolean(2) || !row.getBoolean(3))
                throw new IllegalStateException("STALE_GROUP_ENDPOINT_IDENTITY_CHANGED");
        }
        try { Main.main(new String[]{"check", args[0]}); }
        catch (SyncFailure refusal) {
            if (!"SOURCE_NO_PRIMARY".equals(refusal.code) && !"SOURCE_REPLICATION_ERROR".equals(refusal.code)) throw refusal;
            System.out.println(refusal.code);
            return;
        }
        throw new IllegalStateException("STALE_GROUP_MEMBER_ADMITTED");
    }
}
""")
            refused = subprocess.run([java, "-cp", jar, probe, stale, server_ids["mysql"]], cwd=root, env=environment,
                                     text=True, capture_output=True, check=True, timeout=40).stdout.strip()
            assert refused in ("SOURCE_NO_PRIMARY", "SOURCE_REPLICATION_ERROR")
            assert cut(new_primary, elected_tx) == after
            manifest["staleMember"] = {"physicalUuid": server_ids["mysql"], "writeRefusalCode": 1290,
                                       "sourceAdmission": refused, "missingGtid": elected_tx,
                                       "samePublishedEndpoint": True, "verifiedJdbcConnection": True}
            passed("Restarted old primary is stale, fenced by MySQL, and refused by the packaged source preflight")

            configure_member("mysql", False)
            my("mysql", "START GROUP_REPLICATION")
            wait(lambda: primary(MEMBERS, 3) == new_primary, "rejoining the old member as a secondary")
            assert [server_ids["mysql"], "SECONDARY", "ONLINE"] in group_view(new_primary)
            assert my("mysql", "SELECT @@global.read_only,@@global.super_read_only") == "1\t1"
            assert my("mysql", f"SELECT WAIT_FOR_EXECUTED_GTID_SET({gtid_sql(elected_tx)},20)") == "0"
            final_tx = business_commit(new_primary, """START TRANSACTION;
                INSERT INTO sales.items(id,note,payload) VALUES (52,'after rejoin',JSON_OBJECT('n',52));
                UPDATE billing.ledger SET amount=567.1234 WHERE id=2;
                COMMIT""")
            final = wait(lambda: cut(new_primary, final_tx), "draining further transactions after old member rejoins")
            preserves(after, final, final_tx)
            assert worker.poll() is None and digest(task) == config_digest and digest(jar) == manifest["artifactSha256"]
            manifest["afterRejoin"] = final
            manifest["workerPid"] = worker.pid
            passed("Old member catches up as read-only secondary and the original Worker continues on the elected primary")
            manifest["status"] = "PASS"
        finally:
            try:
                stop()
            except Exception:
                manifest["status"] = "FAIL"
                raise
            finally:
                result = compose("down", "-v", "--remove-orphans", check=False, timeout=70)
                manifest["cleanupExitCode"] = result.returncode
                if result.returncode:
                    manifest["status"] = "FAIL"
                (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
                if result.returncode:
                    raise RuntimeError("Fixture cleanup failed")
    print("MYSQL_GROUP_FAILOVER_NATIVE_ACCEPTED artifact=" + manifest["artifactSha256"])


if __name__ == "__main__":
    main()
