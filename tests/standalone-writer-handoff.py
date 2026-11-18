#!/usr/bin/env python3
"""Prove standalone writer handoff with real MySQL capture and PostgreSQL journal/target."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--previous-artifact", type=Path, help="An older JAR that predates writer placement")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    artifact = args.artifact.resolve()
    previous = args.previous_artifact.resolve() if args.previous_artifact else None
    output = args.output.resolve()
    output.relative_to(ROOT / "target")
    output.mkdir(parents=True, exist_ok=True)
    digest = sha256(artifact)
    report = {"status": "RUNNING", "artifactSha256": digest,
              "harnessSha256": sha256(Path(__file__)), "checks": []}
    if previous is not None:
        report["previousArtifactSha256"] = sha256(previous)
    project = "rivomesh-writer-" + uuid.uuid4().hex[:10]
    env = dict(os.environ, RIVOMESH_WRITER_PASSWORD=secrets.token_urlsafe(24))

    def passed(label):
        print("PASS " + label, flush=True)
        report["checks"].append(label)

    with tempfile.TemporaryDirectory(prefix="writer-", dir=ROOT / "target") as scratch:
        home = Path(scratch)
        secret = "${RIVOMESH_WRITER_PASSWORD}"
        assignment_a, assignment_b, unrelated = (str(uuid.uuid4()) for _ in range(3))
        raw = dict(formatVersion=3, pipelineId="pipeline_writer", databases=["sales", "billing"],
                   source=dict(clusterId="writer-source", host="source", username="root", password=secret,
                               serverId=294223, topology="AUTO"),
                   journal=dict(mode="EXTERNAL", product="POSTGRESQL", namespace="writer_journal",
                                url="jdbc:postgresql://target:5432/writer?connectTimeout=3&socketTimeout=8",
                                username="postgres", password=secret),
                   targets=[dict(id="business", product="POSTGRESQL", metadataSchema="writer_meta",
                                 url="jdbc:postgresql://target:5432/writer?connectTimeout=3&socketTimeout=8",
                                 username="postgres", password=secret, mappings={"sales": "sales_copy"}),
                            dict(id="ledger", product="POSTGRESQL", metadataSchema="writer_ledger_meta",
                                 url="jdbc:postgresql://target:5432/writer?connectTimeout=3&socketTimeout=8",
                                 username="postgres", password=secret, mappings={"billing": "billing_copy"})],
                   leaseMillis=5000, heartbeatMillis=1000, pollIntervalMillis=100, retryDelayMillis=200)
        for node, assignment in (("origin", assignment_a), ("candidate", assignment_b), ("retired", None)):
            config = dict(raw, nodeId=node)
            config["journal"] = dict(raw["journal"])
            if assignment:
                config["journal"]["writerAssignment"] = assignment
            (home / (node + ".yml")).write_text(json.dumps(config))
        runtime = {"image": "eclipse-temurin:21-jre-jammy", "environment": {"RIVOMESH_WRITER_PASSWORD": secret},
                   "entrypoint": ["java", "-Xmx256m", "-jar", "/jar/rivomesh.jar"],
                   "volumes": [str(artifact) + ":/jar/rivomesh.jar:ro", str(home) + ":/fixture:ro"]}
        services = {
            "source": dict(image="mysql:8.4", environment={"MYSQL_ROOT_PASSWORD": secret, "MYSQL_ROOT_HOST": "%"},
                           command=["--server-id=1", "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
                                    "--binlog-format=ROW", "--binlog-row-image=FULL", "--log-replica-updates=ON"],
                           healthcheck=dict(test=["CMD-SHELL", "mysqladmin ping -h 127.0.0.1 --silent"],
                                            interval="2s", timeout="3s", retries=60)),
            "target": dict(image="postgres:17.12", environment={"POSTGRES_DB": "writer", "POSTGRES_PASSWORD": secret},
                           healthcheck=dict(test=["CMD-SHELL", "pg_isready -U postgres -d writer"],
                                            interval="2s", timeout="3s", retries=60)),
            "cli": runtime,
            "origin": dict(runtime, command=["run", "/fixture/origin.yml"]),
            "candidate": dict(runtime, command=["run", "/fixture/candidate.yml"]),
        }
        if previous is not None:
            services["legacy"] = dict(runtime,
                                      volumes=[str(previous) + ":/jar/rivomesh.jar:ro",
                                               str(home) + ":/fixture:ro"])
        composition = home / "compose.json"
        composition.write_text(json.dumps({"services": services}))

        def docker(*args, checked=True):
            return subprocess.run(["docker", "compose", "-p", project, "-f", str(composition), *args],
                                  cwd=ROOT, env=env, check=checked, capture_output=True, text=True, timeout=180)

        def cli(action, node, *parameters, checked=True):
            return docker("run", "--rm", "--no-deps", "cli", action, "/fixture/" + node + ".yml",
                          *map(str, parameters), checked=checked)

        def mysql(statement):
            return docker("exec", "-T", "source", "sh", "-c",
                          'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot -NBe "$1"', "shell", statement).stdout.strip()

        def pg(statement):
            return docker("exec", "-T", "target", "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "postgres",
                          "-d", "writer", "-At", "-c", statement).stdout.strip()

        def await_value(fn, expected, label, seconds=90):
            end = time.monotonic() + seconds
            while time.monotonic() < end:
                try:
                    if fn() == expected:
                        return
                except subprocess.CalledProcessError:
                    pass
                time.sleep(0.4)
            raise AssertionError(label)

        def status():
            return json.loads(cli("status", "origin").stdout)

        def refused(result, code):
            assert result.returncode != 0 and code in result.stderr, code

        def origin_fenced():
            logs = docker("logs", "--no-color", "origin")
            return "PIPELINE_RETRY Fenced" in logs.stdout + logs.stderr

        def synchronized(expected):
            await_value(lambda: pg("SELECT string_agg(id::text||':'||label,',' ORDER BY id) FROM sales_copy.items"),
                        expected, "target rows did not converge")
            await_value(lambda: pg("SELECT bool_and(j.next_seq-1=s.acknowledged) FROM writer_journal.rm3_job j "
                                   "JOIN writer_journal.rm3_subscription s USING(pipeline_id)"), "t", "ACK did not converge")

        try:
            docker("up", "-d", "--wait", "--wait-timeout", "150", "source", "target")
            report["sourceVersion"] = mysql("SELECT VERSION()")
            report["targetVersion"] = pg("SHOW server_version")
            mysql("CREATE DATABASE sales; CREATE TABLE sales.items (id INT PRIMARY KEY, label VARCHAR(32)) "
                  "ENGINE=InnoDB; INSERT INTO sales.items VALUES (1,'snapshot'); "
                  "CREATE DATABASE billing; CREATE TABLE billing.ledger (id INT PRIMARY KEY, label VARCHAR(32)) "
                  "ENGINE=InnoDB; INSERT INTO billing.ledger VALUES (1,'original');")
            enrollment = json.loads(cli("init-writer", "origin").stdout)
            assert enrollment["epoch"] == 0 and enrollment["node"] == "origin"
            generation = enrollment["generation"]
            docker("up", "-d", "origin")
            report["runtimeVersion"] = docker("exec", "-T", "origin", "java", "-version").stderr.splitlines()[0]
            synchronized("1:snapshot")
            mysql("INSERT INTO sales.items VALUES (2,'before');")
            synchronized("1:snapshot,2:before")
            initial = status()
            epoch = initial["epoch"]
            original_snapshot = pg("SELECT snapshot_id FROM writer_meta.rm3_sink_table")
            ledger_snapshot = pg("SELECT snapshot_id FROM writer_ledger_meta.rm3_sink_table")
            assert pg("SELECT label FROM billing_copy.ledger WHERE id=1") == "original"
            assert initial["dataFormat"] == 7 and initial["writerPlacement"] == enrollment
            passed("Explicit enrollment and original live snapshot/CDC on the same journal generation")

            refused(cli("transfer-writer", "candidate", assignment_a, generation, epoch, checked=False), "JOURNAL_BUSY")
            refused(cli("transfer-writer", "candidate", assignment_a, unrelated, epoch, checked=False),
                    "WRITER_ASSIGNMENT_JOURNAL_CHANGED")
            assert status()["writerPlacement"] == enrollment
            passed("Live lease and wrong journal generation cannot change assignment")

            docker("pause", "origin")
            await_value(lambda: pg("SELECT lease_until <= EXTRACT(EPOCH FROM clock_timestamp())*1000 "
                                   "FROM writer_journal.rm3_job"), "t", "original lease did not expire", 15)
            mysql("START TRANSACTION; UPDATE sales.items SET label='during' WHERE id=2; "
                  "INSERT INTO sales.items VALUES(3,'during'); UPDATE billing.ledger SET label='during' WHERE id=1; COMMIT;")
            pg("UPDATE writer_meta.rm3_sink_table SET snapshot_id='00000000-0000-0000-0000-000000000000'")
            refused(cli("transfer-writer", "candidate", assignment_a, generation, epoch, checked=False),
                    "WRITER_ASSIGNMENT_TARGET_SNAPSHOT_CHANGED")
            pg("UPDATE writer_meta.rm3_sink_table SET snapshot_id='" + original_snapshot + "'")
            sink_epoch = pg("SELECT epoch FROM writer_meta.rm3_sink_job")
            pg("UPDATE writer_meta.rm3_sink_job SET epoch=" + str(epoch + 1))
            refused(cli("transfer-writer", "candidate", assignment_a, generation, epoch, checked=False),
                    "WRITER_ASSIGNMENT_TARGET_CHECKPOINT_CHANGED")
            pg("UPDATE writer_meta.rm3_sink_job SET epoch=" + sink_epoch)
            assert status()["writerPlacement"] == enrollment
            placement = json.loads(cli("transfer-writer", "candidate", assignment_a, generation, epoch).stdout)
            assert placement["epoch"] == epoch + 1 and placement["assignment"] == assignment_b
            assert placement["sequence"] == initial["lastSequence"]
            assert len(placement["checkpointSha256"]) == len(placement["evidenceSha256"]) == 64
            assert json.loads(cli("transfer-writer", "candidate", assignment_a, generation, epoch).stdout) == placement
            passed("An inconsistent original snapshot is refused; verified transfer preserves position and retries idempotently")

            docker("unpause", "origin")
            await_value(origin_fenced, True, "former Worker did not observe the fence", 20)
            assert status()["captureLeaseState"] == "UNCLAIMED"
            refused(cli("pause", "retired", checked=False), "JOURNAL_LEASE_LOST")
            if previous is not None:
                refused(docker("run", "--rm", "--no-deps", "legacy", "run", "/fixture/retired.yml", checked=False),
                        "JOURNAL_IDENTITY_MISMATCH")
                passed("An older JAR fails closed before it can obtain a lease on the enrolled journal")
            docker("up", "-d", "candidate")
            synchronized("1:snapshot,2:during,3:during")
            final = status()
            assert final["generation"] == generation and final["epoch"] > placement["epoch"]
            assert final["writerPlacement"] == placement and final["owner"].startswith("candidate-")
            assert pg("SELECT snapshot_id FROM writer_meta.rm3_sink_table") == original_snapshot
            assert pg("SELECT snapshot_id FROM writer_ledger_meta.rm3_sink_table") == ledger_snapshot
            assert pg("SELECT label FROM billing_copy.ledger WHERE id=1") == "during"
            assert json.loads(cli("transfer-writer", "candidate", assignment_a, generation, epoch).stdout) == placement
            mysql("DELETE FROM sales.items WHERE id=1; UPDATE sales.items SET label='after' WHERE id=3;")
            synchronized("2:during,3:after")
            passed("Thawed former Worker cannot claim; replacement reuses the original snapshot and catches up exactly")
            report["receipt"] = placement
            report["runtimeImage"] = runtime["image"]
            assert sha256(artifact) == digest
            report["status"] = "PASS"
        except BaseException:
            report["status"] = "FAIL"
            logs = docker("logs", "--no-color", "--tail", "80", "origin", "candidate", checked=False)
            (output / "runtime.log").write_text(logs.stdout.replace(env["RIVOMESH_WRITER_PASSWORD"], "[REDACTED]"))
            raise
        finally:
            docker("unpause", "origin", checked=False)
            report["cleanupExitCode"] = docker("down", "-v", "--remove-orphans", checked=False).returncode
            if report["cleanupExitCode"] != 0:
                report["status"] = "FAIL"
            (output / "manifest.json").write_text(json.dumps(report, indent=2) + "\n")
    assert report["cleanupExitCode"] == 0


if __name__ == "__main__":
    main()
