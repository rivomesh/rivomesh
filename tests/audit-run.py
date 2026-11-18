#!/usr/bin/env python3
"""Actual Java8 release HTTP, audit export and process-kill persistence; no real DB is claimed by this fixture."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import secrets
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import uuid

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("audit_export", ROOT / "tools/audit-export.py")
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


def command(*arguments):
    return subprocess.check_output(arguments, cwd=ROOT, text=True, stderr=subprocess.STDOUT).strip()


def main():
    name = "rivomesh-audit-" + uuid.uuid4().hex[:10]
    relative = Path("target/audit-native") / name
    directory = ROOT / relative
    directory.mkdir(parents=True, mode=0o700)
    artifact = ROOT / "sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar"
    original = hashlib.sha256(artifact.read_bytes()).hexdigest()
    command("python3", "tests/verify-artifact.py")
    tokens = {"RIVOMESH_TOKEN": secrets.token_hex(32), "RIVOMESH_VIEWER_TOKEN": secrets.token_hex(32), "RIVOMESH_OPERATOR_TOKEN": secrets.token_hex(32)}
    environment = dict(os.environ, **tokens)
    created = False
    try:
        args = ["docker", "run", "-d", "--init", "--name", name, "--stop-timeout", "30", "--publish", "127.0.0.1::8080", "--mount", "type=bind,src=" + str(ROOT) + ",dst=/project", "--workdir", "/project", "--env", "RIVOMESH_AUDIT_DIR=/project/" + str(relative)]
        for variable in tokens:
            args.extend(["--env", variable])
        args.extend(["maven:3.9-eclipse-temurin-21", "bash", "-ec", "mkdir -p target/audit-native-classes && javac -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar -d target/audit-native-classes tests/audit/AuditServerAcceptance.java && exec java -Xmx256m -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:target/audit-native-classes io.github.rivomesh.acceptance.AuditServerAcceptance"])
        subprocess.check_output(args, cwd=ROOT, env=environment, text=True, stderr=subprocess.STDOUT)
        created = True
        base = "http://" + command("docker", "port", name, "8080/tcp").splitlines()[0]

        def request(path, token=tokens["RIVOMESH_TOKEN"], body=None, request_id="native_audit_request"):
            headers = {"Authorization": "Bearer " + token}
            if body is not None:
                headers.update({"Content-Type": "application/json", "X-Request-ID": request_id})
            message = Request(base + path, headers=headers, data=None if body is None else json.dumps(body).encode())
            try:
                with urlopen(message, timeout=5) as response:
                    return response.status, json.load(response)
            except HTTPError as error:
                with error:
                    return error.code, json.load(error)

        def ready():
            nonlocal base
            base = "http://" + command("docker", "port", name, "8080/tcp").splitlines()[0]
            end = time.monotonic() + 45
            while time.monotonic() < end:
                try:
                    with urlopen(base + "/healthz", timeout=1) as response:
                        if response.status == 200:
                            return
                except (OSError, URLError):
                    pass
                time.sleep(0.1)
            raise AssertionError("Native audit server did not become healthy")

        ready()
        assert request("/api/session", "invalid-private-token")[0] == 401
        assert request("/api/audit", tokens["RIVOMESH_VIEWER_TOKEN"])[0] == 403
        assert request("/api/audit/export", tokens["RIVOMESH_OPERATOR_TOKEN"])[0] == 403
        task = {"jobId": "native_audit", "source": {"database": "native_audit", "username": "unused", "password": "native-source-private"}, "target": {"url": "jdbc:postgresql://127.0.0.1:1/unused?connectTimeout=1&socketTimeout=1", "type": "POSTGRESQL", "schema": "native_audit", "username": "unused", "password": "native-target-private"}}
        status, body = request("/api/tasks", body={"config": task}, request_id="native_audit_create")
        assert status == 201, (status, body.get("code"))
        hidden = {"jobId": "native_hidden", "source": {"database": "native_hidden", "username": "unused", "serverId": 910002}, "target": {"url": "jdbc:postgresql://127.0.0.1:1/unused?connectTimeout=1&socketTimeout=1", "type": "POSTGRESQL", "schema": "native_hidden", "metadataSchema": "hidden_meta", "username": "unused"}}
        assert request("/api/tasks", body={"config": hidden}, request_id="native_hidden_create")[0] == 201
        operator_tasks = request("/api/tasks", tokens["RIVOMESH_OPERATOR_TOKEN"])[1]["tasks"]
        assert len(operator_tasks) == 1 and operator_tasks[0]["id"] == "native_audit"
        assert request("/api/tasks", tokens["RIVOMESH_VIEWER_TOKEN"])[1]["tasks"] == []
        assert request("/api/tasks/native_hidden", tokens["RIVOMESH_OPERATOR_TOKEN"])[0] == 404
        assert request("/api/tasks/native_hidden/actions", tokens["RIVOMESH_OPERATOR_TOKEN"], {"action": "START", "expectedRevision": "forged"}, "native_hidden_denied")[0] == 404
        assert request("/api/session", tokens["RIVOMESH_OPERATOR_TOKEN"])[1]["taskScope"] == ["native_audit"]
        assert request("/api/tasks/native_audit/actions", tokens["RIVOMESH_OPERATOR_TOKEN"], {"action": "CHECK", "expectedRevision": body["revision"]}, "native_operator_check")[0] == 202
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            operations = request("/api/tasks/native_audit")[1]["operations"]
            if operations and operations[-1]["status"] == "SUCCEEDED":
                break
            time.sleep(0.1)
        else:
            raise AssertionError("Asynchronous CHECK result missing")
        first = directory / "before-kill.jsonl"
        assert audit.export(base, first, tokens["RIVOMESH_TOKEN"], 5) > 5
        records = [json.loads(line) for line in first.read_text().splitlines()]
        events = [record["event"] for record in records if record["type"] == "EVENT"]
        first_ids = {event["id"] for event in events}
        assert any(event.get("httpStatus") == 401 for event in events)
        assert any(event.get("httpStatus") == 403 for event in events)
        assert any(event.get("actorId") == "operator" and event.get("status") == "SUCCEEDED" for event in events)
        for secret in list(tokens.values()) + ["native-source-private", "native-target-private"]:
            assert secret not in first.read_text()
        command("docker", "kill", "--signal", "KILL", name)
        command("docker", "start", name)
        ready()
        assert request("/api/tasks/native_hidden", tokens["RIVOMESH_OPERATOR_TOKEN"])[0] == 404
        second = directory / "after-kill.jsonl"
        audit.export(base, second, tokens["RIVOMESH_TOKEN"], 5)
        restored = [json.loads(line) for line in second.read_text().splitlines()]
        second_ids = {record["event"]["id"] for record in restored if record["type"] == "EVENT"}
        assert first_ids.issubset(second_ids), "Process kill lost committed audit events"
        assert records[0]["storeId"] == restored[0]["storeId"]
        command("docker", "stop", name)
        (directory / "data/audit/events.mv.db").unlink()
        command("docker", "start", name)
        deadline = time.monotonic() + 30
        while command("docker", "inspect", "--format", "{{.State.Running}}", name) == "true" and time.monotonic() < deadline:
            time.sleep(0.2)
        assert command("docker", "inspect", "--format", "{{.State.Running}}", name) == "false", "Deleted audit store was not rejected"
        exit_code = command("docker", "inspect", "--format", "{{.State.ExitCode}}", name)
        assert exit_code != "0"
        assert "AUDIT_STORE_IDENTITY_CHANGED" in command("docker", "logs", name)
        assert original == hashlib.sha256(artifact.read_bytes()).hexdigest(), "Release changed during acceptance"
        print("AUDIT_NATIVE_ACCEPTED: exact Java8 release; authenticated/denied HTTP, actor-attributed scoped asynchronous CHECK, exact task-list/direct-action scope and restart enforcement, multi-page native JSON hash verification/private export, SIGKILL/restart retains committed IDs, deleted audit store refuses silent reset; stub database backend")
        print("ARTIFACT_SHA256: " + original)
        print("EVIDENCE: " + str(relative))
    finally:
        if created:
            logs = command("docker", "logs", name)
            (directory / "server.log").write_text(logs)
            command("docker", "rm", "-f", name)


if __name__ == "__main__":
    main()
