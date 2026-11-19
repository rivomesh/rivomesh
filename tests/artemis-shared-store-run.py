#!/usr/bin/env python3
"""Run the packaged Artemis transport against a native shared-store primary and backup."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import select
import socket
import socketserver
import subprocess
import tempfile
import threading
import time


ROOT = Path(__file__).resolve().parents[1]
IMAGE = "apache/artemis:2.57.0"
STORE = "/var/lib/artemis-instance/data"


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def run(*command, check=True, timeout=120):
    return subprocess.run(command, check=check, timeout=timeout, capture_output=True, text=True)


def until(condition, reason, timeout=100):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if condition():
            return
        time.sleep(0.2)
    raise AssertionError(reason)


def connect(port):
    try:
        return socket.create_connection(("127.0.0.1", port), timeout=0.5)
    except OSError:
        return None


class Gateway(socketserver.ThreadingTCPServer):
    daemon_threads = True

    def __init__(self, target_port):
        super().__init__(("127.0.0.1", 0), ForwardCore)
        self.target_port = target_port
        self.accepting = threading.Event()
        self.accepting.set()


class ForwardCore(socketserver.BaseRequestHandler):
    def handle(self):
        if not self.server.accepting.is_set():
            return
        upstream = connect(self.server.target_port)
        if upstream is None:
            return
        with upstream:
            upstream.settimeout(None)
            try:
                peers = (self.request, upstream)
                while True:
                    ready, _, _ = select.select(peers, (), ())
                    for source in ready:
                        chunk = source.recv(65536)
                        if not chunk:
                            return
                        destination = upstream if source is self.request else self.request
                        destination.sendall(chunk)
            except OSError:
                return


def compose_file(workspace, password, cluster_password):
    base = (ROOT / "tests/artemis-broker.xml").read_text()
    services = {"initialize": {"image": IMAGE, "pull_policy": "never", "user": "0:0",
                               "entrypoint": ["sh", "-c", "chown -R artemis:artemis " + STORE],
                               "volumes": ["journal:" + STORE]}}
    for role, peer, policy in (
            ("primary", "backup", "<primary/>"),
            ("backup", "primary", "<backup><allow-failback>false</allow-failback></backup>")):
        definition = f"""
    <ha-policy><shared-store>{policy}</shared-store></ha-policy>
    <cluster-user>rivo-ha</cluster-user>
    <cluster-password>{cluster_password}</cluster-password>
    <connectors>
      <connector name="self">tcp://{role}:61616</connector>
      <connector name="peer">tcp://{peer}:61616</connector>
    </connectors>
    <cluster-connections><cluster-connection name="same-journal">
      <connector-ref>self</connector-ref>
      <retry-interval>250</retry-interval>
      <message-load-balancing>OFF</message-load-balancing>
      <max-hops>1</max-hops>
      <static-connectors><connector-ref>peer</connector-ref></static-connectors>
    </cluster-connection></cluster-connections>
"""
        assert base.count("    <acceptors>") == 1
        config = workspace / (role + ".xml")
        config.write_text(base.replace("    <acceptors>", definition + "    <acceptors>"))
        services[role] = {
            "image": IMAGE, "pull_policy": "never",
            "environment": {"ARTEMIS_USER": "rivomesh", "ARTEMIS_PASSWORD": password, "ANONYMOUS_LOGIN": "false",
                            "EXTRA_ARGS": "--no-autotune",
                            "JAVA_ARGS_APPEND": "-Xms128m -Xmx512m -XX:MaxDirectMemorySize=128m -XX:ActiveProcessorCount=2"},
            "ports": ["127.0.0.1::61616"],
            "volumes": ["journal:" + STORE, {"type": "bind", "source": str(config),
                        "target": "/var/lib/artemis-instance/etc-override/broker.xml", "read_only": True}],
            "command": ["run"],
        }
    config = workspace / "compose.json"
    config.write_text(json.dumps({"services": services, "volumes": {"journal": {}}}))
    return config


def acceptance(args, output, workspace, report):
    password = secrets.token_urlsafe(18)
    cluster_password = secrets.token_urlsafe(18)
    compose = ("docker", "compose", "-p", report["project"], "-f",
               str(compose_file(workspace, password, cluster_password)))
    markers = workspace / "markers"
    markers.mkdir()
    runner = None

    def port(role):
        return int(run(*compose, "port", role, "61616").stdout.strip().rsplit(":", 1)[1])

    def state_pid(role):
        container = run(*compose, "ps", "-q", role).stdout.strip()
        return int(run("docker", "inspect", "--format", "{{.State.Pid}}", container).stdout)

    def listening(role):
        # Observe the broker listener inside its namespace, beyond Docker's host port proxy.
        command = 'awk \'$2 ~ /:F0B0$/ && $4 == "0A" { found=1 } END { exit !found }\' /proc/net/tcp /proc/net/tcp6'
        return run(*compose, "exec", "-T", role, "sh", "-c", command, check=False).returncode == 0

    def checked(message):
        report["checks"].append(message)
        print(message, flush=True)

    def waypoint(name):
        def present():
            if runner.poll() is not None:
                raise AssertionError("Packaged transport exited before " + name)
            return (markers / name).is_file()
        until(present, "Packaged transport did not complete " + name, timeout=45)

    try:
        run(*compose, "run", "--rm", "-T", "initialize")
        run(*compose, "up", "-d", "primary")
        active_port = port("primary")
        until(lambda: listening("primary"), "The primary broker never opened its CORE acceptor")
        run(*compose, "up", "-d", "backup")
        standby_port = port("backup")
        until(lambda: "AMQ221109" in run(*compose, "logs", "--no-color", "backup").stdout,
              "The backup never entered the shared-store standby")
        standby_pid = state_pid("backup")
        assert standby_pid > 0 and not listening("backup")
        checked("The primary accepts CORE while the original backup process remains passive")

        with Gateway(active_port) as gateway, (workspace / "java.log").open("w") as log:
            thread = threading.Thread(target=gateway.serve_forever, daemon=True)
            thread.start()
            try:
                java = args.java_home / "bin/java"
                classpath = os.pathsep.join(map(str, (args.artifact, args.test_classes)))
                runner = subprocess.Popen([str(java), "-Xmx256m", "-XX:ActiveProcessorCount=2", "-cp", classpath,
                    "io.github.rivomesh.transport.artemis.ArtemisSharedStoreAcceptance",
                    "artemis://127.0.0.1:" + str(gateway.server_address[1]), str(markers)],
                    env=dict(os.environ, RIVOMESH_ARTEMIS_TEST_PASSWORD=password),
                    stdout=log, stderr=log)
                waypoint("primary-checked")
                checked("The original Core event is confirmed and remains readable after consumer ACK")
                gateway.accepting.clear()
                run(*compose, "kill", "-s", "SIGKILL", "primary")
                assert not listening("primary")
                (markers / "gateway-isolated").touch()
                waypoint("outage-checked")
                checked("The isolated publisher retains its previous confirmation")
                until(lambda: listening("backup"), "The original backup never activated")
                assert state_pid("backup") == standby_pid
                gateway.target_port = standby_port
                gateway.accepting.set()
                (markers / "backup-routed").touch()
                assert runner.wait(timeout=60) == 0
                checked("The backup accepts the same endpoint and identity and confirms the exact retry after checking history")
            finally:
                gateway.shutdown()
                thread.join(timeout=5)
    finally:
        if runner is not None and runner.poll() is None:
            runner.kill()
            runner.wait(timeout=10)
        traces = run(*compose, "logs", "--no-color", "--tail", "80", check=False)
        logs = traces.stdout + traces.stderr
        if (workspace / "java.log").is_file():
            logs += (workspace / "java.log").read_text(errors="replace")
        (output / "native-redacted.log").write_text(logs.replace(password, "[REDACTED]").replace(cluster_password, "[REDACTED]"))
        cleanup = run(*compose, "down", "-v", "--remove-orphans", check=False)
        report["cleanupExitCode"] = cleanup.returncode
        label = "label=com.docker.compose.project=" + report["project"]
        report["remaining"] = {
            "containers": len(run("docker", "ps", "-aq", "--filter", label).stdout.split()),
            "volumes": len(run("docker", "volume", "ls", "-q", "--filter", label).stdout.split()),
            "networks": len(run("docker", "network", "ls", "-q", "--filter", label).stdout.split()),
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("artifact", "java-home", "test-classes", "output"):
        parser.add_argument("--" + option, type=lambda value: Path(value).resolve(), required=True)
    args = parser.parse_args()
    args.output.mkdir(mode=0o700)
    image = run("docker", "image", "inspect", IMAGE, "--format", "{{.Id}} {{.Architecture}}").stdout.strip()
    test_class = args.test_classes / "io/github/rivomesh/transport/artemis/ArtemisSharedStoreAcceptance.class"
    test_helper = test_class.with_name("ArtemisTransportAcceptance.class")
    artifact_sha = sha(args.artifact)
    report = {"status": "FAIL", "checks": [], "project": "rivomesh-artemis-ha-" + secrets.token_hex(5),
              "artifactSha256": artifact_sha, "harnessSha256": sha(__file__), "testClassSha256": sha(test_class),
              "testHelperSha256": sha(test_helper), "brokerConfigSha256": sha(ROOT / "tests/artemis-broker.xml"),
              "brokerImage": IMAGE, "brokerImageDetails": image}
    try:
        with tempfile.TemporaryDirectory(prefix=".fixture-", dir=args.output) as directory:
            acceptance(args, args.output, Path(directory), report)
        assert sha(args.artifact) == artifact_sha
        assert report["cleanupExitCode"] == 0 and not any(report["remaining"].values())
        report["status"] = "PASS"
    finally:
        (args.output / "manifest.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
