#!/usr/bin/env python3
"""Check the packaged Classic OpenWire publisher against a native shared KahaDB pair."""
import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import select
import socket
import socketserver
import subprocess
import tempfile
import threading
import time


ROOT = Path(__file__).resolve().parents[1]
IMAGE = "apache/activemq:5.19.12"
INSTALL = "/opt/apache-activemq"
LOCK_WAIT = "is now in slave mode waiting a lock to be acquired"
SOURCE = ROOT / "tests/classic/ClassicSharedStoreAcceptance.java"
CLASS = "io.github.rivomesh.acceptance.ClassicSharedStoreAcceptance"
TEMPLATES = tuple(ROOT / "tests" / name for name in
                  ("classic-broker.xml", "classic-jetty-realm.properties", "classic-jolokia-access.xml"))


def run(*command, check=True, timeout=120):
    return subprocess.run(command, check=check, timeout=timeout, capture_output=True, text=True)


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def until(condition, reason, timeout=75):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if condition():
            return
        time.sleep(0.25)
    raise AssertionError(reason)


class Gateway(socketserver.ThreadingTCPServer):
    daemon_threads = True

    def __init__(self, routes, index):
        super().__init__(("127.0.0.1", 0), Forward)
        self.routes = routes
        self.index = index


class Forward(socketserver.BaseRequestHandler):
    def handle(self):
        ports = self.server.routes["ports"]
        if ports is None:
            return
        try:
            with socket.create_connection(("127.0.0.1", ports[self.server.index]), timeout=2) as upstream:
                upstream.settimeout(None)
                peers = (self.request, upstream)
                while True:
                    ready, _, _ = select.select(peers, (), ())
                    for source in ready:
                        chunk = source.recv(65536)
                        if not chunk:
                            return
                        target = upstream if source is self.request else self.request
                        target.sendall(chunk)
        except OSError:
            return


def compose_file(workspace, publisher, reader, management_port):
    broker, realm, jolokia = (path.read_text() for path in TEMPLATES)
    assert broker.count("Classic@Test123") == broker.count("Consumer@Test123") == realm.count("Classic@Test123") == 1
    broker = broker.replace("Classic@Test123", publisher).replace("Consumer@Test123", reader)
    # Keep each broker's temporary files local and share exactly the KahaDB directory.
    marker = 'checkForCorruptJournalFiles="true"/>'
    assert broker.count(marker) == 1
    broker = broker.replace(marker, """checkForCorruptJournalFiles="true" lockKeepAlivePeriod="1500">
      <locker><shared-file-locker lockAcquireSleepInterval="750"/></locker>
    </kahaDB>""")
    realm = realm.replace("Classic@Test123", publisher)
    assert jolokia.count("http://classic:8161") == 2
    jolokia = jolokia.replace("http://classic:8161", f"http://127.0.0.1:{management_port}")

    files = dict(zip((path.name for path in TEMPLATES), (broker, realm, jolokia)))
    store = INSTALL + "/data/kahadb"
    volumes = ["journal:" + store]
    for name, contents in files.items():
        path = workspace / name
        path.write_text(contents)
        volumes.append({"type": "bind", "source": str(path), "target": INSTALL + "/conf/" +
                        (name.removeprefix("classic-") if name != "classic-broker.xml" else "activemq.xml"),
                        "read_only": True})
    definition = {"image": IMAGE, "pull_policy": "never", "mem_limit": "768m",
                  "environment": {"ACTIVEMQ_OPTS_MEMORY": "-Xms64m -Xmx384m -XX:ActiveProcessorCount=2"},
                  "ports": ["127.0.0.1::61616", "127.0.0.1::8161"], "volumes": volumes}
    initialize = {"image": IMAGE, "pull_policy": "never", "user": "0:0",
                  "entrypoint": ["chown", "activemq:activemq", store], "command": [],
                  "volumes": ["journal:" + store]}
    config = workspace / "compose.json"
    config.write_text(json.dumps({"services": {"initialize": initialize, "primary": definition, "standby": definition},
                                  "volumes": {"journal": {}}}))
    return config


def accept(args, workspace, report):
    publisher, reader = (secrets.token_urlsafe(24) for _ in range(2))
    markers, classes = (workspace / folder for folder in ("markers", "classes"))
    markers.mkdir()
    classes.mkdir()
    run(args.java_home / "bin/javac", "--release", "21", "-cp", args.artifact, "-d", classes, SOURCE)
    report["testClassSha256"] = sha(classes / (CLASS.replace(".", "/") + ".class"))
    routes = {"ports": None}
    runner = None

    def checked(message):
        report["checks"].append(message)
        print(message, flush=True)

    with Gateway(routes, 0) as wire, Gateway(routes, 1) as management:
        compose = ("docker", "compose", "--env-file", "/dev/null", "-p", report["project"], "-f",
                   str(compose_file(workspace, publisher, reader, management.server_address[1])))

        def port(role, container_port):
            return int(run(*compose, "port", role, str(container_port)).stdout.strip().rsplit(":", 1)[-1])

        def selected(role):
            return tuple(port(role, item) for item in (61616, 8161))

        def listening(role, container_port):
            marker = f"{container_port:04X}"
            scan = ("awk '$2 ~ /:" + marker + "$/ && $4 == \"0A\" { found=1 } "
                    "END { exit !found }' /proc/net/tcp /proc/net/tcp6")
            return run(*compose, "exec", "-T", role, "sh", "-c", scan, check=False).returncode == 0

        def pid(role):
            container = run(*compose, "ps", "-q", role).stdout.strip()
            return int(run("docker", "inspect", "--format", "{{.State.Pid}}", container).stdout)

        def waits_for_lock(role):
            observed = run(*compose, "logs", "--since", report[role + "StartedAt"], "--no-color", role)
            return LOCK_WAIT in observed.stdout + observed.stderr

        def startup(role, action):
            report[role + "StartedAt"] = datetime.now(timezone.utc).isoformat()
            run(*compose, *action, role)

        def wait_marker(name):
            def reached():
                if runner.poll() is not None:
                    raise AssertionError("Packaged Classic publisher exited before " + name)
                return (markers / name).is_file()
            until(reached, "Packaged Classic publisher did not complete " + name)

        threads = []
        log_path = workspace / "publisher.log"
        try:
            run(*compose, "config", "--quiet")
            run(*compose, "run", "--rm", "--no-deps", "-T", "initialize")
            startup("primary", ("up", "-d"))
            until(lambda: listening("primary", 61616) and listening("primary", 8161),
                  "The primary broker did not open both listeners")
            active_ports = selected("primary")
            startup("standby", ("up", "-d"))
            until(lambda: waits_for_lock("standby"), "The standby did not wait for the KahaDB lock")
            standby_pid = pid("standby")
            assert standby_pid > 0 and not listening("standby", 61616)
            checked("The primary holds the KahaDB lock and the live standby cannot accept OpenWire")

            routes["ports"] = active_ports
            for gateway in (wire, management):
                thread = threading.Thread(target=gateway.serve_forever, daemon=True)
                thread.start()
                threads.append((gateway, thread))
            classpath = os.pathsep.join(map(str, (args.artifact, classes)))
            with log_path.open("w") as log:
                runner = subprocess.Popen(
                    [args.java_home / "bin/java", "-Xmx192m", "-XX:ActiveProcessorCount=2", "-cp", classpath,
                     CLASS, str(wire.server_address[1]), str(management.server_address[1]), markers],
                    env=dict(os.environ, RIVO_CLASSIC_TEST_PUBLISHER=publisher, RIVO_CLASSIC_TEST_READER=reader),
                    stdout=log, stderr=log)
                wait_marker("primary-confirmed")
                checked("The packaged publisher confirms native receipts and selected EVENT consumption retains ownership")
                routes["ports"] = None
                run(*compose, "kill", "-s", "SIGKILL", "primary")
                assert not listening("primary", 61616)
                (markers / "gateway-isolated").touch()
                wait_marker("outage-confirmed")
                checked("The isolated publication fails retryably and retains the prior confirmation")
                until(lambda: listening("standby", 61616) and listening("standby", 8161),
                      "The existing standby did not open the OpenWire and Jolokia listeners")
                assert pid("standby") == standby_pid
                routes["ports"] = selected("standby")
                (markers / "standby-routed").touch()
                wait_marker("standby-confirmed")
                checked("The original standby validates the same owner and native receipt and confirms the exact retry at the unchanged endpoints")
                startup("primary", ("start",))
                until(lambda: waits_for_lock("primary"), "The restarted primary did not enter KahaDB standby")
                assert pid("primary") > 0 and not listening("primary", 61616)
                assert pid("standby") == standby_pid and listening("standby", 61616)
                (markers / "old-primary-waiting").touch()
                assert runner.wait(timeout=45) == 0, "Packaged Classic publisher did not complete the final check"
                checked("The restarted former primary stays passive and the active broker confirms another exact EVENT and receipt")
        finally:
            if runner is not None and runner.poll() is None:
                runner.kill()
                runner.wait(timeout=10)
            for gateway, thread in threads:
                gateway.shutdown()
                thread.join(timeout=5)
            logs = run(*compose, "logs", "--no-color", "--tail", "80", check=False)
            content = logs.stdout + logs.stderr
            if log_path.is_file():
                content += log_path.read_text(errors="replace")
            basic = base64.b64encode(("admin:" + publisher).encode()).decode()
            for value in (publisher, reader, basic):
                content = content.replace(value, "[REDACTED]")
            (args.output / "native-redacted.log").write_text(content)
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
    for option in ("artifact", "java-home", "output"):
        parser.add_argument("--" + option, type=lambda value: Path(value).resolve(), required=True)
    args = parser.parse_args()
    version = run(args.java_home / "bin/java", "-version").stderr
    if not re.search(r'version "21(?:[."]|$)', version) or "Temurin" not in version:
        parser.error("--java-home must identify a Temurin 21 JDK")
    args.output.mkdir(mode=0o700)
    artifact_sha = sha(args.artifact)
    report = {"status": "FAIL", "checks": [], "project": "rivomesh-classic-ha-" + secrets.token_hex(5),
              "artifactSha256": artifact_sha, "harnessSha256": sha(Path(__file__)), "testSourceSha256": sha(SOURCE),
              "fixtureSources": {path.name: sha(path) for path in TEMPLATES},
              "brokerImage": IMAGE, "javaVersion": version.splitlines()}
    try:
        report["brokerImageDetails"] = run("docker", "image", "inspect", IMAGE, "--format",
                                          "{{.Id}} {{.Architecture}}").stdout.strip()
        with tempfile.TemporaryDirectory(prefix=".fixture-", dir=args.output) as directory:
            accept(args, Path(directory), report)
        assert sha(args.artifact) == artifact_sha
        assert report["cleanupExitCode"] == 0 and not any(report["remaining"].values())
        report["status"] = "PASS"
    finally:
        (args.output / "manifest.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
