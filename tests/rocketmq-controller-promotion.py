#!/usr/bin/env python3
"""Observe RocketMQ Controller broker promotion and the existing adapter's unsupported-profile refusal."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import secrets
import subprocess
import tempfile
import time
from contextlib import suppress


ROOT = Path(__file__).resolve().parents[1]
IMAGE = "apache/rocketmq:5.5.0"
JAVA_IMAGE = "maven:3.9-eclipse-temurin-21"
STORE = "/home/rocketmq/store"
BROKERS = ("rocket-primary", "rocket-replica-a", "rocket-replica-b")
TEMPLATE = ROOT / "tests/rocketmq-broker.properties"
SOURCE = ROOT / "tests/rocketmq/RocketControllerPromotionAcceptance.java"
CLASS = "io.github.rivomesh.acceptance.RocketControllerPromotionAcceptance"


def run(*command, check=True, timeout=120):
    return subprocess.run(command, check=check, timeout=timeout, capture_output=True, text=True)


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def render(home, artifact, product=False):
    source = dict(line.split("=", 1) for line in TEMPLATE.read_text().splitlines() if line)
    del source["brokerRole"], source["brokerId"]
    source.update(enableControllerMode="true", controllerAddr="rocketnamesrv:9877",
                  enableDLegerCommitLog="false", storePathRootDir=STORE, storePathCommitLog=STORE + "/commitlog",
                  brokerHeartbeatTimeoutMillis="5000", checkSyncStateSetPeriod="1000",
                  syncBrokerMetadataPeriod="1000", allAckInSyncStateSet="true", minInSyncReplicas="2",
                  syncFromLastFile="false", asyncLearner="false")
    nameserver = home / "nameserver.properties"
    nameserver.write_text("\n".join((
        "listenPort=9876", "enableControllerInNamesrv=true",
        "controllerDLegerGroup=rivomesh-fixture-controller", "controllerDLegerPeers=n0-rocketnamesrv:9877",
        "controllerDLegerSelfId=n0", "controllerStorePath=" + STORE + "/controller",
        "enableElectUncleanMaster=false", "notifyBrokerRoleChanged=true",
    )) + "\n")
    volumes = {member: {} for member in ("controller", *BROKERS)}
    services = {
        "initialize": {"image": IMAGE, "pull_policy": "never", "user": "0:0",
                       "entrypoint": ["chown", "rocketmq:rocketmq"],
                       "command": ["/mnt/" + member for member in volumes],
                       "volumes": [member + ":/mnt/" + member for member in volumes]},
        "rocketnamesrv": {"image": IMAGE, "pull_policy": "never", "mem_limit": "512m",
                          "environment": {"JAVA_OPT_EXT": "-Xms96m -Xmx256m -Xmn64m -XX:ActiveProcessorCount=2"},
                          "command": ["sh", "mqnamesrv", "-c", "/home/rocketmq/nameserver.properties"],
                          "volumes": ["controller:" + STORE,
                                      str(nameserver) + ":/home/rocketmq/nameserver.properties:ro"]},
    }
    for name in BROKERS:
        config = home / (name + ".properties")
        config.write_text("\n".join(f"{key}={value}" for key, value in dict(source, brokerIP1=name).items()) + "\n")
        services[name] = {
            "image": IMAGE, "pull_policy": "never", "mem_limit": "768m",
            "environment": {"JAVA_OPT_EXT": "-Xms128m -Xmx400m -Xmn96m -XX:MaxDirectMemorySize=128m -XX:ActiveProcessorCount=2"},
            "volumes": [name + ":" + STORE, str(config) + ":/home/rocketmq/broker.properties:ro"],
            "command": ["sh", "mqbroker", "-c", "/home/rocketmq/broker.properties"],
        }
    services["tests"] = {
        "image": JAVA_IMAGE, "pull_policy": "never", "mem_limit": "384m",
        "volumes": [str(artifact) + ":/app.jar:ro", str(home / "classes") + ":/classes:ro",
                    str(home / "markers") + ":/markers"],
        "entrypoint": ["java", "-Xmx192m", "-XX:ActiveProcessorCount=2", "-cp", "/app.jar:/classes", CLASS, "/markers"],
    }
    if product:
        services["tests"]["entrypoint"].append("--product")
    config = home / "compose.json"
    config.write_text(json.dumps(dict(services=services, volumes=volumes)))
    return config


def read_cut(path, runner, timeout):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        if path.is_file():
            with suppress(json.JSONDecodeError):
                return json.loads(path.read_text())
        if runner.poll() is not None:
            raise AssertionError("The native SDK runner exited before " + path.name)
        time.sleep(0.25)
    raise AssertionError("No native SDK result at " + path.name)


def acceptance(args, home, report):
    classes, marks = home / "classes", home / "markers"
    classes.mkdir()
    marks.mkdir()
    run(args.java_home / "bin/javac", "--release", "21", "-cp", args.artifact, "-d", classes, SOURCE)
    report["testClassSha256"] = sha(classes / (CLASS.replace(".", "/") + ".class"))
    compose = ("docker", "compose", "--env-file", "/dev/null", "-p", report["project"], "-f",
               str(render(home, args.artifact, args.product)))
    runner = None
    log = args.output / "sdk.log"

    def pid(service):
        container = run(*compose, "ps", "-q", service).stdout.strip()
        return int(run("docker", "inspect", "--format", "{{.State.Pid}}", container).stdout)

    try:
        run(*compose, "config", "--quiet")
        run(*compose, "run", "--rm", "--no-deps", "-T", "initialize")
        run(*compose, "up", "-d", "rocketnamesrv")
        # Elect the distinguished primary before the other two named members join.
        run(*compose, "up", "-d", BROKERS[0])
        with log.open("w") as stream:
            runner = subprocess.Popen([*compose, "run", "--rm", "--no-deps", "-T", "tests"], stdout=stream, stderr=stream)
            ready = read_cut(marks / "leader-ready.json", runner, 140)
            assert ready["master"] == BROKERS[0] + ":10911"
            run(*compose, "up", "-d", *BROKERS[1:])
            original_pids = {member: pid(member) for member in BROKERS}
            assert all(value > 0 for value in original_pids.values())
            original = read_cut(marks / "original-cut.json", runner, 205)
            report["original"] = original
            assert original["master"] == BROKERS[0] + ":10911" and len(original["liveInSync"]) == 3
            assert set(original["topicConfigurationReplicas"]) == set(original["liveInSync"])
            print("Three live synchronized stores agree on the topic configuration, ownership and native retained offset", flush=True)
            run(*compose, "kill", "-s", "SIGKILL", BROKERS[0])
            assert not run(*compose, "ps", "-q", BROKERS[0]).stdout.strip()
            (marks / "original-killed").touch()
            promoted = read_cut(marks / "promoted-cut.json", runner, 170)
            report["promoted"] = promoted
            new_master = promoted["master"].rsplit(":", 1)[0]
            assert new_master in BROKERS[1:] and pid(new_master) == original_pids[new_master]
            assert promoted["epoch"] > original["epoch"] and promoted["masterId"] != original["masterId"]
            assert len(promoted["liveInSync"]) == 2 and promoted["retained"] == original["retained"]
            assert set(promoted["topicConfigurationReplicas"]) == set(promoted["liveInSync"])
            assert promoted["appended"]["queueOffset"] == original["retained"]["queueOffset"] + 1
            assert promoted["appended"]["commitLogOffset"] > original["retained"]["commitLogOffset"]
            assert promoted["adapterOutcome"] == ("CONTROLLER_PROFILE_PUBLISHED" if args.product
                                                  else "ROCKETMQ_REPLICATION_PROFILE_REQUIRES_PLAN")
            print("The original surviving replica takes over without operator election and confirms the next position with its follower", flush=True)
            if args.product:
                remaining = next(member for member in BROKERS[1:] if member != new_master)
                run(*compose, "kill", "-s", "SIGKILL", remaining)
                assert not run(*compose, "ps", "-q", remaining).stdout.strip()
                (marks / "remaining-follower-killed").touch()
                refusal = read_cut(marks / "minimum-refused.json", runner, 150)
                assert refusal == {"status": "PASS", "confirmed": promoted["appended"]}
                report["minimumRefusal"] = refusal
                print("The opt-in publisher retains its saved receipt and makes no write with only one live synchronized replica", flush=True)
            else:
                print("The current Rivomesh adapter without opt-in refuses Controller mode before and after promotion", flush=True)
            assert runner.wait(timeout=25) == 0
    finally:
        traces = run(*compose, "logs", "--no-color", "--tail", "90", check=False)
        (args.output / "brokers.log").write_text(traces.stdout + traces.stderr)
        if runner is not None and runner.poll() is None:
            runner.terminate()
        cleaned = run(*compose, "down", "-v", "--remove-orphans", check=False, timeout=100)
        report["cleanupExitCode"] = cleaned.returncode
        if runner is not None and runner.poll() is None:
            try:
                runner.wait(timeout=5)
            except subprocess.TimeoutExpired:
                runner.kill()
                runner.wait(timeout=10)
        label = "label=com.docker.compose.project=" + report["project"]
        report["remaining"] = {
            "containers": len(run("docker", "ps", "-aq", "--filter", label).stdout.split()),
            "volumes": len(run("docker", "volume", "ls", "-q", "--filter", label).stdout.split()),
            "networks": len(run("docker", "network", "ls", "-q", "--filter", label).stdout.split()),
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("artifact", "java-home", "output"):
        parser.add_argument("--" + key, required=True, type=lambda value: Path(value).resolve())
    parser.add_argument("--product", action="store_true", help="Exercise the opt-in Rivomesh publisher through promotion")
    args = parser.parse_args()
    java = run(args.java_home / "bin/java", "-version").stderr
    if not re.search(r'version "21(?:[."]|$)', java) or "Temurin" not in java:
        parser.error("--java-home must point to a Temurin 21 JDK")
    args.output.mkdir(mode=0o700)
    digest = sha(args.artifact)
    report = {
        "status": "FAIL", "scope": ("Opt-in publisher through Controller promotion and minimum replica refusal" if args.product
                                    else "Broker-native Controller promotion and default Rivomesh adapter refusal"),
        "project": "rivomesh-rocketmq-controller-" + secrets.token_hex(5), "artifactSha256": digest,
        "harnessSha256": sha(Path(__file__)), "testSourceSha256": sha(SOURCE), "brokerTemplateSha256": sha(TEMPLATE),
        "hostJava": java.splitlines(),
    }
    try:
        report["images"] = {image: run("docker", "image", "inspect", image, "--format",
                                       "{{.Id}} {{.Architecture}}").stdout.strip() for image in (IMAGE, JAVA_IMAGE)}
        with tempfile.TemporaryDirectory(prefix=".fixture-", dir=args.output) as directory:
            acceptance(args, Path(directory), report)
        assert sha(args.artifact) == digest
        assert report["cleanupExitCode"] == 0 and not any(report["remaining"].values())
        report["status"] = "PASS"
    finally:
        (args.output / "manifest.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
