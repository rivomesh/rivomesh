#!/usr/bin/env python3
"""Verify RocketMQ Controller broker promotion and Rivomesh CDC recovery."""
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
CONTROLLERS = ("rocket-controller-a", "rocket-controller-b", "rocket-controller-c")
TEMPLATE = ROOT / "tests/rocketmq-broker.properties"
SOURCE = ROOT / "tests/rocketmq/RocketControllerPromotionAcceptance.java"
CONSUMER_SOURCE = ROOT / "tests/rocketmq/RocketConsumerReplayAcceptance.java"
CLASS = "io.github.rivomesh.acceptance.RocketControllerPromotionAcceptance"


def run(*command, check=True, timeout=120):
    return subprocess.run(command, check=check, timeout=timeout, capture_output=True, text=True)


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def render(home, args):
    controller_services = CONTROLLERS if args.controller_quorum else ("rocketnamesrv",)
    addresses = [name + ":9877" for name in controller_services]
    peers = ";".join(f"n{index}-{address}" for index, address in enumerate(addresses))
    source = dict(line.split("=", 1) for line in TEMPLATE.read_text().splitlines() if line)
    del source["brokerRole"], source["brokerId"]
    source.update(enableControllerMode="true", controllerAddr=";".join(addresses),
                  enableDLegerCommitLog="false", storePathRootDir=STORE, storePathCommitLog=STORE + "/commitlog",
                  brokerHeartbeatTimeoutMillis="5000", checkSyncStateSetPeriod="1000",
                  syncBrokerMetadataPeriod="1000", allAckInSyncStateSet="true", minInSyncReplicas="2",
                  syncFromLastFile="false", asyncLearner="false")
    controller_volumes = CONTROLLERS if args.controller_quorum else ("controller",)
    volumes = {member: {} for member in (*controller_volumes, *BROKERS)}
    server_limits = {"image": IMAGE, "pull_policy": "never", "mem_limit": "512m",
                     "environment": {"JAVA_OPT_EXT": "-Xms96m -Xmx256m -Xmn64m -XX:ActiveProcessorCount=2"}}
    services = {
        "initialize": {"image": IMAGE, "pull_policy": "never", "user": "0:0",
                       "entrypoint": ["chown", "rocketmq:rocketmq"],
                       "command": ["/mnt/" + member for member in volumes],
                       "volumes": [member + ":/mnt/" + member for member in volumes]},
        "rocketnamesrv": {**server_limits, "command": ["sh", "mqnamesrv"]},
    }
    for index, (service, volume) in enumerate(zip(controller_services, controller_volumes)):
        config = home / (service + ".properties")
        config.write_text("\n".join((
            f"controllerDLegerGroup=rivomesh-fixture-controller", f"controllerDLegerPeers={peers}",
            f"controllerDLegerSelfId=n{index}", "controllerStorePath=" + STORE + "/controller",
            "enableElectUncleanMaster=false", "notifyBrokerRoleChanged=true",
        )) + "\n")
        command = "mqcontroller"
        if not args.controller_quorum:
            with config.open("a") as stream:
                stream.write("listenPort=9876\nenableControllerInNamesrv=true\n")
            command = "mqnamesrv"
        services[service] = {**server_limits,
                             "command": ["sh", command, "-c", "/home/rocketmq/controller.properties"],
                             "volumes": [volume + ":" + STORE,
                                         str(config) + ":/home/rocketmq/controller.properties:ro"]}
    if args.memory_stores:
        for name in BROKERS:
            volumes[name] = {"driver": "local", "driver_opts": {"type": "tmpfs", "device": "tmpfs", "o": "size=128m"}}
        services["store-keeper"] = {
            "image": IMAGE, "pull_policy": "never", "entrypoint": ["sleep", "900"],
            "volumes": [name + ":/mnt/" + name for name in BROKERS],
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
        "volumes": [str(args.artifact) + ":/app.jar:ro", str(home / "classes") + ":/classes:ro",
                    str(home / "markers") + ":/markers"],
        "entrypoint": ["java", "-Xmx192m", "-XX:ActiveProcessorCount=2", "-cp", "/app.jar:/classes", CLASS, "/markers"],
    }
    if args.worker:
        services["tests"]["entrypoint"].append("--worker")
        services["tests"]["mem_limit"] = "768m"
        password = secrets.token_hex(24)
        services["tests"]["environment"] = {"RM_ROCKET_MYSQL_PASSWORD": password}
        services["mysql-source"] = {
            "image": "mysql:8.4", "pull_policy": "never",
            "environment": {"MYSQL_ROOT_PASSWORD": password, "MYSQL_ROOT_HOST": "%"},
            "command": ["--server-id=362", "--log-bin=mysql-bin", "--gtid-mode=ON",
                        "--enforce-gtid-consistency=ON", "--binlog-format=ROW", "--binlog-row-image=FULL",
                        "--log-replica-updates=ON", "--binlog-transaction-compression=OFF"],
        }
        (home / "markers" / "worker.json").write_text(json.dumps({
            "formatVersion": 3, "pipelineId": "rocket_worker", "nodeId": "worker", "databases": ["sales"],
            "source": {"type": "MYSQL", "clusterId": "rocket-worker-source", "host": "mysql-source",
                       "username": "root", "password": "${RM_ROCKET_MYSQL_PASSWORD}", "serverId": 363,
                       "jdbcOptions": "connectTimeout=3000&sslMode=DISABLED&allowPublicKeyRetrieval=true"},
            "journal": {"mode": "LOCAL", "path": "worker-journal"},
            "targets": [{"id": "rocket", "product": "ROCKETMQ", "url": "rocketmq://rocketnamesrv:9876",
                         "mappings": {"sales": "sales_events"},
                         "messaging": {"topic": "worker_events", "cluster": "RivomeshTest",
                                       "broker": "rivomesh-broker", "replicas": 2, "autoCreate": True,
                                       "minimumRetentionHours": 87600,
                                       "rocketmqControllerEndpoints": ["rocketmq://" + address for address in addresses]}}],
            "chunkSize": 1, "heartbeatMillis": 500, "leaseMillis": 6000,
            "pollIntervalMillis": 100, "retryDelayMillis": 500,
        }))
    elif args.product:
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
    run(args.java_home / "bin/javac", "--release", "21", "-cp", args.artifact, "-d", classes, SOURCE, CONSUMER_SOURCE)
    report["testClassSha256"] = sha(classes / (CLASS.replace(".", "/") + ".class"))
    report["consumerSourceSha256"] = sha(CONSUMER_SOURCE)
    compose = ("docker", "compose", "--env-file", "/dev/null", "-p", report["project"], "-f",
               str(render(home, args)))
    runner = None
    log = args.output / "sdk.log"

    def controller_majority_restore(leader, successor, before, controller_pids):
        follower = next(member for member in CONTROLLERS if member not in (leader, successor))
        run(*compose, "kill", "-s", "SIGKILL", follower)
        assert not run(*compose, "ps", "-q", follower).stdout.strip()
        assert run(*compose, "ps", "--services", "--status", "running", *CONTROLLERS).stdout.split() == [successor]
        assert pid(successor) == controller_pids[successor]
        (marks / "controller-majority-lost").touch()
        held = read_cut(marks / "controller-majority-held.json", runner, 155)
        assert held["reachableMinority"] == successor + ":9877" and held["diagnostic"] == "ROCKETMQ_CONTROLLER_ROUTE_RETRY"
        assert held["worker"] == {**before["worker"], "capturedSequence": before["worker"]["sequence"] + 1}
        print("With the Controller majority offline, MySQL capture advances while both broker receipts hold", flush=True)

        original_container = run(*compose, "ps", "-aq", leader).stdout.strip()
        assert original_container
        run(*compose, "start", leader)
        assert run(*compose, "ps", "-q", leader).stdout.strip() == original_container
        assert pid(successor) == controller_pids[successor]
        (marks / "controller-majority-restored").touch()
        resumed = read_cut(marks / "controller-majority-resumed.json", runner, 155)
        assert resumed["leader"] in (leader + ":9877", successor + ":9877")
        assert resumed["worker"]["sequence"] == held["worker"]["capturedSequence"]
        assert resumed["worker"]["epoch"] == before["worker"]["epoch"]
        assert resumed["brokerMaster"] == report["promoted"]["master"]
        assert resumed["brokerEpoch"] == report["promoted"]["epoch"]
        report["controllerMajorityRecovery"] = {"stopped": [leader, follower], "held": held, "resumed": resumed}
        print("Restoring the original Controller store lets the same Worker deliver the queued GTID once", flush=True)
        consumer = read_cut(marks / "consumer-replay.json", runner, 125)
        assert consumer["reopenedOffset"] == before["worker"]["retained"]["queueOffset"] - 2
        assert consumer["committedOffset"] == resumed["worker"]["retained"]["queueOffset"] + 1
        assert consumer["deduplicatedEvents"] == 3 and consumer["committedBusinessTransactions"] == 2
        report["consumerReplay"] = consumer
        print("A fresh native consumer reopens its previous offset and deduplicates the three redelivered records", flush=True)

    def pid(service):
        container = run(*compose, "ps", "-q", service).stdout.strip()
        return int(run("docker", "inspect", "--format", "{{.State.Pid}}", container).stdout)

    try:
        run(*compose, "config", "--quiet")
        if args.memory_stores:
            # Hold the mounts across broker stops so the restarted follower sees its original store.
            run(*compose, "up", "-d", "store-keeper")
        run(*compose, "run", "--rm", "--no-deps", "-T", "initialize")
        run(*compose, "up", "-d", "rocketnamesrv")
        if args.controller_quorum:
            run(*compose, "up", "-d", *CONTROLLERS)
        if args.worker:
            run(*compose, "up", "-d", "mysql-source")
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
            assert promoted["appended"]["queueOffset"] == original["retained"]["queueOffset"] + (3 if args.worker else 1)
            assert promoted["appended"]["commitLogOffset"] > original["retained"]["commitLogOffset"]
            outcome = ("CONTROLLER_PROFILE_WORKER_CONTINUED" if args.worker else "CONTROLLER_PROFILE_PUBLISHED"
                       if args.product else "ROCKETMQ_REPLICATION_PROFILE_REQUIRES_PLAN")
            assert promoted["adapterOutcome"] == outcome
            print("The original surviving replica takes over without operator election and confirms the next position with its follower", flush=True)
            if args.product or args.worker:
                remaining = next(member for member in BROKERS[1:] if member != new_master)
                run(*compose, "kill", "-s", "SIGKILL", remaining)
                assert not run(*compose, "ps", "-q", remaining).stdout.strip()
                (marks / "remaining-follower-killed").touch()
                refusal = read_cut(marks / "minimum-refused.json", runner, 150)
                assert refusal == {"status": "PASS", "confirmed": promoted["appended"]}
                report["minimumRefusal"] = refusal
                print("With one live synchronized replica, the broker queue and durable receipt stay at the previous cut", flush=True)
                if args.worker:
                    run(*compose, "up", "-d", remaining)
                    (marks / "remaining-follower-restarted").touch()
                    recovered = read_cut(marks / "worker-recovered.json", runner, 150)
                    assert recovered["status"] == "PASS" and recovered["sameRunningWorker"]
                    assert recovered["worker"]["sequence"] == promoted["worker"]["sequence"] + 1
                    assert recovered["worker"]["snapshotId"] == original["worker"]["snapshotId"]
                    report["workerRecovered"] = recovered
                    print("The live Worker delivers the retained GTID once after the original follower rejoins", flush=True)
                    if args.controller_quorum:
                        before = read_cut(marks / "controller-ready.json", runner, 60)
                        leader = before["leader"].rsplit(":", 1)[0]
                        assert leader in CONTROLLERS
                        original_controller_pids = {member: pid(member) for member in CONTROLLERS}
                        run(*compose, "kill", "-s", "SIGKILL", leader)
                        assert not run(*compose, "ps", "-q", leader).stdout.strip()
                        (marks / "controller-leader-killed").touch()
                        changed = read_cut(marks / "controller-succeeded.json", runner, 155)
                        successor = changed["leader"].rsplit(":", 1)[0]
                        assert successor in CONTROLLERS and successor != leader
                        assert pid(successor) == original_controller_pids[successor]
                        assert changed["worker"]["sequence"] == recovered["worker"]["sequence"] + 1
                        report["controllerElection"] = {"original": before, "successor": changed}
                        print("The original Worker continues on the same broker after the Controller leader is killed", flush=True)
                        controller_majority_restore(leader, successor, changed, original_controller_pids)
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
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--product", action="store_true", help="Exercise the opt-in Rivomesh publisher through promotion")
    mode.add_argument("--worker", action="store_true", help="Exercise the actual MySQL CDC Worker and durable journal through promotion")
    parser.add_argument("--memory-stores", action="store_true", help="Use retained Linux tmpfs stores for this fixture only")
    parser.add_argument("--controller-quorum", action="store_true", help="Exercise Controller leader and majority loss and restoration in Worker mode")
    args = parser.parse_args()
    if args.controller_quorum and not args.worker:
        parser.error("--controller-quorum requires --worker")
    java = run(args.java_home / "bin/java", "-version").stderr
    if not re.search(r'version "21(?:[."]|$)', java) or "Temurin" not in java:
        parser.error("--java-home must point to a Temurin 21 JDK")
    args.output.mkdir(mode=0o700)
    digest = sha(args.artifact)
    report = {
        "status": "FAIL", "scope": ("Packaged MySQL CDC Worker, original journal and RocketMQ replica recovery" if args.worker
                                    else "Opt-in publisher through Controller promotion and minimum replica refusal" if args.product
                                    else "Broker-native Controller promotion and default Rivomesh adapter refusal"),
        "project": "rivomesh-rocketmq-controller-" + secrets.token_hex(5), "artifactSha256": digest,
        "harnessSha256": sha(Path(__file__)), "testSourceSha256": sha(SOURCE), "brokerTemplateSha256": sha(TEMPLATE),
        "hostJava": java.splitlines(),
        "brokerStore": "KEPT_TMPFS" if args.memory_stores else "DOCKER_VOLUME",
        "controllerCount": 3 if args.controller_quorum else 1,
    }
    try:
        images = (IMAGE, JAVA_IMAGE, "mysql:8.4") if args.worker else (IMAGE, JAVA_IMAGE)
        report["images"] = {image: run("docker", "image", "inspect", image, "--format",
                                       "{{.Id}} {{.Architecture}}").stdout.strip() for image in images}
        with tempfile.TemporaryDirectory(prefix=".fixture-", dir=args.output) as directory:
            acceptance(args, Path(directory), report)
        assert sha(args.artifact) == digest
        assert report["cleanupExitCode"] == 0 and not any(report["remaining"].values())
        report["status"] = "PASS"
    finally:
        (args.output / "manifest.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
