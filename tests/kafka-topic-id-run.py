#!/usr/bin/env python3
"""Verify the Kafka 4.1.2 Topic-ID protocol in a disposable standalone broker."""
import argparse
import base64
import hashlib
import os
from pathlib import Path
import re
import runpy
import secrets
import socket
import subprocess
from tempfile import TemporaryDirectory

CLIENT_SHA256 = "afe861c05067f4018a3148d73c1ed1e5fc90808757c15b043527d7e535a5d431"
SLF4J_SHA256 = "d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0"
BROKER_IMAGE = "apache/kafka:4.1.2"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java-home", required=True, type=Path)
    parser.add_argument("--kafka-client", required=True, type=Path, help="Official kafka-clients 4.1.2 JAR")
    parser.add_argument("--slf4j-api", required=True, type=Path, help="Official slf4j-api 1.7.36 JAR")
    args = parser.parse_args()
    java_home, client, slf4j = (path.resolve(strict=True) for path in (args.java_home, args.kafka_client, args.slf4j_api))
    for path, expected in ((client, CLIENT_SHA256), (slf4j, SLF4J_SHA256)):
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            parser.error(f"Unexpected test dependency SHA-256 for {path.name}")
    version = subprocess.run([java_home / "bin/java", "-version"], check=True, capture_output=True, text=True, timeout=10).stderr
    if "Temurin" not in version or not re.search(r'version "21(?:[.+"]|$)', version):
        parser.error("Use Eclipse Temurin 21 LTS")
    tests = Path(__file__).resolve().parent
    helpers = runpy.run_path(str(tests / "kafka-notice-writer-run.py"))
    subprocess.run(["docker", "image", "inspect", BROKER_IMAGE], check=True, capture_output=True, timeout=12)
    secret = secrets.token_hex(24)
    name = "rivomesh-kafka-topic-id-" + secrets.token_hex(6)
    cluster = base64.urlsafe_b64encode(secrets.token_bytes(16)).decode().rstrip("=")
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]

    with TemporaryDirectory(prefix="rivomesh-kafka-topic-id-") as folder:
        home = Path(folder).resolve(strict=True)
        home.chmod(0o755)
        classes = home / "classes"
        classes.mkdir()
        identity = os.pathsep.join(map(str, (client, slf4j)))
        subprocess.run([java_home / "bin/javac", "--release", "21", "-cp", identity, "-d", classes,
                        tests / "kafka/TopicIdProducerAcceptance.java"], check=True, timeout=30)
        trust, password = helpers["provision_tls"](home, str(java_home / "bin/keytool"))
        try:
            helpers["start"](home, port, password, name, cluster, image=BROKER_IMAGE)
            helpers["provision_users"](name, secret)
            child = subprocess.run(
                [java_home / "bin/java", "-Xmx256m", f"-Djavax.net.ssl.trustStore={trust}",
                 f"-Djavax.net.ssl.trustStorePassword={password}", "-cp", f"{classes}{os.pathsep}{identity}",
                 "org.apache.kafka.clients.producer.TopicIdProducerAcceptance", f"127.0.0.1:{port}"],
                env=dict(os.environ, RIVOMESH_KAFKA_FIXTURE_SECRET=secret), capture_output=True, text=True, timeout=145,
            )
            for output in (child.stdout, child.stderr):
                if output:
                    print(output.replace(secret, "[synthetic-secret]").strip(), flush=True)
            child.check_returncode()
        finally:
            subprocess.run(["docker", "rm", "-f", "-v", name], check=False, capture_output=True, timeout=20)
            for command in (("ps", "-aq"), ("volume", "ls", "-q"), ("network", "ls", "-q")):
                remaining = subprocess.check_output(["docker", *command, "--filter", f"name={name}"], text=True, timeout=15)
                if remaining.strip():
                    raise RuntimeError("The disposable Kafka fixture was not cleaned up")
            print("Scoped Kafka 4.1.2 fixture cleanup verified.", flush=True)


if __name__ == "__main__":
    main()
