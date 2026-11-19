#!/usr/bin/env python3
"""Run the read-only Kafka writer entitlement checks on one disposable TLS/SCRAM broker."""
import argparse
import base64
import hashlib
import os
from pathlib import Path
import re
import secrets
import socket
import subprocess
import tempfile
import time


def run(*args, **options):
    return subprocess.run(args, check=True, text=True, **options)


def provision_tls(home, keytool):
    store = home / 'broker.jks'
    trust = home / 'trust.jks'
    certificate = home / 'broker.pem'
    # Disposable test-only Java key-store passwords. SCRAM uses a separate randomized secret.
    password = 'fixture-keystore-only'
    run(keytool, '-genkeypair', '-alias', 'fixture', '-keyalg', 'RSA', '-keysize', '2048', '-validity', '2',
        '-dname', 'CN=localhost', '-ext', 'SAN=dns:localhost,ip:127.0.0.1', '-storetype', 'JKS',
        '-keystore', str(store), '-storepass', password, '-keypass', password, capture_output=True)
    run(keytool, '-exportcert', '-alias', 'fixture', '-rfc', '-keystore', str(store), '-storepass',
        password, '-file', str(certificate), capture_output=True)
    run(keytool, '-importcert', '-noprompt', '-alias', 'fixture', '-file', str(certificate),
        '-storetype', 'JKS', '-keystore', str(trust), '-storepass', password, capture_output=True)
    store.chmod(0o644)
    (home / 'jaas.conf').write_text('KafkaServer { org.apache.kafka.common.security.scram.ScramLoginModule required; };\n')
    return trust, password


def start(home, port, password, name):
    cluster = base64.urlsafe_b64encode(secrets.token_bytes(16)).decode().rstrip('=')
    settings = {
        'CLUSTER_ID': cluster, 'KAFKA_NODE_ID': '1', 'KAFKA_PROCESS_ROLES': 'broker,controller',
        'KAFKA_LOG_DIRS': '/tmp/kafka-test-data',
        'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@127.0.0.1:9093',
        'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
        'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:SASL_SSL',
        'KAFKA_LISTENERS': 'INTERNAL://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093,EXTERNAL://:9094',
        'KAFKA_ADVERTISED_LISTENERS': f'INTERNAL://127.0.0.1:9092,EXTERNAL://127.0.0.1:{port}',
        'KAFKA_INTER_BROKER_LISTENER_NAME': 'INTERNAL',
        'KAFKA_AUTHORIZER_CLASS_NAME': 'org.apache.kafka.metadata.authorizer.StandardAuthorizer',
        'KAFKA_SUPER_USERS': 'User:ANONYMOUS;User:fixture-admin', 'KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND': 'false',
        'KAFKA_SASL_ENABLED_MECHANISMS': 'SCRAM-SHA-512',
        'KAFKA_SSL_KEYSTORE_LOCATION': '/fixture/broker.jks', 'KAFKA_SSL_KEYSTORE_TYPE': 'JKS',
        'KAFKA_SSL_KEYSTORE_PASSWORD': password, 'KAFKA_SSL_KEY_PASSWORD': password,
        'KAFKA_OPTS': '-Djava.security.auth.login.config=/fixture/jaas.conf',
        'KAFKA_HEAP_OPTS': '-Xms128m -Xmx512m -XX:ActiveProcessorCount=2',
        'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1', 'KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS': '3',
        'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': '1', 'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '1',
        'KAFKA_AUTO_CREATE_TOPICS_ENABLE': 'false',
    }
    command = ['docker', 'run', '--rm', '-d', '--name', name, '--memory', '1280m', '--cpus', '2',
               '-p', f'127.0.0.1:{port}:9094', '-v', f'{home}:/fixture:ro']
    for key, value in settings.items():
        command.extend(['-e', f'{key}={value}'])
    run(*command, 'apache/kafka:3.9.2', capture_output=True)


def provision_users(name, secret):
    configs = ['docker', 'exec', name, '/opt/kafka/bin/kafka-configs.sh', '--bootstrap-server', '127.0.0.1:9092']
    deadline = time.monotonic() + 120
    while True:
        probe = subprocess.run(['docker', 'exec', name, 'bash', '-c', ': </dev/tcp/127.0.0.1/9092'],
                               capture_output=True, timeout=8)
        if probe.returncode == 0:
            break
        if time.monotonic() >= deadline:
            raise RuntimeError('Kafka internal provisioning listener did not become ready')
        time.sleep(2)
    for role in ('admin', 'reviewer', 'writer'):
        try:
            run(*configs, '--alter', '--entity-type', 'users', '--entity-name', f'fixture-{role}',
                '--add-config', f'SCRAM-SHA-512=[iterations=4096,password={secret}-{role}]', capture_output=True, timeout=35)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            raise RuntimeError(f'Could not provision the synthetic {role} SCRAM identity') from None


def main():
    options = argparse.ArgumentParser(description=__doc__)
    options.add_argument('artifact', type=Path)
    mode = options.add_mutually_exclusive_group()
    mode.add_argument('--physical-ownership', action='store_true')
    mode.add_argument('--managed-physical-receipt', action='store_true')
    options.add_argument('--artifact-sha256', help='Require this SHA-256 of the normal application JAR')
    options.add_argument('--java-home', type=Path, default=os.environ.get('JAVA_HOME'), help='Eclipse Temurin 21 JDK')
    options.add_argument('--preflight', action='store_true', help='Compile the fixture without starting Docker')
    args = options.parse_args()
    artifact = args.artifact.resolve(strict=True)
    root = Path(__file__).resolve().parent
    if args.java_home is None:
        options.error('Set JAVA_HOME or --java-home to Eclipse Temurin 21 LTS')
    java_home = args.java_home / 'bin'
    version = run(str(java_home / 'java'), '-version', capture_output=True, timeout=10).stderr
    if 'Temurin' not in version or not re.search(r'version "21(?:[.+"]|$)', version):
        options.error('This fixture requires Eclipse Temurin 21 LTS')
    if args.artifact_sha256:
        digest = hashlib.sha256()
        with artifact.open('rb') as jar:
            for chunk in iter(lambda: jar.read(1024 * 1024), b''):
                digest.update(chunk)
        if digest.hexdigest() != args.artifact_sha256:
            options.error('Artifact SHA-256 differs from --artifact-sha256')
    fixtures = ['KafkaNoticeWriterAcceptance']
    if args.physical_ownership:
        fixtures = ['KafkaPhysicalOwnershipAcceptance']
    if args.managed_physical_receipt:
        fixtures = ['KafkaManagedPhysicalReceiptAcceptance', 'KafkaPhysicalOwnershipAcceptance']
    name = f'rivomesh-kafka-writer-{secrets.token_hex(6)}'
    secret = secrets.token_hex(24)
    with tempfile.TemporaryDirectory(prefix='rivomesh-kafka-writer-') as directory:
        home = Path(directory)
        home.chmod(0o755)
        classes = home / 'classes'
        classes.mkdir()
        run(str(java_home / 'javac'), '-cp', str(artifact), '-d', str(classes),
            *(str(root / 'kafka' / f'{fixture}.java') for fixture in fixtures), timeout=35)
        classpath = os.pathsep.join((str(artifact), str(classes)))
        environment = dict(os.environ, RIVOMESH_KAFKA_FIXTURE_SECRET=secret)
        if args.preflight:
            if args.managed_physical_receipt:
                run(str(java_home / 'java'), '-Xmx384m', '-cp', classpath,
                    f'io.github.rivomesh.cdc.{fixtures[0]}', '--control-preflight', str(home), env=environment, timeout=65)
            print('Fixture compiled against the supplied application. No broker started.', flush=True)
            return
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        trust, password = provision_tls(home, str(java_home / 'keytool'))
        try:
            start(home, port, password, name)
            provision_users(name, secret)
            fixture_arguments = [f'127.0.0.1:{port}']
            if args.managed_physical_receipt:
                fixture_arguments.append(str(home))
            run(str(java_home / 'java'), '-Xmx384m' if args.managed_physical_receipt else '-Xmx256m',
                '-Dorg.slf4j.simpleLogger.defaultLogLevel=off',
                f'-Djavax.net.ssl.trustStore={trust}', f'-Djavax.net.ssl.trustStorePassword={password}',
                '-cp', classpath, f'io.github.rivomesh.cdc.{fixtures[0]}', *fixture_arguments,
                env=environment, timeout=210 if args.managed_physical_receipt else 125)
        except Exception:
            logs = subprocess.run(['docker', 'logs', '--tail', '35', name], capture_output=True, text=True, timeout=10)
            if logs.returncode == 0:
                print((logs.stdout + logs.stderr).replace(secret, '[synthetic-secret]'), flush=True)
            raise
        finally:
            subprocess.run(['docker', 'rm', '-f', name], check=False, capture_output=True, timeout=15)
            remaining = subprocess.run(['docker', 'ps', '-a', '--filter', f'name=^{name}$', '--format', '{{.Names}}'],
                                       check=True, capture_output=True, text=True, timeout=15)
            if remaining.stdout.strip():
                raise RuntimeError('Disposable Kafka fixture was not cleaned up')
            for kind in ('volume','network'):
                leftovers = run('docker', kind, 'ls', '--filter', f'name={name}', '--format', '{{.Name}}',
                                capture_output=True, timeout=15)
                if leftovers.stdout.strip():
                    raise RuntimeError(f'Disposable Kafka fixture {kind} was not cleaned up')
            print('Scoped Kafka fixture cleanup verified.', flush=True)


if __name__ == '__main__':
    main()
