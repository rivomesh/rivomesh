#!/usr/bin/env python3
"""Run Kafka recovery and first-send checks on disposable TLS/SCRAM brokers."""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import hashlib
import os
from pathlib import Path
import re
import secrets
import shutil
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


def start(home, port, password, name, cluster, node=1, voters='1@127.0.0.1:9093', network=None,
          image='apache/kafka:3.9.2', fast_abort_reaper=False):
    internal_host = name if network else '127.0.0.1'
    internal_bind = '' if network else '127.0.0.1'
    replicas = '3' if network else '1'
    settings = {
        'CLUSTER_ID': cluster, 'KAFKA_NODE_ID': str(node), 'KAFKA_PROCESS_ROLES': 'broker,controller',
        'KAFKA_LOG_DIRS': '/tmp/kafka-test-data',
        'KAFKA_CONTROLLER_QUORUM_VOTERS': voters,
        'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
        'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:SASL_SSL',
        'KAFKA_LISTENERS': f'INTERNAL://{internal_bind}:9092,CONTROLLER://{internal_bind}:9093,EXTERNAL://:9094',
        'KAFKA_ADVERTISED_LISTENERS': f'INTERNAL://{internal_host}:9092,EXTERNAL://127.0.0.1:{port}',
        'KAFKA_INTER_BROKER_LISTENER_NAME': 'INTERNAL',
        'KAFKA_AUTHORIZER_CLASS_NAME': 'org.apache.kafka.metadata.authorizer.StandardAuthorizer',
        'KAFKA_SUPER_USERS': 'User:ANONYMOUS;User:fixture-admin', 'KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND': 'false',
        'KAFKA_SASL_ENABLED_MECHANISMS': 'SCRAM-SHA-512',
        'KAFKA_SSL_KEYSTORE_LOCATION': '/fixture/broker.jks', 'KAFKA_SSL_KEYSTORE_TYPE': 'JKS',
        'KAFKA_SSL_KEYSTORE_PASSWORD': password, 'KAFKA_SSL_KEY_PASSWORD': password,
        'KAFKA_OPTS': '-Djava.security.auth.login.config=/fixture/jaas.conf',
        'KAFKA_HEAP_OPTS': '-Xms128m -Xmx' + ('320m' if network else '512m') + ' -XX:ActiveProcessorCount=2',
        'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': replicas, 'KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS': '3',
        'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': replicas,
        'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '2' if network else '1',
        'KAFKA_AUTO_CREATE_TOPICS_ENABLE': 'false',
    }
    if fast_abort_reaper:
        settings['KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS'] = '1000'
    command = (['docker', 'create', '--rm'] if network else ['docker', 'run', '--rm', '-d']) + [
               '--name', name, '--memory', '1024m' if network else '1280m',
               '--cpus', '1.25' if network else '2',
               '-p', f'127.0.0.1:{port}:9094', '-v', f'{home}:/fixture:ro']
    if network:
        command.extend(['--network', network])
    for key, value in settings.items():
        command.extend(['-e', f'{key}={value}'])
    run(*command, image, capture_output=True, timeout=30)


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


def private_worker_java(source, directory, certificate):
    copied = directory / 'temurin'
    shutil.copytree(source, copied, symlinks=True)
    trust = copied / 'lib' / 'security' / 'cacerts'
    if trust.is_symlink():
        original = source / 'lib' / 'security' / 'cacerts'
        trust.unlink()
        shutil.copyfile(original, trust)
    trust.chmod(0o600)
    copied_bin = copied / 'bin'
    run(str(copied_bin / 'keytool'), '-importcert', '-noprompt', '-alias', 'rivomesh-fixture',
        '-keystore', str(trust), '-storepass', 'changeit', '-file', str(certificate), capture_output=True)
    return copied_bin / 'java'


def start_first_send_mysql(directory, name, port, secret):
    fixture = directory / name
    fixture.mkdir(mode=0o700)
    password = fixture / 'root-password'
    schema = fixture / 'schema.sql'
    password.write_text(secret + '-mysql\n')
    schema.write_text(f"""
CREATE DATABASE fixture_sales;
CREATE DATABASE fixture_control CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
CREATE USER 'fixture_source'@'%' IDENTIFIED BY '{secret}-source';
GRANT SELECT ON fixture_sales.* TO 'fixture_source'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'fixture_source'@'%';
GRANT SELECT ON performance_schema.replication_group_members TO 'fixture_source'@'%';
GRANT FLUSH_TABLES ON *.* TO 'fixture_source'@'%';
CREATE USER 'fixture_control'@'%' IDENTIFIED BY '{secret}-control';
GRANT ALL PRIVILEGES ON fixture_control.* TO 'fixture_control'@'%';
CREATE USER 'fixture_agent'@'%' IDENTIFIED BY '{secret}-agent';
GRANT ALL PRIVILEGES ON fixture_control.* TO 'fixture_agent'@'%';
""")
    password.chmod(0o644)
    schema.chmod(0o644)
    run('docker', 'run', '-d', '--rm', '--name', name, '--memory', '1400m', '--cpus', '2',
        '-p', f'127.0.0.1:{port}:3306',
        '-e', 'MYSQL_ROOT_PASSWORD_FILE=/run/fixture-root-password', '-e', 'MYSQL_ROOT_HOST=%',
        '-v', f'{password}:/run/fixture-root-password:ro',
        '-v', f'{schema}:/docker-entrypoint-initdb.d/00-first-send.sql:ro',
        'mysql:8.4', '--server-id=1', '--log-bin=mysql-bin', '--binlog-format=ROW', '--binlog-row-image=FULL',
        '--binlog-transaction-compression=OFF', '--gtid-mode=ON', '--enforce-gtid-consistency=ON',
        '--log-replica-updates=ON', '--innodb-buffer-pool-size=128M', capture_output=True, timeout=90)
    probe = ('docker', 'exec', name, 'sh', '-c',
             'MYSQL_PWD="$(cat /run/fixture-root-password)" mysql --protocol=TCP -h127.0.0.1 '
             '-uroot -N -e "SELECT 1" fixture_sales')
    deadline = time.monotonic() + 135
    while time.monotonic() < deadline:
        try:
            result = subprocess.run(probe, text=True, capture_output=True, timeout=6)
            if result.returncode == 0 and result.stdout.strip() == '1':
                return
        except subprocess.TimeoutExpired:
            pass
        time.sleep(2)
    raise RuntimeError('Disposable MySQL 8.4 did not finish GTID source setup')


def main():
    options = argparse.ArgumentParser(description=__doc__)
    options.add_argument('artifact', type=Path)
    mode = options.add_mutually_exclusive_group()
    mode.add_argument('--physical-ownership', action='store_true')
    mode.add_argument('--managed-physical-receipt', action='store_true')
    mode.add_argument('--managed-first-send', action='store_true', help='Exercise the ordinary Worker with native MySQL and Kafka')
    mode.add_argument('--managed-notice-preflight', action='store_true', help='Use a three-broker TLS/SCRAM control Topic')
    mode.add_argument('--managed-topic-id-ledger', action='store_true', help='Verify the packaged UUID sender with Kafka 4.1.2 RF3')
    mode.add_argument('--managed-notice-dispatch', action='store_true', help='Exercise the approved management UI API and Kafka 4.1.2 RF3 dispatch')
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
    managed = args.managed_physical_receipt or args.managed_notice_preflight or args.managed_notice_dispatch
    if managed:
        fixtures = ['KafkaManagedPhysicalReceiptAcceptance', 'KafkaPhysicalOwnershipAcceptance']
    if args.managed_first_send:
        fixtures = ['KafkaFirstSendAcceptance', 'KafkaPhysicalOwnershipAcceptance']
    if args.managed_topic_id_ledger:
        fixtures = ['KafkaTopicIdLedgerAcceptance', 'KafkaRecoveryNoticeAcceptance', 'KafkaPhysicalOwnershipAcceptance']
    name = f'rivomesh-kafka-writer-{secrets.token_hex(6)}'
    replicated = args.managed_notice_preflight or args.managed_topic_id_ledger or args.managed_notice_dispatch
    names = [f'{name}-{node}' for node in (1,2,3)] if replicated else [name]
    network = name+'-net' if replicated else None
    mysqls = [name+'-mysql-source', name+'-mysql-control'] if args.managed_first_send else []
    secret = secrets.token_hex(24)
    with tempfile.TemporaryDirectory(prefix='rivomesh-kafka-writer-') as directory:
        home = Path(directory).resolve(strict=True)
        home.chmod(0o755)
        classes = home / 'classes'
        classes.mkdir()
        run(str(java_home / 'javac'), '-cp', str(artifact), '-d', str(classes),
            *(str(root / 'kafka' / f'{fixture}.java') for fixture in fixtures), timeout=35)
        classpath = os.pathsep.join((str(artifact), str(classes)))
        environment = dict(os.environ, RIVOMESH_KAFKA_FIXTURE_SECRET=secret)
        if args.preflight:
            if managed:
                run(str(java_home / 'java'), '-Xmx384m', '-cp', classpath,
                    f'io.github.rivomesh.cdc.{fixtures[0]}', '--control-preflight', str(home),
                    *(['--notice-preflight'] if args.managed_notice_preflight else []), env=environment, timeout=65)
            if args.managed_first_send:
                run(str(java_home / 'java'), '-Xmx256m', '-cp', classpath,
                    'io.github.rivomesh.cdc.KafkaFirstSendAcceptance', '--control-preflight', '3306', '3307', str(home),
                    env=environment, timeout=25)
            print('Fixture compiled against the supplied application. No broker started.', flush=True)
            return
        with ExitStack() as reservations:
            sockets = [reservations.enter_context(socket.socket()) for _ in range(len(names) + len(mysqls))]
            for reservation in sockets:
                reservation.bind(('127.0.0.1', 0))
            ports = [reservation.getsockname()[1] for reservation in sockets]
        broker_home = home
        if mysqls:
            broker_home = home / 'broker'
            broker_home.mkdir(mode=0o755)
        trust, password = provision_tls(broker_home, str(java_home / 'keytool'))
        cluster = base64.urlsafe_b64encode(secrets.token_bytes(16)).decode().rstrip('=')
        try:
            if network:
                run('docker', 'network', 'create', network, capture_output=True, timeout=15)
                voters = ','.join(f'{node}@{broker}:9093' for node, broker in enumerate(names, 1))
                for node, (broker, port) in enumerate(zip(names, ports), 1):
                    broker_image = 'apache/kafka:4.1.2' if args.managed_topic_id_ledger or args.managed_notice_dispatch else 'apache/kafka:3.9.2'
                    start(broker_home, port, password, broker, cluster, node, voters, network, image=broker_image,
                          fast_abort_reaper=args.managed_topic_id_ledger)
                run('docker', 'start', *names, capture_output=True, timeout=35)
            else:
                start(broker_home, ports[0], password, name, cluster)
            provision_users(names[0], secret)
            if mysqls:
                with ThreadPoolExecutor(max_workers=2) as pool:
                    starts = [pool.submit(start_first_send_mysql, home, member, port, secret)
                              for member, port in zip(mysqls, ports[-2:])]
                    for started in starts:
                        started.result()
                java = private_worker_java(args.java_home, home, broker_home / 'broker.pem')
                result = subprocess.run([str(java), '-Xmx384m', '-Dorg.slf4j.simpleLogger.defaultLogLevel=off', '-cp',
                                         classpath, 'io.github.rivomesh.cdc.KafkaFirstSendAcceptance',
                                         f'127.0.0.1:{ports[0]}', str(ports[-2]), str(ports[-1]), str(home)],
                                        env=environment, timeout=390, text=True, capture_output=True)
                for output in (result.stdout, result.stderr):
                    lines = output.replace(secret, '[synthetic-secret]').strip().splitlines()
                    if lines:
                        print('\n'.join(line[:300] for line in lines[-40:]), flush=True)
                result.check_returncode()
                return
            fixture_arguments = [','.join(f'127.0.0.1:{port}' for port in ports)]
            if managed or args.managed_topic_id_ledger:
                fixture_arguments.append(str(home))
            if args.managed_notice_preflight:
                fixture_arguments.append('--notice-preflight')
            if args.managed_notice_dispatch:
                fixture_arguments.append('--notice-dispatch')
            run(str(java_home / 'java'), '-Xmx384m' if managed or args.managed_topic_id_ledger else '-Xmx256m',
                '-Dorg.slf4j.simpleLogger.defaultLogLevel=off',
                f'-Djavax.net.ssl.trustStore={trust}', f'-Djavax.net.ssl.trustStorePassword={password}',
                '-cp', classpath, f'io.github.rivomesh.cdc.{fixtures[0]}', *fixture_arguments,
                env=environment, timeout=250 if replicated else 210 if managed else 125)
        except Exception:
            for member in names + mysqls:
                logs = subprocess.run(['docker', 'logs', '--tail', '25', member], capture_output=True, text=True, timeout=10)
                if logs.returncode == 0:
                    print((logs.stdout + logs.stderr).replace(secret, '[synthetic-secret]'), flush=True)
            raise
        finally:
            subprocess.run(['docker', 'rm', '-f', *names, *mysqls],
                           check=False, capture_output=True, timeout=20)
            if network:
                subprocess.run(['docker','network','rm',network],check=False,capture_output=True,timeout=15)
            remaining = subprocess.run(['docker', 'ps', '-a', '--filter', f'name={name}', '--format', '{{.Names}}'],
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
