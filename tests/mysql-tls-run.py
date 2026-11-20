#!/usr/bin/env python3
"""Verify MySQL 8.4 snapshot, binlog, and target JDBC TLS on the packaged Java 21 Worker."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parents[1]
MYSQL_IMAGE = 'mysql:8.4'
JAVA_IMAGE = 'eclipse-temurin:21-jre-jammy'
JDBC = 'sslMode=VERIFY_IDENTITY&tlsVersions=TLSv1.3&serverTimezone=UTC&connectTimeout=5000&socketTimeout=15000'
LOCAL_HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def certificates(directory, java_home):
    trust = directory / 'trust'
    trust.mkdir()

    def openssl(*args):
        subprocess.run(['openssl', *args], cwd=directory, check=True, capture_output=True, timeout=40)

    openssl('req', '-x509', '-new', '-nodes', '-newkey', 'rsa:2048', '-days', '2', '-sha256',
            '-subj', '/CN=Rivomesh temporary fixture CA',
            '-addext', 'basicConstraints=critical,CA:TRUE',
            '-addext', 'keyUsage=critical,keyCertSign,cRLSign',
            '-keyout', 'root.key', '-out', 'root.pem')
    for endpoint in ('source', 'target'):
        endpoint_dir = directory / endpoint
        endpoint_dir.mkdir()
        openssl('req', '-new', '-nodes', '-newkey', 'rsa:2048', '-sha256', '-subj', '/CN='+endpoint,
                '-keyout', endpoint+'/server.key', '-out', endpoint+'.csr')
        extension = directory / (endpoint + '.cnf')
        extension.write_text(f'basicConstraints=critical,CA:FALSE\nsubjectAltName=DNS:{endpoint}\n'
                             'keyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n')
        openssl('x509', '-req', '-sha256', '-days', '2', '-in', endpoint+'.csr', '-extfile', extension.name,
                '-CA', 'root.pem', '-CAkey', 'root.key', '-CAcreateserial', '-out', endpoint+'/server.crt')
        (endpoint_dir/'server.key').chmod(0o600)
        (endpoint_dir/'ca.pem').write_bytes((directory/'root.pem').read_bytes())
    subprocess.run([str(java_home/'bin/keytool'), '-importcert', '-noprompt', '-storetype', 'JKS',
                    '-keystore', str(trust/'cacerts'), '-storepass', 'changeit', '-alias', 'fixture-ca',
                    '-file', str(directory/'root.pem')],
                   check=True, capture_output=True, timeout=30)
    return trust


def mysql_service(directory, server_id):
    command = [
        'mysqld', '--server-id='+str(server_id), '--log-bin=mysql-bin', '--gtid-mode=ON',
        '--enforce-gtid-consistency=ON', '--log-replica-updates=ON', '--binlog-format=ROW',
        '--binlog-row-image=FULL', '--binlog-transaction-compression=OFF',
        '--require-secure-transport=ON', '--tls-version=TLSv1.3',
        '--ssl-ca=/run/rivomesh-tls/ca.pem', '--ssl-cert=/run/rivomesh-tls/server.crt',
        '--ssl-key=/run/rivomesh-tls/server.key',
    ]
    entrypoint = (
        'install -d -o mysql -g mysql -m 0700 /run/rivomesh-tls\n'
        'install -o mysql -g mysql -m 0600 /tls-fixture/server.key /run/rivomesh-tls/server.key\n'
        'install -o mysql -g mysql -m 0644 /tls-fixture/server.crt /run/rivomesh-tls/server.crt\n'
        'install -o mysql -g mysql -m 0644 /tls-fixture/ca.pem /run/rivomesh-tls/ca.pem\n'
        'exec docker-entrypoint.sh "$@"'
    )
    return {'image': MYSQL_IMAGE, 'command': command, 'entrypoint': ['bash', '-ceu', entrypoint, 'fixture'],
            'environment': {'MYSQL_ROOT_PASSWORD': '${RM_TLS_PASSWORD}', 'MYSQL_ROOT_HOST': '%'},
            'volumes': [str(directory)+':/tls-fixture:ro']}


def whole_digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def wait_for(check, label, seconds=100):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (subprocess.CalledProcessError, OSError, urllib.error.URLError):
            pass
        time.sleep(.4)
    raise AssertionError('Timed out: ' + label)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact', type=Path, required=True)
    parser.add_argument('--java-home', type=Path, required=True, help='Host Temurin 21 for fixture trust store generation')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--external-mysql-driver', type=Path, help='Supply Connector/J separately on the worker classpath')
    args = parser.parse_args()
    artifact = args.artifact.resolve()
    external = args.external_mysql_driver.resolve(strict=True) if args.external_mysql_driver else None
    launch = ['-jar', '/app.jar']
    if external:
        marker = 'com/mysql/cj/jdbc/Driver.class'
        with ZipFile(artifact) as app, ZipFile(external) as driver:
            assert marker not in app.namelist() and marker in driver.namelist()
        launch = ['-cp', '/app.jar:/operator/mysql-driver.jar', 'io.github.rivomesh.Main']
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    project = 'rivomesh-mysql-tls-' + uuid.uuid4().hex[:10]
    environment = dict(os.environ, RM_TLS_PASSWORD=secrets.token_hex(25), RM_TLS_TOKEN=secrets.token_urlsafe(36))
    manifest = {'status': 'FAIL', 'artifactSha256': whole_digest(artifact),
                'harnessSha256': whole_digest(Path(__file__)), 'checks': []}

    with (tempfile.TemporaryDirectory(dir=ROOT/'target', prefix='mysql-tls-config-') as fixture,
          tempfile.TemporaryDirectory(dir=ROOT/'target', prefix='mysql-tls-private-') as private):
        home, private_certificates = Path(fixture), Path(private)
        tasks = home/'tasks'
        tasks.mkdir()
        trust = certificates(private_certificates, args.java_home.resolve())
        source = mysql_service(private_certificates/'source', 311)
        source['networks'] = {'default': {'aliases': ['source-wrong-name']}}
        target = mysql_service(private_certificates/'target', 312)
        target['networks'] = {'default': {'aliases': ['target-wrong-name']}}
        worker = {
            'image': JAVA_IMAGE, 'entrypoint': ['java', '-Xmx384m', *launch],
            'command': ['serve', '/fixture/platform.json'], 'ports': ['127.0.0.1::8080'],
            'environment': {'RM_TLS_PASSWORD': '${RM_TLS_PASSWORD}', 'RM_TLS_TOKEN': '${RM_TLS_TOKEN}'},
            'volumes': [str(artifact)+':/app.jar:ro', str(home)+':/fixture',
                        str(trust/'cacerts')+':/opt/java/openjdk/lib/security/cacerts:ro'],
        }
        if external:
            worker['volumes'].append(str(external)+':/operator/mysql-driver.jar:ro')
            manifest['externalMysqlDriverSha256'] = whole_digest(external)
        compose_file = home/'compose.json'
        compose_file.write_text(json.dumps({'services': {'source': source, 'target': target, 'worker': worker}}))
        pipeline = {
            'formatVersion': 3, 'pipelineId': 'mysql_tls', 'nodeId': 'tls-worker', 'databases': ['sales'],
            'source': {'type': 'MYSQL', 'clusterId': 'mysql-tls', 'host': 'source', 'username': 'root',
                       'password': '${RM_TLS_PASSWORD}', 'serverId': 313, 'expectedVersion': '8.4',
                       'jdbcOptions': JDBC, 'sslMode': 'VERIFY_IDENTITY'},
            'journal': {'mode': 'LOCAL'},
            'targets': [{'id': 'mysql', 'product': 'MYSQL', 'metadataSchema': 'tls_meta',
                         'url': 'jdbc:mysql://target:3306/?'+JDBC, 'username': 'root',
                         'password': '${RM_TLS_PASSWORD}', 'mappings': {'sales': 'replica_sales'}}],
            'pollIntervalMillis': 100, 'retryDelayMillis': 400, 'leaseMillis': 6000, 'heartbeatMillis': 400,
            'chunkSize': 1,
        }
        (tasks/'mysql_tls.yml').write_text(json.dumps(pipeline))
        (home/'platform.json').write_text(json.dumps({'bind': '0.0.0.0', 'port': 8080, 'autoStart': True,
                                                     'pollMillis': 300, 'authToken': '${RM_TLS_TOKEN}',
                                                     'tasksDirectory': '/fixture/tasks', 'dataDirectory': '/fixture/state',
                                                     'workerHeapMiB': 256}))

        def compose(*argv, check=True, statement=None):
            return subprocess.run(['docker', 'compose', '-p', project, '-f', str(compose_file), *argv],
                                  env=environment, input=statement, check=check, capture_output=True, text=True,
                                  timeout=180)

        def sql(service, statement):
            return compose('exec', '-T', service, 'bash', '-c',
                           'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -BN', statement=statement).stdout.strip()

        def status():
            host_port = compose('port', 'worker', '8080').stdout.strip()
            request = urllib.request.Request('http://'+host_port+'/api/tasks/mysql_tls',
                                             headers={'Authorization': 'Bearer '+environment['RM_TLS_TOKEN']})
            with LOCAL_HTTP.open(request, timeout=8) as response:
                return json.load(response)['status']

        def exact():
            columns = 'id,note,CAST(doc AS CHAR),derived'
            return sql('source', f'SELECT {columns} FROM sales.items ORDER BY id;') == sql(
                'target', f'SELECT {columns} FROM replica_sales.items ORDER BY id;')

        def passed(label):
            manifest['checks'].append(label)
            print('PASS ' + label, flush=True)

        try:
            compose('up', '-d', 'source', 'target')
            for service in ('source', 'target'):
                wait_for(lambda: sql(service, 'SELECT @@global.require_secure_transport;') == '1',
                         service + ' enforces TLS on TCP')
            manifest['mysqlVersions'] = {name: sql(name, 'SELECT VERSION();') for name in ('source', 'target')}
            sql('source', "CREATE DATABASE sales; CREATE TABLE sales.items(id BIGINT PRIMARY KEY, note VARCHAR(30),"
                "doc JSON, derived BIGINT GENERATED ALWAYS AS(id*2) STORED);"
                "INSERT INTO sales.items(id,note,doc) VALUES(1,'original',JSON_OBJECT('n',18446744073709551615));")
            preflight = compose('run', '--rm', '--no-deps', 'worker', 'check', '/fixture/tasks/mysql_tls.yml')
            assert json.loads(preflight.stdout)['sourceTables'] == 1
            if external:
                missing = compose('run', '--rm', '--no-deps', '--entrypoint', 'java', 'worker',
                                  '-jar', '/app.jar', 'check', '/fixture/tasks/mysql_tls.yml', check=False)
                assert missing.returncode and 'JDBC_DRIVER_UNAVAILABLE' in missing.stderr
                passed('The external package refuses preflight without Connector/J and accepts the scoped driver classpath')
            wrong = dict(pipeline, source=dict(pipeline['source'], host='source-wrong-name'))
            (home/'wrong-name.yml').write_text(json.dumps(wrong))
            denied = compose('run', '--rm', '--no-deps', 'worker', 'check', '/fixture/wrong-name.yml', check=False)
            assert denied.returncode and not denied.stdout.strip()
            assert any(message in denied.stderr.lower() for message in (
                'identity', 'subject alternative', '没有健康的可写主库'))
            # The alias reaches the same trusted server. Only the hostname verification changes in this control.
            wrong['source']['jdbcOptions'] = JDBC.replace('VERIFY_IDENTITY', 'VERIFY_CA')
            (home/'alias-control.yml').write_text(json.dumps(wrong))
            alias = compose('run', '--rm', '--no-deps', 'worker', 'check', '/fixture/alias-control.yml')
            assert json.loads(alias.stdout)['sourceTables'] == 1
            wrong_target = dict(pipeline, targets=[dict(pipeline['targets'][0],
                url=pipeline['targets'][0]['url'].replace('://target:', '://target-wrong-name:'))])
            (home/'wrong-target-name.yml').write_text(json.dumps(wrong_target))
            denied = compose('run', '--rm', '--no-deps', 'worker', 'check', '/fixture/wrong-target-name.yml', check=False)
            assert denied.returncode and not denied.stdout.strip()
            assert any(message in denied.stderr.lower() for message in ('identity', 'subject alternative', 'hostname'))
            passed('The packaged preflight verifies and refuses wrong source and target DNS identities')

            compose('up', '-d', 'worker')
            reported_jvm = compose('exec', '-T', 'worker', 'java', '-version').stderr
            version = re.search(r'version "([^"]+)"', reported_jvm)
            assert version and version.group(1).startswith('21.')
            manifest['runtimeVersion'] = version.group(1)
            wait_for(lambda: status().get('capture_gtid') and exact(), 'TLS snapshot and original binlog position')
            snapshots = sql('target', 'SELECT snapshot_id FROM tls_meta.rm3_sink_table;')
            assert snapshots
            require_tls = """
                SELECT COUNT(*) FROM performance_schema.threads t
                JOIN performance_schema.status_by_thread s USING (THREAD_ID)
                WHERE s.VARIABLE_NAME='Ssl_version' AND s.VARIABLE_VALUE='TLSv1.3' AND
            """
            wait_for(lambda: sql('source', require_tls+" t.PROCESSLIST_COMMAND LIKE 'Binlog Dump%';") == '1',
                     'native authenticated TLS binlog session')
            wait_for(lambda: int(sql('target', require_tls+" t.PROCESSLIST_USER='root';")) > 0,
                     'native TLS target JDBC connection')
            passed('MySQL native session counters identify TLS 1.3 for the actual binlog and JDBC target channels')

            original = status()['capture_gtid']
            sql('source', "UPDATE sales.items SET note='streamed' WHERE id=1;")
            wait_for(lambda: exact() and status().get('capture_gtid') != original, 'verified live TLS CDC')
            passed('Exact generated and JSON data continue over verified TLS without a new snapshot')
            original = status()['capture_gtid']
            compose('kill', '-s', 'SIGKILL', 'worker')
            sql('source', "INSERT INTO sales.items(id,note,doc) VALUES (2,'offline',JSON_OBJECT('n',2));")
            compose('up', '-d', 'worker')
            wait_for(lambda: exact() and status().get('capture_gtid') != original, 'verified TLS reconnect at saved GTID')
            assert sql('target', 'SELECT snapshot_id FROM tls_meta.rm3_sink_table;') == snapshots
            wait_for(lambda: sql('source', require_tls+" t.PROCESSLIST_COMMAND LIKE 'Binlog Dump%';") == '1',
                     'new Worker binlog TLS session')
            passed('After SIGKILL the Worker reconnects securely from the original GTID and snapshot')
            assert whole_digest(artifact) == manifest['artifactSha256']
            manifest['status'] = 'PASS'
        finally:
            cleanup = compose('down', '-v', '--remove-orphans', check=False)
            manifest['cleanupExitCode'] = cleanup.returncode
            (output/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
            assert cleanup.returncode == 0, 'Fixture cleanup failed'


if __name__ == '__main__':
    main()
