#!/usr/bin/env python3
"""Run paused-failure journal contracts on disposable PostgreSQL/MySQL, with the release JAR first."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import uuid
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--artifact', type=Path, required=True)
parser.add_argument('--test-classes', type=Path, required=True)
parser.add_argument('--product', choices=('POSTGRESQL', 'MYSQL'), required=True)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
artifact, classes, output = args.artifact.resolve(), args.test_classes.resolve(), args.output.resolve()
for path in (artifact, classes, output):
    path.relative_to(ROOT)
assert artifact.is_file() and classes.is_dir()
output.mkdir(parents=True, exist_ok=True)
mysql = args.product == 'MYSQL'
project = 'rivomesh-commit-' + uuid.uuid4().hex[:10]
environment = dict(os.environ, RIVOMESH_FAILURE_DB_PASSWORD=secrets.token_urlsafe(28),
                   RIVOMESH_FAILURE_DB_USER='root' if mysql else 'failure_test',
                   RIVOMESH_FAILURE_DB_DRIVER='com.mysql.cj.jdbc.Driver' if mysql else 'org.postgresql.Driver',
                   RIVOMESH_FAILURE_DB_URL='jdbc:mysql://database:3306/?allowPublicKeyRetrieval=true&sslMode=DISABLED&connectTimeout=3000&socketTimeout=15000' if mysql else 'jdbc:postgresql://database:5432/failure_test?connectTimeout=3&socketTimeout=15')
digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
test_hash = hashlib.sha256()
for entry in sorted(classes.rglob('*')):
    if entry.is_file():
        test_hash.update(str(entry.relative_to(classes)).encode() + b'\0' + entry.read_bytes())
manifest = dict(status='RUNNING', product=args.product, artifactSha256=digest,
                testClassesSha256=test_hash.hexdigest(), harnessSourceSha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                scope='14 native checkpoint,13 transactional commit acknowledgment,10 paused journal contracts and1 actual SQL protocol3/4/5-to6 admission contract;no HTTP/UI mutation,skip,compensation or complete connector matrix')
log = output / 'native.log'
with tempfile.TemporaryDirectory(prefix='failure-native-', dir=ROOT / 'target') as temporary:
    home = Path(temporary)
    home.chmod(0o700)
    db = {'image': 'mysql:8.4' if mysql else 'postgres:17',
          'environment': {'MYSQL_ROOT_PASSWORD': '${RIVOMESH_FAILURE_DB_PASSWORD}'} if mysql else {'POSTGRES_DB': 'failure_test', 'POSTGRES_USER': 'failure_test', 'POSTGRES_PASSWORD': '${RIVOMESH_FAILURE_DB_PASSWORD}'},
          'healthcheck': {'test': ['CMD-SHELL', 'mysqladmin ping -h 127.0.0.1 --silent'] if mysql else ['CMD-SHELL', 'pg_isready -U failure_test -d failure_test'], 'interval': '2s', 'timeout': '3s', 'retries': 60}}
    container_path = lambda path: '/project/' + str(path.relative_to(ROOT))
    runner = {'image': 'eclipse-temurin:21-jre-jammy', 'volumes': [str(ROOT) + ':/project'], 'working_dir': '/project',
              'environment': {key: '${' + key + '}' for key in environment if key.startswith('RIVOMESH_FAILURE_DB_')},
              'command': ['java', '-Xmx256m', '-cp', container_path(artifact) + ':' + container_path(classes) + ':/project/target/tools/junit-platform-console-standalone-1.11.0.jar',
                          'org.junit.platform.console.ConsoleLauncher', 'execute', '--disable-banner', '--fail-if-no-tests',
                          '--select-class=io.github.rivomesh.cdc.FailureCheckpointInspectionTest', '--select-class=io.github.rivomesh.cdc.CommitAcknowledgementTest', '--select-class=io.github.rivomesh.cdc.FailureReviewTest', '--reports-dir=' + container_path(output) + '/junit']}
    compose_file = home / 'compose.json'
    compose_file.write_text(json.dumps({'services': {'database': db, 'runner': runner}}))
    compose = ['docker', 'compose', '-p', project, '-f', str(compose_file)]
    with log.open('w') as stream:
        try:
            subprocess.run(compose + ['up', '-d', '--wait', '--wait-timeout', '150', 'database'], env=environment, stdout=stream, stderr=subprocess.STDOUT, check=True, timeout=180)
            manifest['images'] = {}
            for image in (db['image'], runner['image']):
                metadata = json.loads(subprocess.check_output(['docker', 'image', 'inspect', image], text=True))[0]
                manifest['images'][image] = {'id': metadata['Id'], 'architecture': metadata['Architecture']}
            subprocess.run(compose + ['run', '--rm', '--no-deps', 'runner', 'java', '-version'], env=environment, stdout=stream, stderr=subprocess.STDOUT, check=True, timeout=60)
            subprocess.run(compose + ['run', '--rm', '--no-deps', 'runner'], env=environment, stdout=stream, stderr=subprocess.STDOUT, check=True, timeout=240)
            totals = {key: 0 for key in ('tests', 'failures', 'errors', 'skipped')}
            for report in (output / 'junit').glob('TEST-*.xml'):
                suite = ET.parse(report).getroot()
                for key in totals:
                    totals[key] += int(suite.attrib.get(key, 0))
            manifest['results'] = totals
            assert totals == {'tests': 37, 'failures': 0, 'errors': 0, 'skipped': 0}, totals
            manifest['status'] = 'PASS'
        except Exception:
            manifest['status'] = 'FAIL'
            raise
        finally:
            cleanup = subprocess.run(compose + ['down', '-v', '--remove-orphans'], env=environment, stdout=stream, stderr=subprocess.STDOUT, timeout=90)
            manifest['cleanupExitCode'] = cleanup.returncode
            (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
print(args.product + ': PASS ' + digest)
