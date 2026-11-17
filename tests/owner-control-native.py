#!/usr/bin/env python3
"""Native owner-control SQL protocol gate; temporary fixture and development classes, not a release gate."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import shlex
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--product', choices=('POSTGRESQL', 'MYSQL'), required=True)
parser.add_argument('--artifact', type=Path, default=ROOT / 'sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar')
parser.add_argument('--test-classes', type=Path, help='Run the frozen native fixture against the release JAR first, without compiling development sources')
parser.add_argument('--release-manifest', type=Path)
parser.add_argument('--fixture', choices=('owner','configuration','execution'), default='owner')
args = parser.parse_args()
if args.fixture == 'execution' and not args.test_classes:
    parser.error('execution admission uses the packaged artifact first and requires --test-classes')
artifact = args.artifact.resolve()
artifact.relative_to(ROOT)
test_classes = args.test_classes.resolve() if args.test_classes else None
if test_classes:
    test_classes.relative_to(ROOT)
    assert test_classes.is_dir(), 'Frozen fixture classes missing'
    classes_hash = hashlib.sha256()
    for entry in sorted(test_classes.rglob('*')):
        if entry.is_file():
            classes_hash.update(str(entry.relative_to(test_classes)).encode() + b'\0' + entry.read_bytes())
    if args.release_manifest:
        manifest = json.loads(args.release_manifest.read_text())
        assert artifact == (ROOT / manifest['artifact']).resolve()
        assert test_classes == (ROOT / manifest['testClasses']).resolve()
        assert hashlib.sha256(artifact.read_bytes()).hexdigest() == manifest['artifactSha256']
        assert classes_hash.hexdigest() == manifest['testClassesSha256']
elif args.release_manifest:
    parser.error('--release-manifest requires --test-classes')
mysql = args.product == 'MYSQL'
environment = dict(os.environ, RIVOMESH_OWNER_DB_PASSWORD=secrets.token_urlsafe(24),
                   RIVOMESH_OWNER_TOKEN=secrets.token_urlsafe(32),
                   RIVOMESH_OWNER_KEY=base64.b64encode(secrets.token_bytes(32)).decode())
project = 'rivomesh-owner-' + uuid.uuid4().hex[:10]
with tempfile.TemporaryDirectory(prefix='owner-native-', dir=ROOT / 'target') as temporary:
    home = Path(temporary)
    home.chmod(0o700)
    source_paths = sorted((ROOT / 'sync-app/src/main/java/io/github/rivomesh/platform').glob('*.java'))
    source_paths.append(ROOT / 'sync-app/src/test/java/io/github/rivomesh/platform/OwnerControlAcceptance.java')
    source_paths.append(ROOT / 'sync-app/src/test/java/io/github/rivomesh/platform/GlobalConfigurationAcceptance.java')
    sources = home / 'sources.args'
    sources.write_text('\n'.join('/project/' + str(p.relative_to(ROOT)) for p in source_paths) + '\n')
    (home / 'classes').mkdir()
    artifact_path = '/project/' + str(artifact.relative_to(ROOT))
    fixture_class = 'io.github.rivomesh.platform.' + ('ExecutionAdmissionAcceptance' if args.fixture == 'execution' else 'GlobalConfigurationAcceptance' if args.fixture == 'configuration' else 'OwnerControlAcceptance')
    if args.fixture == 'execution':
        frozen_classes = '/project/' + str(test_classes.relative_to(ROOT))
        fixture_source = ROOT / 'tests/roles/ExecutionAdmissionAcceptance.java'
        fixture_digest = hashlib.sha256(fixture_source.read_bytes()).hexdigest()
        command = ['sh', '-c', 'javac -source 8 -target 8 -cp ' + shlex.quote(artifact_path + ':' + frozen_classes) + ' -d /fixture/classes /project/tests/roles/ExecutionAdmissionAcceptance.java && java -Xmx256m -cp ' + shlex.quote(artifact_path + ':/fixture/classes:' + frozen_classes) + ' ' + fixture_class + ' /fixture/state']
    elif test_classes:
        command = ['java', '-Xmx256m', '-cp', artifact_path + ':/project/' + str(test_classes.relative_to(ROOT)), fixture_class, '/fixture/state']
    else:
        command = ['sh', '-c', 'javac -source 8 -target 8 -cp ' + shlex.quote(artifact_path) + ' -d /fixture/classes @/fixture/sources.args && java -Xmx256m -cp ' + shlex.quote('/fixture/classes:' + artifact_path) + ' ' + fixture_class + ' /fixture/state']
    services = {
        'store': {'image': 'mysql:8.4' if mysql else 'postgres:17',
                  'environment': {'MYSQL_ROOT_PASSWORD': '${RIVOMESH_OWNER_DB_PASSWORD}', 'MYSQL_DATABASE': 'rivomesh_control'} if mysql else {'POSTGRES_PASSWORD': '${RIVOMESH_OWNER_DB_PASSWORD}', 'POSTGRES_DB': 'rivomesh_control'},
                  'tmpfs': ['/var/lib/mysql'] if mysql else ['/var/lib/postgresql/data']},
        'runner': {'image': 'maven:3.9-eclipse-temurin-8', 'entrypoint': command,
                   'environment': {'RIVOMESH_OWNER_DB_PASSWORD': '${RIVOMESH_OWNER_DB_PASSWORD}', 'RIVOMESH_OWNER_TOKEN': '${RIVOMESH_OWNER_TOKEN}', 'RIVOMESH_OWNER_KEY': '${RIVOMESH_OWNER_KEY}',
                                   'RIVOMESH_OWNER_DRIVER': 'com.mysql.cj.jdbc.Driver' if mysql else 'org.postgresql.Driver',
                                   'RIVOMESH_OWNER_URL': 'jdbc:mysql://store:3306/rivomesh_control?allowPublicKeyRetrieval=true&sslMode=DISABLED' if mysql else 'jdbc:postgresql://store:5432/rivomesh_control',
                                   'RIVOMESH_OWNER_USERNAME': 'root' if mysql else 'postgres'},
                   'volumes': [str(ROOT) + ':/project:ro', str(home) + ':/fixture']}}
    compose_file = home / 'compose.json'
    compose_file.write_text(json.dumps({'services': services}))

    def compose(*arguments, check=True, timeout=120):
        return subprocess.run(['docker', 'compose', '-p', project, '-f', str(compose_file), *arguments],
                              env=environment, check=check, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout)

    try:
        compose('up', '-d', 'store')
        probe = ['mysqladmin', 'ping', '-h', '127.0.0.1', '--silent'] if mysql else ['pg_isready', '-h', '127.0.0.1', '-U', 'postgres', '-d', 'rivomesh_control']
        deadline = time.monotonic() + 100
        while compose('exec', '-T', 'store', *probe, check=False).returncode:
            if time.monotonic() >= deadline:
                raise AssertionError('OWNER_STORE_STARTUP_TIMEOUT')
            time.sleep(0.5)
        result = compose('run', '--rm', '-T', 'runner', check=False, timeout=180)
        for line in (result.stdout + result.stderr).splitlines():
            if any(secret in line for secret in (environment['RIVOMESH_OWNER_DB_PASSWORD'], environment['RIVOMESH_OWNER_TOKEN'], environment['RIVOMESH_OWNER_KEY'])):
                print('[private fixture output omitted]')
            else:
                print(line.replace(str(ROOT), '$WORKSPACE'))
        if result.returncode:
            raise SystemExit(result.returncode)
        print(('ACTUAL_JAR_FIRST_ARTIFACT_SHA256=' if test_classes else 'BASE_ARTIFACT_SHA256=') + hashlib.sha256(artifact.read_bytes()).hexdigest())
        if test_classes:
            print('FROZEN_FIXTURE_CLASSES_SHA256=' + classes_hash.hexdigest())
            if args.fixture == 'execution':
                assert hashlib.sha256(fixture_source.read_bytes()).hexdigest() == fixture_digest
                print('NATIVE_EXECUTION_FIXTURE_SOURCE_SHA256=' + fixture_digest)
        else:
            print('DEVELOPMENT_PLATFORM_SOURCE_SHA256=' + hashlib.sha256(b''.join(p.read_bytes() for p in source_paths)).hexdigest())
    finally:
        compose('down', '-v', '--remove-orphans', timeout=150)
