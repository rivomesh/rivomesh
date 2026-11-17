#!/usr/bin/env python3
"""Maintain a local Docker development console, isolated from acceptance fixtures."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[1]
PREVIEW_HOME = ROOT / 'target/development-preview'
ARTIFACT = ROOT / 'sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar'
URL = 'http://127.0.0.1:18088'


def private_json(path, value):
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as out:
        json.dump(value, out, indent=2)
        out.write('\n')


def initialize():
    PREVIEW_HOME.mkdir(parents=True, exist_ok=True, mode=0o700)
    for directory in ('runtime', 'state', 'state-node-b'):
        (PREVIEW_HOME / directory).mkdir(exist_ok=True, mode=0o700)
    control = PREVIEW_HOME / 'control-credentials.json'
    if not control.exists():
        private_json(control, {'password': secrets.token_urlsafe(30)})
    credentials = PREVIEW_HOME / 'credentials.json'
    if not credentials.exists():
        private_json(credentials, {'account': 'admin', 'token': secrets.token_urlsafe(30), 'url': URL})
    return json.loads(credentials.read_text())


def compose(credentials, *args, stdout=None):
    env = dict(os.environ, RIVOMESH_PREVIEW_HOME=str(PREVIEW_HOME),
               RIVOMESH_PREVIEW_UID=str(os.getuid()), RIVOMESH_PREVIEW_GID=str(os.getgid()),
               RIVOMESH_PREVIEW_TOKEN=credentials['token'],
               RIVOMESH_CONTROL_PASSWORD=json.loads((PREVIEW_HOME / 'control-credentials.json').read_text())['password'])
    subprocess.run(['docker', 'compose', '-p', 'rivomesh-dev-preview', '-f',
                    str(ROOT / 'deploy/development/compose.yml'), *args], env=env, cwd=ROOT, check=True, stdout=stdout)


def backup_migration(credentials):
    states=[PREVIEW_HOME / name / 'controller/controller.json' for name in ('state','state-node-b')]
    # Preview data mounts /app/data, while the controller directory is /app/data/controller.
    present=[path for path in states if path.exists()]
    if not present or all(json.loads(path.read_text()).get('version',1)>=3 for path in present):
        return
    parent=PREVIEW_HOME / 'backups';parent.mkdir(exist_ok=True,mode=0o700)
    backup=parent / ('before-shared-access-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ'))
    backup.mkdir(mode=0o700)
    for name in ('state','state-node-b'):
        shutil.copytree(PREVIEW_HOME / name,backup / name)
    runtime=PREVIEW_HOME / 'runtime/rivomesh.jar'
    if runtime.exists():shutil.copyfile(runtime,backup / 'rivomesh.jar')
    original=(ROOT / 'deploy/development/platform.yml').read_text().replace('  sharedAccess: true\n','')
    config=backup / 'platform-registry-only.yml';config.write_text(original);config.chmod(0o600)
    descriptor=os.open(str(backup / 'control-store.dump'),os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(descriptor,'wb') as output:
        compose(credentials,'exec','-T','control-store','pg_dump','-U','rivomesh','-d','rivomesh_control','--format=custom',stdout=output)
    private_json(backup / 'manifest.json',{'purpose':'coordinated pre-shared-access migration backup','sql':'control-store.dump','states':['state','state-node-b'],'originalArtifactSha256':hashlib.sha256((backup / 'rivomesh.jar').read_bytes()).hexdigest() if runtime.exists() else None})
    print('Private coordinated migration backup:',backup)


def publish():
    # Copy to a temporary file first: the running JVM never reads Maven's output JAR.
    fd, name = tempfile.mkstemp(prefix='release-', suffix='.jar', dir=PREVIEW_HOME / 'runtime')
    os.close(fd)
    temporary = Path(name)
    try:
        shutil.copyfile(ARTIFACT, temporary)
        with zipfile.ZipFile(temporary) as archive:
            if archive.testzip() is not None:
                raise ValueError('Corrupt release JAR')
            for entry in ('io/github/rivomesh/Main.class', 'console/index.html', 'META-INF/services/java.sql.Driver'):
                archive.getinfo(entry)
            manifest = archive.read('META-INF/MANIFEST.MF').decode()
            if 'Main-Class: io.github.rivomesh.Main' not in manifest:
                raise ValueError('Unexpected release entry point')
        digest = hashlib.sha256(temporary.read_bytes()).hexdigest()
        temporary.replace(PREVIEW_HOME / 'runtime/rivomesh.jar')
        (PREVIEW_HOME / 'runtime/release.json').write_text(json.dumps({'sha256': digest, 'source': str(ARTIFACT)}, indent=2) + '\n')
        return digest
    finally:
        temporary.unlink(missing_ok=True)


def verify(credentials):
    with urllib.request.urlopen(URL + '/healthz', timeout=10) as response:
        assert response.status == 200
    request = urllib.request.Request(URL + '/api/session', headers={'Authorization': 'Bearer ' + credentials['token']})
    with urllib.request.urlopen(request, timeout=10) as response:
        session = json.load(response)
        assert session['id'] == 'admin' and session['role'] == 'ADMIN'
    request_b = urllib.request.Request('http://localhost:18089/api/nodes', headers={'Authorization': 'Bearer ' + credentials['token']})
    with urllib.request.urlopen(request_b, timeout=10) as response:
        page = json.load(response)
        assert {node['id'] for node in page['nodes']} == {'preview-a', 'preview-b'}
    print('PREVIEW_ACCEPTED:', URL, 'account=admin; private credentials:', PREVIEW_HOME / 'credentials.json')


def main():
    global ARTIFACT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('start', 'update', 'status', 'stop', 'credentials'))
    parser.add_argument('--artifact',type=Path,default=ARTIFACT,help='Explicit frozen development JAR')
    args = parser.parse_args()
    ARTIFACT=args.artifact.resolve()
    credentials = initialize()
    if args.action in ('start', 'update'):
        # Quiesce both old members before advancing shared-control protocol.
        # Primary imports the existing managed directory before B can bootstrap.
        compose(credentials, 'stop', 'console-b', 'console')
        backup_migration(credentials)
        digest = publish()
        compose(credentials, 'up', '-d', '--force-recreate', '--wait', '--wait-timeout', '120', 'console', 'console-b')
        verify(credentials)
        print('Backend/driver SHA-256:', digest)
        print('UI resources are mounted from source; refresh the page after UI edits.')
    elif args.action == 'stop':
        compose(credentials, 'down', '--remove-orphans')
    elif args.action == 'status':
        compose(credentials, 'ps')
        verify(credentials)
    else:
        print(json.dumps(credentials, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
