#!/usr/bin/env python3
"""Current Java 21 account/auth/audit/kill/recovery. Does not claim database synchronization."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, ProxyHandler, build_opener
import uuid

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('audit_export', ROOT / 'tools/audit-export.py')
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


def command(*args, check=True):
    return subprocess.run(args, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=check)


def main():
    name = 'rivomesh-accounts-native-' + uuid.uuid4().hex[:10]
    relative = Path('target/accounts-native') / name
    directory = ROOT / relative
    directory.mkdir(parents=True, mode=0o700)
    artifact = ROOT / 'sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar'
    original = hashlib.sha256(artifact.read_bytes()).hexdigest()
    command('python3', 'tests/verify-artifact.py')
    for source, target in [(artifact, directory / 'current.jar')]:
        result = command('cp', '-c', str(source), str(target), check=False)
        if result.returncode:
            shutil.copyfile(source, target)
    prefix = '/project/' + str(relative)
    active = directory / 'active-jar'
    active.write_text(prefix + '/current.jar\n')
    root_token, viewer, operator, rotated, new_root = [secrets.token_hex(32) for _ in range(5)]
    config = {'bind': '0.0.0.0', 'port': 8080, 'authToken': '${RIVOMESH_NATIVE_ADMIN}',
              'users': [{'id': 'viewer', 'role': 'VIEWER', 'token': '${RIVOMESH_NATIVE_VIEWER}', 'taskIds': []}],
              'tasksDirectory': prefix + '/tasks', 'dataDirectory': prefix + '/data', 'pollMillis': 100, 'auditMaxEvents': 1000}
    settings = directory / 'platform.json'
    settings.write_text(json.dumps(config)); settings.chmod(0o600)
    environment = dict(os.environ, RIVOMESH_NATIVE_ADMIN=root_token, RIVOMESH_NATIVE_VIEWER=viewer)
    created = False
    try:
        args = ['docker', 'run', '-d', '--init', '--name', name, '--publish', '127.0.0.1::8080',
                '--mount', 'type=bind,src=' + str(ROOT) + ',dst=/project', '--workdir', '/project',
                '--env', 'RIVOMESH_NATIVE_ADMIN', '--env', 'RIVOMESH_NATIVE_VIEWER',
                'maven:3.9-eclipse-temurin-21', 'bash', '-ec', 'read -r jar < "$1"; exec java -Xmx256m -jar "$jar" serve "$2"',
                'accounts-native', prefix + '/active-jar', prefix + '/platform.json']
        subprocess.run(args, cwd=ROOT, env=environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True)
        created = True
        base = 'http://' + command('docker', 'port', name, '8080/tcp').stdout.strip().splitlines()[0]
        opener = build_opener(ProxyHandler({}), audit.NoRedirect())

        def request(path, token=root_token, body=None, method=None, key=None):
            headers = {'Authorization': 'Bearer ' + token}
            if key: headers['X-Request-ID'] = key
            data = None
            if body is not None:
                data = json.dumps(body).encode(); headers['Content-Type'] = 'application/json'
            req = Request(base + path, data=data, headers=headers, method=method or ('POST' if data else 'GET'))
            try:
                with opener.open(req, timeout=5) as response: return response.status, json.load(response)
            except HTTPError as response: return response.code, json.load(response)

        def ready(token):
            nonlocal base
            base = 'http://' + command('docker', 'port', name, '8080/tcp').stdout.strip().splitlines()[0]
            end = time.monotonic() + 60
            while time.monotonic() < end:
                try:
                    if request('/api/session', token)[0] == 200: return
                except (URLError, TimeoutError, ConnectionError): pass
                if command('docker', 'inspect', name, '--format', '{{.State.Running}}').stdout.strip() != 'true':
                    raise AssertionError('Controller exited before becoming ready')
                time.sleep(.2)
            raise AssertionError('Controller readiness timeout')

        def restart(token, kill=False):
            command('docker', 'kill' if kill else 'stop', name)
            command('docker', 'start', name)
            ready(token)

        def failed_start(label):
            command('docker', 'start', name)
            end = time.monotonic() + 30
            while command('docker', 'inspect', name, '--format', '{{.State.Running}}').stdout.strip() == 'true':
                assert time.monotonic() < end, 'Unsafe controller stayed live: ' + label
                time.sleep(.2)
            log = command('docker', 'logs', '--tail', '80', name).stdout
            (directory / (label + '.log')).write_text(log)
            assert command('docker', 'inspect', name, '--format', '{{.State.ExitCode}}').stdout.strip() != '0'
            return log

        ready(root_token)
        initial = request('/api/audit/export?limit=1000')[1]; audit.verify_page(initial)
        status, page = request('/api/accounts'); assert status == 200 and page['mode'] == 'CONFIG'
        assert request('/api/accounts', viewer)[0] == 403
        activation = {'expectedRevision': page['revision']}
        status, page = request('/api/accounts/activate', body=activation, key='activate_native_accounts'); assert status == 200
        assert request('/api/accounts/activate', body=activation, key='activate_native_accounts')[1]['replayed']
        controller = directory / 'data/controller.json'; older = controller.read_bytes()
        task = {'jobId': 'offline-probe', 'source': {'host': '127.0.0.1', 'port': 1, 'database': 'offline_probe', 'username': 'unused'},
                'target': {'type': 'POSTGRESQL', 'url': 'jdbc:postgresql://127.0.0.1:1/unused?connectTimeout=1&socketTimeout=1', 'username': 'unused', 'schema': 'offline_probe'}}
        assert request('/api/tasks', body={'config': task}, key='create_offline_probe')[0] == 201

        create = {'expectedRevision': page['revision'], 'id': 'operator', 'role': 'OPERATOR', 'enabled': True,
                  'taskIds': ['allowed', 'future-task'], 'token': operator}
        status, page = request('/api/accounts', body=create, key='create_native_operator'); assert status == 201
        assert request('/api/accounts', body=create, key='create_native_operator')[1]['replayed']
        assert request('/api/session', operator)[1]['taskScope'] == ['allowed', 'future-task']
        update = {'expectedRevision': page['revision'], 'role': 'VIEWER', 'enabled': True, 'taskIds': []}
        status, page = request('/api/accounts/operator', body=update, method='PUT', key='revoke_native_grants'); assert status == 200
        assert request('/api/session', operator)[1]['taskScope'] == []
        status, page = request('/api/accounts/operator/token', body={'expectedRevision': page['revision'], 'token': rotated}, key='rotate_native_operator'); assert status == 200
        assert request('/api/session', operator)[0] == 401 and request('/api/session', rotated)[0] == 200
        assert request('/api/accounts/legacy-admin', body={'expectedRevision': page['revision']}, method='DELETE', key='deny_last_admin_delete')[1]['code'] == 'ACCOUNT_LAST_ADMIN'
        status, page = request('/api/accounts/legacy-admin/token', body={'expectedRevision': page['revision'], 'token': new_root}, key='rotate_native_root'); assert status == 200
        assert request('/api/session', root_token)[0] == 401
        restart(new_root, kill=True)
        assert request('/api/session', root_token)[0] == 401 and request('/api/session', rotated)[1]['role'] == 'VIEWER'
        locked = command('docker', 'exec', name, 'java', '-jar', prefix + '/current.jar', 'recover-access', prefix + '/platform.json', 'legacy-admin', check=False)
        assert locked.returncode != 0 and 'CONTROLLER_ALREADY_RUNNING' in locked.stdout
        command('docker', 'stop', name); current = controller.read_bytes()
        controller.write_bytes(older)
        assert 'ACCOUNT_AUDIT_ANCHOR_MISMATCH' in failed_start('older-directory')
        controller.write_bytes(current); command('docker', 'start', name); ready(new_root)
        command('docker', 'stop', name); current = controller.read_bytes(); controller.unlink()
        assert 'ACCOUNT_STATE_REQUIRED' in failed_start('missing-directory')
        assert not controller.exists()
        controller.write_bytes(current)
        assert json.loads(current)['version'] == 2
        active.write_text(prefix + '/current.jar\n'); command('docker', 'start', name); ready(new_root)
        command('docker', 'stop', name)
        # Represent a committed RUNNING intent without first launching a source worker.
        pending = json.loads(controller.read_text()); pending['tasks']['offline-probe']['desired'] = 'RUNNING'
        controller.write_text(json.dumps(pending))
        assert not list((directory / 'data/logs').iterdir())
        offline = ['docker', 'run', '--rm', '--mount', 'type=bind,src=' + str(ROOT) + ',dst=/project', '--workdir', '/project',
                   '--env', 'RIVOMESH_NATIVE_ADMIN', '--env', 'RIVOMESH_NATIVE_VIEWER', 'maven:3.9-eclipse-temurin-21',
                   'java', '-jar', prefix + '/current.jar', 'recover-access', prefix + '/platform.json', 'legacy-admin']
        result = subprocess.run(offline, cwd=ROOT, env=environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        assert result.returncode == 0 and 'ACCESS_RECOVERED_OFFLINE_NO_WORKERS_STARTED' in result.stdout
        recovered = json.loads(controller.read_text())
        assert recovered['tasks']['offline-probe']['desired'] == 'RUNNING'
        assert not list((directory / 'data/logs').iterdir()), 'Offline recovery launched a worker'
        recovered['tasks']['offline-probe']['desired'] = 'STOPPED'; controller.write_text(json.dumps(recovered))
        command('docker', 'start', name); ready(root_token)
        assert request('/api/session', new_root)[0] == 401
        status, exported = request('/api/audit/export?limit=1000'); assert status == 200; audit.verify_page(exported)
        assert any(item.get('action') == 'ACCOUNT_RECOVER' for item in exported['events'])
        exported_text = json.dumps(exported)
        for secret in [root_token, viewer, operator, rotated, new_root]:
            assert secret not in exported_text and secret not in controller.read_text()
            assert hashlib.sha256(secret.encode()).hexdigest() not in json.dumps(request('/api/accounts')[1])
        (directory / 'audit-export.json').write_text(json.dumps(exported)); (directory / 'audit-export.json').chmod(0o600)
        assert hashlib.sha256(artifact.read_bytes()).hexdigest() == original
        command('python3', 'tests/verify-artifact.py')
        print('ACCOUNTS_NATIVE_ACCEPTED: Java 21 actual release HTTP, explicit activation, grants, token rotation, last-admin guard, SIGKILL persistence, stale/missing directory refusal, controller lock, offline recovery preserving RUNNING intent without launching workers, and current chained audit proofs; artifact=' + original)
    finally:
        if created:
            (directory / 'controller.log').write_text(command('docker', 'logs', '--tail', '100', name).stdout)
            command('docker', 'rm', '-f', name, check=False)


if __name__ == '__main__':
    main()
