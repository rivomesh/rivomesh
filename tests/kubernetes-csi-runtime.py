#!/usr/bin/env python3
"""Verify Helm and checked-in Kubernetes manifests against an isolated CSI test cluster."""

import argparse
from contextlib import contextmanager
from http.cookies import SimpleCookie
import json
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / 'deploy/helm/rivomesh'
LOCAL_HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def command(*args, data=None, timeout=240):
    return subprocess.run(args, cwd=ROOT, input=data, text=True, capture_output=True,
                          check=True, timeout=timeout).stdout.strip()


def wait(check, description, timeout=120):
    deadline = time.monotonic() + timeout
    last_error = ''
    while time.monotonic() < deadline:
        try:
            if result := check():
                return result
        except (OSError, subprocess.CalledProcessError, urllib.error.URLError) as error:
            last_error = str(error)
        time.sleep(1)
    raise AssertionError(f'{description} timed out. {last_error}')


@contextmanager
def forward(kubectl, target_pod):
    with socket.socket() as address:
        address.bind(('127.0.0.1', 0))
        port = address.getsockname()[1]
    process = subprocess.Popen([*kubectl, 'port-forward', 'pod/' + target_pod, f'{port}:8080',
                                '--address=127.0.0.1'], stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
    try:
        yield f'http://127.0.0.1:{port}'
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def request(base, path, token=None, body=None, expected=200, cookie=None, issue_cookie=False):
    headers = {'X-Request-ID': 'kube_' + secrets.token_hex(12)}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    if cookie:
        headers['Cookie'] = cookie
    data = None
    if body is not None:
        headers['Content-Type'] = 'application/json'
        data = json.dumps(body).encode()
    query = urllib.request.Request(base + path, data=data, headers=headers,
                                   method='POST' if issue_cookie else None)
    try:
        response = LOCAL_HTTP.open(query, timeout=40 if data is not None or issue_cookie else 10)
    except urllib.error.HTTPError as error:
        if error.code in (502, 503, 504) and expected == 200:
            raise
        response = error
    with response:
        assert response.status == expected, f'{path}: HTTP {response.status}, expected {expected}'
        if issue_cookie:
            cookies = SimpleCookie(response.headers['Set-Cookie'])
            assert len(cookies) == 1
            session = next(iter(cookies.values()))
            assert session['httponly'] and session['secure']
            return session.key + '=' + session.value
        return json.load(response)


def resource(kind, name, **fields):
    return {'apiVersion': 'v1', 'kind': kind, 'metadata': {'name': name}, **fields}


def profile(options, mode, record):
    namespace = f'rivomesh-proof-{mode}-{secrets.token_hex(4)}'
    cluster = ['kubectl', '--context', options.context]
    client = [*cluster, '-n', namespace]
    worker = options.worker_outage_node
    storage_class = namespace + '-storage' if worker else options.storage_class
    token = secrets.token_urlsafe(32)
    password = secrets.token_hex(24)
    route = '/api/tasks/csi_test'

    def kube(*args, **kwargs):
        return command(*client, *args, **kwargs)

    def apply(item):
        kube('apply', '-f', '-', data=json.dumps(item))

    def secret(value):
        apply(resource('Secret', 'rivomesh-auth', type='Opaque', stringData={'token': value}))

    def observe(label):
        record['checks'].append(label)
        print(f'PASS {mode} {label}', flush=True)

    def sql(database, statement):
        return kube('exec', 'pg-fixture', '--', 'psql', '-XAt', '-v', 'ON_ERROR_STOP=1',
                    '-U', 'postgres', '-d', database, '-c', statement)

    def ready():
        kube('rollout', 'status', 'deployment/rivomesh', '--timeout=180s')

    def pod():
        return json.loads(kube('get', 'pods', '-l', 'app.kubernetes.io/name=rivomesh',
                               '-o', 'json'))['items'][0]

    def volume():
        pvc = json.loads(kube('get', 'pvc', 'rivomesh-data', '-o', 'json'))
        pv = json.loads(command(*cluster, 'get', 'pv', pvc['spec']['volumeName'], '-o', 'json'))
        assert pv['spec']['csi']['driver'] == options.csi_driver
        assert pvc['spec']['accessModes'] == ['ReadWriteOnce']
        return pvc['metadata']['uid'], pv['spec']['csi']['volumeHandle']

    def deploy(heap):
        repository, tag = options.image.rsplit(':', 1)
        if mode == 'helm':
            command('helm', '--kube-context', options.context, '-n', namespace,
                    'upgrade', '--install', 'rivomesh', str(CHART),
                    '--set-string', f'image.repository={repository},image.tag={tag},'
                                    f'persistence.storageClass={storage_class},persistence.size=1Gi',
                    '--set', f'controllerHeapMiB={heap}')
        else:
            with tempfile.TemporaryDirectory(prefix='rivomesh-native-') as directory:
                folder = Path(directory)
                source = ROOT / 'deploy/kubernetes/rivomesh.yaml'
                (folder / 'rivomesh.yaml').write_text(source.read_text())
                deployment = {
                    'apiVersion': 'apps/v1', 'kind': 'Deployment', 'metadata': {'name': 'rivomesh'},
                    'spec': {'template': {'spec': {'containers': [
                        {'name': 'rivomesh', 'image': options.image,
                         'env': [{'name': 'JAVA_TOOL_OPTIONS', 'value': f'-Xmx{heap}m'}]}
                    ]}}}
                }
                if worker:
                    deployment['spec']['template']['spec']['nodeSelector'] = {'kubernetes.io/hostname': worker}
                claim = resource('PersistentVolumeClaim', 'rivomesh-data',
                                 spec={'storageClassName': storage_class,
                                       'resources': {'requests': {'storage': '1Gi'}}})
                for name, settings in (('deployment', deployment), ('claim', claim)):
                    (folder / (name + '.json')).write_text(json.dumps(settings))
                (folder / 'kustomization.yaml').write_text(json.dumps({
                    'apiVersion': 'kustomize.config.k8s.io/v1beta1', 'kind': 'Kustomization',
                    'resources': ['rivomesh.yaml'],
                    'patches': [{'path': 'deployment.json'}, {'path': 'claim.json'}]
                }))
                kube('apply', '-k', directory)
        ready()

    document = {
        'formatVersion': 3, 'pipelineId': 'csi_test', 'nodeId': 'kube-worker',
        'source': {'type': 'POSTGRESQL', 'clusterId': 'kube-fixture', 'database': 'postgres',
                   'topology': 'SINGLE', 'expectedVersion': '17', 'username': 'postgres',
                   'password': password, 'postgresql': {
                       'url': 'jdbc:postgresql://pg-fixture:5432/postgres',
                       'publication': 'rive_test', 'slotPrefix': 'rive_test'}},
        'databases': ['sales'], 'journal': {'mode': 'LOCAL'},
        'targets': [{'id': 'pg', 'product': 'POSTGRESQL', 'username': 'postgres',
                     'password': password, 'url': 'jdbc:postgresql://pg-fixture:5432/sync_test',
                     'metadataSchema': 'sync_meta', 'mappings': {'sales': 'copy_sales'}}],
        'pollIntervalMillis': 100, 'retryDelayMillis': 200
    }

    def view(base):
        return request(base, route, token)

    def start(base):
        current = view(base)
        if current['desired'] == 'RUNNING':
            return
        request(base, route + '/actions', token, {'action': 'START', 'expectedRevision': current['revision']},
                expected=202)

    def snapshot():
        return sql('sync_test', 'SELECT snapshot_id FROM sync_meta.rm3_sink_table')

    def node_ready():
        node = json.loads(command(*cluster, 'get', 'node', worker, '-o', 'json'))
        return any(row['type'] == 'Ready' and row['status'] == 'True'
                   for row in node['status']['conditions'])

    def caught_up(base):
        status = view(base)['status']
        if status.get('run_state') in ('ERROR', 'PAUSED'):
            observed = {field: status.get(field) for field in
                        ('run_state', 'error_code', 'error_stage', 'capture_lsn', 'lastSequence')}
            raise AssertionError('Pipeline failed after remount: ' + json.dumps(observed))
        return sql('sync_test', 'SELECT label FROM copy_sales.items') == 'offline'

    command(*cluster, 'create', 'namespace', namespace)
    try:
        if worker:
            storage = json.loads(command(*cluster, 'get', 'sc', options.storage_class, '-o', 'json'))
            apply({'apiVersion': 'storage.k8s.io/v1', 'kind': 'StorageClass',
                   'metadata': {'name': storage_class}, 'reclaimPolicy': 'Delete',
                   'provisioner': storage['provisioner'], 'parameters': storage.get('parameters', {}),
                   'volumeBindingMode': 'WaitForFirstConsumer'})
        secret(token)
        apply(resource('Secret', 'pg-fixture', type='Opaque', stringData={'password': password}))
        apply(resource('Service', 'pg-fixture', spec={'selector': {'app': 'pg-fixture'},
                                                    'ports': [{'port': 5432}]}))
        database = resource('Pod', 'pg-fixture', spec={'containers': [{
            'name': 'postgres', 'image': 'postgres:17',
            'args': ['-c', 'wal_level=logical'],
            'env': [{'name': 'POSTGRES_PASSWORD', 'valueFrom': {
                'secretKeyRef': {'name': 'pg-fixture', 'key': 'password'}
            }}]}]})
        database['metadata']['labels'] = {'app': 'pg-fixture'}
        if worker:
            planes = command(*cluster, 'get', 'nodes', '-l', 'node-role.kubernetes.io/control-plane', '-o', 'name')
            (plane,) = planes.splitlines()
            database['spec']['nodeSelector'] = {'kubernetes.io/hostname': plane.split('/', 1)[1]}
            database['spec']['tolerations'] = [{'key': 'node-role.kubernetes.io/control-plane',
                                                'operator': 'Exists', 'effect': 'NoSchedule'}]
        apply(database)
        wait(lambda: kube('exec', 'pg-fixture', '--', 'pg_isready', '-h', '127.0.0.1'),
             'PostgreSQL fixture')
        sql('postgres', 'CREATE DATABASE sync_test')
        sql('postgres', """CREATE SCHEMA sales; CREATE TABLE sales.items(id BIGINT PRIMARY KEY,label TEXT);
            ALTER TABLE sales.items REPLICA IDENTITY FULL; INSERT INTO sales.items VALUES(1,'snapshot');
            CREATE PUBLICATION rive_test FOR TABLE sales.items WITH (publish='insert,update,delete')""")
        deploy(512)
        first = volume()
        original_pod = pod()
        record['node'] = original_pod['spec']['nodeName']
        record['containerImageID'] = original_pod['status']['containerStatuses'][0]['imageID']
        with forward(client, original_pod['metadata']['name']) as base:
            wait(lambda: request(base, '/api/session', token), 'console authentication')
            session = request(base, '/api/session', token, issue_cookie=True)
            assert request(base, '/api/session', cookie=session)['authentication'] == 'COOKIE'
            request(base, '/api/tasks', token, {'config': document}, expected=201)
            start(base)
            wait(lambda: sql('sync_test', 'SELECT label FROM copy_sales.items') == 'snapshot',
                 'snapshot delivery')
            initial = snapshot()
            assert initial
        observe('cookie login survives refresh and SQL snapshot writes to a dynamically provisioned RWO CSI volume')

        kube('scale', 'deployment/rivomesh', '--replicas=0')
        wait(lambda: not kube('get', 'pod', original_pod['metadata']['name'],
                              '--ignore-not-found', '-o', 'name'), 'old Pod exits')
        sql('postgres', "UPDATE sales.items SET label='offline' WHERE id=1")
        kube('scale', 'deployment/rivomesh', '--replicas=1')
        ready()
        assert volume() == first
        assert pod()['metadata']['uid'] != original_pod['metadata']['uid']
        with forward(client, pod()['metadata']['name']) as base:
            wait(lambda: request(base, '/api/session', token), 'console after remount')
            start(base)
            wait(lambda: caught_up(base), 'incremental delivery after remount')
            assert snapshot() == initial
        observe('scale to zero and back remounts the same CSI volume and resumes without resnapshot')

        previous = pod()['metadata']['uid']
        deploy(640)
        assert volume() == first and pod()['metadata']['uid'] != previous
        with forward(client, pod()['metadata']['name']) as base:
            wait(lambda: request(base, '/api/session', token), 'console after configuration rollout')
            assert view(base)['sourceType'] == 'POSTGRESQL'
        observe('configuration rollout preserves the task and CSI claim')

        rotated = secrets.token_urlsafe(32)
        secret(rotated)
        kube('rollout', 'restart', 'deployment/rivomesh')
        ready()
        with forward(client, pod()['metadata']['name']) as base:
            wait(lambda: request(base, '/api/session', rotated), 'rotated API token')
            request(base, '/api/session', token, expected=401)
        observe('Secret rotation rejects the old token after Pod replacement')

        if worker:
            token = rotated
            app = pod()
            assert app['spec']['nodeName'] == worker and node_ready()
            with forward(client, app['metadata']['name']) as base:
                wait(lambda: request(base, '/api/session', token), 'console before node outage')
                before = wait(lambda: view(base)['status'].get('capture_lsn') and view(base)['status'],
                              'original durable position')
            cluster_manager = ['minikube', '-p', options.context, 'node']
            command(*cluster_manager, 'stop', worker)
            try:
                wait(lambda: not node_ready(), 'worker node becomes unavailable')
                sql('postgres', "UPDATE sales.items SET label='worker offline' WHERE id=1")
                assert sql('sync_test', 'SELECT label FROM copy_sales.items') == 'offline'
                assert volume() == first
            finally:
                command(*cluster_manager, 'start', worker)
            wait(node_ready, 'original worker rejoins', timeout=180)
            wait(lambda: sql('sync_test', 'SELECT label FROM copy_sales.items') == 'worker offline',
                 'CDC resumes after physical worker restart', timeout=180)
            assert volume() == first and snapshot() == initial
            with forward(client, pod()['metadata']['name']) as base:
                wait(lambda: request(base, '/api/session', token), 'console after node restart')
                resumed = wait(lambda: view(base)['status'].get('epoch', -1) > before['epoch']
                               and view(base)['status'], 'lease epoch advances')
                assert resumed['capture_lsn'] != before['capture_lsn']
            observe('worker-node outage retains the local CSI volume and CDC resumes under a newer lease')
        record['status'] = 'PASS'
    except Exception as failure:
        details = [str(failure)]
        if isinstance(failure, subprocess.CalledProcessError):
            details.append(failure.stderr)
        for query in (('get', 'pods,pvc', '-o', 'wide'),
                      ('get', 'events', '--sort-by=.lastTimestamp'),
                      ('exec', 'deployment/rivomesh', '--', 'tail', '-n', '40',
                       '/app/data/controller/logs/csi_test.log')):
            try:
                details.append(kube(*query))
            except (OSError, subprocess.CalledProcessError):
                pass
        diagnostic = '\n'.join(details).replace(token, '[REDACTED]').replace(password, '[REDACTED]')
        (options.output / (mode + '-failure.log')).write_text(diagnostic + '\n')
        raise
    finally:
        try:
            kube('delete', 'namespace', namespace, '--timeout=180s')
        finally:
            if worker:
                command(*cluster, 'delete', 'sc', storage_class, '--ignore-not-found')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', required=True, help='Explicit context for the disposable test cluster')
    parser.add_argument('--image', default='rivomesh:0.1.0-SNAPSHOT')
    parser.add_argument('--storage-class', default='csi-hostpath-sc')
    parser.add_argument('--csi-driver', default='hostpath.csi.k8s.io')
    parser.add_argument('--worker-outage-node', help='Also stop and restart a worker in the named disposable Minikube profile')
    parser.add_argument('--output', required=True, type=Path)
    options = parser.parse_args()
    if options.worker_outage_node:
        kube = ['kubectl', '--context', options.context]
        worker = json.loads(command(*kube, 'get', 'node', options.worker_outage_node, '-o', 'json'))
        if 'node-role.kubernetes.io/control-plane' in worker['metadata'].get('labels', {}):
            parser.error('The outage node must be a worker')
        profile_nodes = command('minikube', '-p', options.context, 'node', 'list')
        if options.worker_outage_node not in {line.split()[0] for line in profile_nodes.splitlines() if line.strip()}:
            parser.error('The outage node must belong to the explicitly selected Minikube profile')
    version = json.loads(command('kubectl', '--context', options.context, 'version', '-o', 'json'))
    report = {'serverVersion': version['serverVersion']['gitVersion'], 'storageClass': options.storage_class,
              'csiDriver': options.csi_driver, 'image': options.image, 'profiles': {}}
    options.output.mkdir(parents=True, exist_ok=True)
    try:
        for mode in ('native',) if options.worker_outage_node else ('helm', 'native'):
            record = {'checks': [], 'status': 'FAIL'}
            report['profiles'][mode] = record
            profile(options, mode, record)
    finally:
        (options.output / 'manifest.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
