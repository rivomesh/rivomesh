#!/usr/bin/env python3
"""Verify Helm and checked-in Kubernetes manifests against an isolated CSI test cluster."""

import argparse
from contextlib import contextmanager
from http.cookies import SimpleCookie
import json
from pathlib import Path
import re
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

    def artifact_hash(expected):
        digest = kube('exec', 'deployment/rivomesh', '--', 'sha256sum', '/app/rivomesh.jar').split()[0]
        assert re.fullmatch('[a-f0-9]{64}', digest), 'Invalid runtime artifact hash'
        if expected:
            assert expected == digest, 'Running JAR differs from the candidate'
        return digest

    def volume():
        pvc = json.loads(kube('get', 'pvc', 'rivomesh-data', '-o', 'json'))
        pv = json.loads(command(*cluster, 'get', 'pv', pvc['spec']['volumeName'], '-o', 'json'))
        assert pv['spec']['csi']['driver'] == options.csi_driver
        assert pvc['spec']['accessModes'] == ['ReadWriteOnce']
        return pvc['metadata']['uid'], pv['spec']['csi']['volumeHandle']

    def deploy(heap, image):
        repository, tag = image.rsplit(':', 1)
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
                        {'name': 'rivomesh', 'image': image,
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
            CREATE PUBLICATION rive_test FOR TABLE sales.items WITH (publish='insert,update,delete,truncate')""")
        deploy(512, options.image)
        first = volume()
        original_pod = pod()
        record['node'] = original_pod['spec']['nodeName']
        record['containerImageID'] = original_pod['status']['containerStatuses'][0]['imageID']
        record['artifactSha256'] = artifact_hash(options.artifact_sha256)
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
        deploy(640, options.image)
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

        token = rotated
        previous_label = 'offline'
        expected_artifact = record['artifactSha256']
        if options.upgrade_image:
            previous = pod()
            with forward(client, previous['metadata']['name']) as base:
                before = wait(lambda: view(base)['status'].get('capture_lsn') and view(base),
                              'capture position before image replacement')
                assert before['desired'] == 'RUNNING'
            deploy(640, options.upgrade_image)
            replacement = pod()
            assert replacement['metadata']['uid'] != previous['metadata']['uid']
            assert volume() == first and snapshot() == initial
            expected_artifact = artifact_hash(options.upgrade_artifact_sha256)
            assert expected_artifact != record['artifactSha256']
            previous_label = 'upgraded'
            sql('postgres', "UPDATE sales.items SET label='upgraded' WHERE id=1")
            wait(lambda: sql('sync_test', 'SELECT label FROM copy_sales.items') == previous_label,
                 'incremental delivery after the image replacement')
            with forward(client, replacement['metadata']['name']) as base:
                def upgraded_capture():
                    current = view(base)
                    status = current['status']
                    return current if (status.get('epoch', -1) > before['status']['epoch']
                                       and status.get('capture_lsn')
                                       and status['capture_lsn'] != before['status']['capture_lsn']) else None
                after = wait(upgraded_capture, 'new lease and capture position after image replacement')
                assert after['desired'] == 'RUNNING'
            assert snapshot() == initial
            record['upgrade'] = {
                'artifactSha256': expected_artifact,
                'containerImageID': replacement['status']['containerStatuses'][0]['imageID'],
                'initialEpoch': before['status']['epoch'], 'newEpoch': after['status']['epoch'],
            }
            observe('Replacing the application image preserves the original volume and snapshot and resumes CDC without START')

        if options.cross_node:
            previous=pod()
            old_node=previous['spec']['nodeName']
            peers=json.loads(command(*cluster,'get','nodes','-o','json'))['items']
            candidates=[node['metadata']['name'] for node in peers if node['metadata']['name']!=old_node
                        and any(state['type']=='Ready' and state['status']=='True' for state in node['status']['conditions'])]
            assert candidates, 'A second ready node is required for shared CSI acceptance'
            next_node=candidates[0]
            with forward(client,previous['metadata']['name']) as base:
                before=wait(lambda:view(base)['status'].get('capture_lsn') and view(base), 'existing source position')
                assert before['desired']=='RUNNING'
            kube('scale','deployment/rivomesh','--replicas=0')
            wait(lambda:not kube('get','pod',previous['metadata']['name'],'--ignore-not-found','-o','name'), 'old Pod terminates before move')
            sql('postgres',"UPDATE sales.items SET label='moved node' WHERE id=1")
            assert sql('sync_test','SELECT label FROM copy_sales.items')==previous_label
            update={'spec':{'template':{'spec':{'nodeSelector':{'kubernetes.io/hostname':next_node},
                    'tolerations':[{'key':'node-role.kubernetes.io/control-plane','operator':'Exists','effect':'NoSchedule'}]}}}}
            kube('patch','deployment/rivomesh','--type=merge','-p',json.dumps(update))
            kube('scale','deployment/rivomesh','--replicas=1')
            ready()
            replacement=pod()
            assert replacement['metadata']['uid']!=previous['metadata']['uid'] and replacement['spec']['nodeName']==next_node
            assert volume()==first
            wait(lambda:sql('sync_test','SELECT label FROM copy_sales.items')=='moved node','native CDC resumes automatically on the second node')
            assert snapshot()==initial
            with forward(client,replacement['metadata']['name']) as base:
                def moved_capture():
                    current = view(base)
                    status = current['status']
                    return current if (status.get('epoch',-1)>before['status']['epoch']
                                       and status.get('capture_lsn')
                                       and status['capture_lsn']!=before['status']['capture_lsn']) else None
                after=wait(moved_capture, 'new lease and capture position on the second node')
                assert after['desired']=='RUNNING'
            record['crossNode']={'before':old_node,'after':next_node,'initialEpoch':before['status']['epoch'],'newEpoch':after['status']['epoch']}
            observe('The same RWO CSI claim moves to another node and restarts CDC automatically with original snapshot and newer lease')

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
        assert artifact_hash(expected_artifact) == expected_artifact
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


def external_fence(options, record):
    namespace='rivomesh-proof-fence-'+secrets.token_hex(4)
    cluster=['kubectl','--context',options.context]
    client=[*cluster,'-n',namespace]
    nodes=json.loads(command(*cluster,'get','nodes','-o','json'))['items']
    control=next(node['metadata']['name'] for node in nodes
                 if 'node-role.kubernetes.io/control-plane' in node['metadata'].get('labels',{}))
    worker=next(node['metadata']['name'] for node in nodes if node['metadata']['name']!=control)
    actual={line.split()[0] for line in command('minikube','-p',options.context,'node','list').splitlines()}
    assert {control,worker}<=actual, 'Use a disposable Minikube Docker profile'
    password=secrets.token_urlsafe(32)
    paused=None

    def kube(*args,**kwargs):
        return command(*client,*args,**kwargs)

    def apply(item):
        kube('apply','-f','-',data=json.dumps(item))

    def sql(database,statement):
        return kube('exec','pg-fence','--','psql','-XAt','-v','ON_ERROR_STOP=1','-U','postgres','-d',database,'-c',statement)

    def observed(label):
        record['checks'].append(label)
        print('PASS external-fence '+label,flush=True)

    def node_selector(node):
        return {'nodeSelector':{'kubernetes.io/hostname':node},'tolerations':[
            {'key':'node-role.kubernetes.io/control-plane','operator':'Exists','effect':'NoSchedule'}]}

    def launch(pod_name,node):
        worker_config={
            'formatVersion':3,'pipelineId':'external-fence','nodeId':pod_name,'databases':['sales'],
            'source':{'type':'POSTGRESQL','clusterId':'external-fence-pg','database':'postgres','topology':'SINGLE',
                      'expectedVersion':'17','username':'postgres','password':'${PG_FIXTURE_PASSWORD}',
                      'postgresql':{'url':'jdbc:postgresql://pg-fence:5432/postgres?connectTimeout=5',
                                    'publication':'fence_capture','slotPrefix':'fence_capture'}},
            'journal':{'mode':'EXTERNAL','product':'POSTGRESQL',
                       'url':'jdbc:postgresql://pg-fence:5432/journal_store?connectTimeout=5',
                       'username':'postgres','password':'${PG_FIXTURE_PASSWORD}','namespace':'fence_journal'},
            'targets':[{'id':'pg','product':'POSTGRESQL','username':'postgres','password':'${PG_FIXTURE_PASSWORD}',
                        'url':'jdbc:postgresql://pg-fence:5432/sync_test?connectTimeout=5',
                        'metadataSchema':'fence_meta','mappings':{'sales':'copy_sales'}}],
            'leaseMillis':5000,'heartbeatMillis':500,'pollIntervalMillis':100,'retryDelayMillis':300,
        }
        apply(resource('ConfigMap',pod_name,data={'task.json':json.dumps(worker_config)}))
        apply(resource('Pod',pod_name,spec={**node_selector(node),'restartPolicy':'Never','containers':[{
            'name':'rivomesh','image':options.image,'imagePullPolicy':'IfNotPresent',
            'args':['run','/fixture/task.json'],'env':[
                {'name':'JAVA_TOOL_OPTIONS','value':'-Xmx256m'},
                {'name':'PG_FIXTURE_PASSWORD','valueFrom':{'secretKeyRef':{'name':'pg-fence','key':'password'}}}
            ],'volumeMounts':[{'name':'task','mountPath':'/fixture','readOnly':True}]
        }],'volumes':[{'name':'task','configMap':{'name':pod_name}}]}))
        kube('wait','--for=condition=Ready','pod/'+pod_name,'--timeout=120s')
        data=json.loads(kube('get','pod',pod_name,'-o','json'))
        assert data['spec']['nodeName']==node
        backend=data['status']['containerStatuses'][0]['containerID']
        assert backend.startswith('docker://'),'This fixture needs the Minikube Docker runtime'
        digest=kube('exec',pod_name,'--','sha256sum','/app/rivomesh.jar').split()[0]
        if options.artifact_sha256:
            assert digest==options.artifact_sha256
        return backend.removeprefix('docker://'),digest

    def owner():
        row=sql('journal_store',"SELECT owner||'|'||epoch FROM fence_journal.rm3_job WHERE pipeline_id='external-fence'")
        node,epoch=row.split('|')
        return node,int(epoch)

    def contents():
        return sql('sync_test',"SELECT string_agg(id::text||':'||label,',' ORDER BY id) FROM copy_sales.items")

    try:
        command(*cluster,'create','namespace',namespace)
        apply(resource('Secret','pg-fence',type='Opaque',stringData={'password':password}))
        apply(resource('Service','pg-fence',spec={'selector':{'app':'pg-fence'},'ports':[{'port':5432}]}))
        database=resource('Pod','pg-fence',spec={**node_selector(control),'containers':[{
            'name':'postgres','image':'postgres:17.12','args':['-c','wal_level=logical','-c','wal_sender_timeout=4000'],
            'env':[{'name':'POSTGRES_PASSWORD','valueFrom':{'secretKeyRef':{'name':'pg-fence','key':'password'}}}]
        }]})
        database['metadata']['labels']={'app':'pg-fence'}
        apply(database)
        wait(lambda:kube('exec','pg-fence','--','pg_isready','-h','127.0.0.1'),'PostgreSQL fence fixture ready')
        sql('postgres','CREATE DATABASE sync_test')
        sql('postgres','CREATE DATABASE journal_store')
        sql('postgres',"""CREATE SCHEMA sales; CREATE TABLE sales.items(id BIGINT PRIMARY KEY,label TEXT);
            ALTER TABLE sales.items REPLICA IDENTITY FULL; INSERT INTO sales.items VALUES(1,'snapshot');
            CREATE PUBLICATION fence_capture FOR TABLE sales.items WITH (publish='insert,update,delete,truncate')""")
        old,digest=launch('worker-a',control)
        record['artifactSha256']=digest
        wait(lambda:contents()=='1:snapshot','first worker snapshot')
        prior=owner()
        assert prior[0].startswith('worker-a-')
        snapshot=sql('sync_test','SELECT snapshot_id FROM fence_meta.rm3_sink_table')
        _,standby_artifact=launch('worker-b',worker)
        assert standby_artifact==digest and owner()==prior
        observed('Workers on distinct nodes share one external journal and one owner epoch')

        command('docker','exec',control,'docker','pause',old)
        paused=(control,old)
        takeover=wait(lambda:(value if (value:=owner())[0].startswith('worker-b-') and value[1]>prior[1] else None),
                      'standby takes next epoch from the external journal',timeout=60)
        sql('postgres',"UPDATE sales.items SET label='takeover' WHERE id=1; INSERT INTO sales.items VALUES(2,'peer')")
        wait(lambda:contents()=='1:takeover,2:peer','replacement worker delivers through original PostgreSQL slot')
        assert snapshot==sql('sync_test','SELECT snapshot_id FROM fence_meta.rm3_sink_table')
        assert sql('sync_test','SELECT epoch FROM fence_meta.rm3_sink_job')==str(takeover[1])
        observed('An OS-frozen Worker loses its lease and the other node delivers under a newer target epoch')

        command('docker','exec',control,'docker','unpause',old)
        paused=None
        wait(lambda:'PIPELINE_RETRY Fenced' in kube('logs','worker-a'),'stale Worker observes the fence')
        sql('postgres',"UPDATE sales.items SET label='fenced' WHERE id=1; DELETE FROM sales.items WHERE id=2")
        wait(lambda:contents()=='1:fenced','new owner continues after the stale Worker returns')
        assert owner()==takeover and snapshot==sql('sync_test','SELECT snapshot_id FROM fence_meta.rm3_sink_table')
        assert sql('sync_test','SELECT epoch FROM fence_meta.rm3_sink_job')==str(takeover[1])
        observed('The stale process resumes but cannot regain ownership or change the newer target epoch')
        record.update(status='PASS',nodes=[control,worker],initialEpoch=prior[1],takeoverEpoch=takeover[1])
    except Exception as error:
        logs=[str(error)]
        for pod in ('worker-a','worker-b','pg-fence'):
            result=subprocess.run([*client,'logs',pod],cwd=ROOT,text=True,capture_output=True)
            logs.append(result.stdout+result.stderr)
        (options.output/'external-fence-failure.log').write_text('\n'.join(logs).replace(password,'[REDACTED]'))
        raise
    finally:
        if paused:
            command('docker','exec',paused[0],'docker','unpause',paused[1])
        kube('delete','namespace',namespace,'--ignore-not-found','--timeout=180s')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--context', required=True, help='Explicit context for the disposable test cluster')
    parser.add_argument('--image', default='rivomesh:0.1.0-SNAPSHOT')
    parser.add_argument('--artifact-sha256', help='Verify the actual JAR in each live deployment')
    parser.add_argument('--upgrade-image', help='Replace the application with a second loaded image on the same CSI volume')
    parser.add_argument('--upgrade-artifact-sha256', help='Expected SHA-256 of the replacement image JAR')
    parser.add_argument('--storage-class', default='csi-hostpath-sc')
    parser.add_argument('--csi-driver', default='hostpath.csi.k8s.io')
    parser.add_argument('--worker-outage-node', help='Also stop and restart a worker in the named disposable Minikube profile')
    parser.add_argument('--cross-node', action='store_true', help='Drain and move the same RWO workload across two nodes using shared CSI')
    parser.add_argument('--external-fence', action='store_true', help='Validate two-node external-journal takeover from a frozen live Worker')
    parser.add_argument('--output', required=True, type=Path)
    options = parser.parse_args()
    if options.artifact_sha256 and not re.fullmatch('[a-f0-9]{64}',options.artifact_sha256):
        parser.error('Invalid artifact SHA-256')
    if options.upgrade_image and (options.external_fence or options.worker_outage_node):
        parser.error('Run image replacement independently of the fencing and physical outage cases')
    if options.upgrade_artifact_sha256 and (not options.upgrade_image or not re.fullmatch(
            '[a-f0-9]{64}', options.upgrade_artifact_sha256)):
        parser.error('Specify an upgrade image and a valid replacement artifact SHA-256')
    if sum(bool(profile) for profile in (options.external_fence, options.worker_outage_node, options.cross_node))>1:
        parser.error('Run the node outage, external fence and shared CSI relocation independently')
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
        if options.external_fence:
            record={'checks':[],'status':'FAIL'}
            report['profiles']['external-fence']=record
            external_fence(options,record)
            return
        for mode in ('native',) if options.worker_outage_node else ('helm', 'native'):
            record = {'checks': [], 'status': 'FAIL'}
            report['profiles'][mode] = record
            profile(options, mode, record)
    finally:
        (options.output / 'manifest.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
