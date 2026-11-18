#!/usr/bin/env python3
"""Two real controller JVMs and PostgreSQL/MySQL registry: kill/rejoin and storage outage.
Fixtures are isolated and removed; no real credentials, raw state or logs are archived.
"""
import argparse
import base64
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ssl
import threading
import urllib.parse
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('--artifact', type=Path, default=ROOT / 'sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar')
parser.add_argument('--shared-access', action='store_true')
parser.add_argument('--shared-tasks', action='store_true')
parser.add_argument('--tls', action='store_true')
parser.add_argument('--product', choices=('POSTGRESQL', 'MYSQL'), required=True)
args = parser.parse_args()
if args.tls and not args.shared_access:
    parser.error('--tls requires --shared-access')
if args.shared_tasks and not args.shared_access:
    parser.error('--shared-tasks requires --shared-access')
mysql = args.product == 'MYSQL'
jar = args.artifact.resolve()
sha = hashlib.sha256(jar.read_bytes()).hexdigest()
project = 'rivomesh-nodes-' + uuid.uuid4().hex[:10]
env = dict(os.environ, RIVOMESH_NODES_TOKEN=secrets.token_urlsafe(32), RIVOMESH_NODES_DB_PASSWORD=secrets.token_urlsafe(24),RIVOMESH_NODES_TASK_KEY=base64.b64encode(secrets.token_bytes(32)).decode())


def wait_for(check, timeout=75):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, urllib.error.URLError, json.JSONDecodeError):
            pass
        time.sleep(0.25)
    raise AssertionError('Native registry acceptance condition timed out')


with tempfile.TemporaryDirectory(prefix='node-registry-', dir=ROOT / 'target') as temporary:
    home = Path(temporary)
    os.chmod(home, 0o700)
    addresses = {}
    tls_server = None
    tls_origin = None
    trust = None
    if args.tls:
        class Proxy(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def log_message(self, *_):
                pass
            def relay(self):
                parts = self.path.split('/', 3)
                if len(parts) != 4 or parts[1] != '_node' or parts[2] not in addresses:
                    self.send_error(404)
                    return
                target = urllib.parse.urlsplit(addresses[parts[2]])
                connection = http.client.HTTPConnection(target.hostname, target.port, timeout=20)
                try:
                    headers = {key: value for key, value in self.headers.items() if key.lower() not in ('connection', 'transfer-encoding')}
                    content = self.rfile.read(int(self.headers.get('Content-Length', '0')))
                    connection.request(self.command, '/' + parts[3], body=content, headers=headers)
                    response = connection.getresponse()
                    content = response.read()
                    self.send_response(response.status)
                    for key, value in response.getheaders():
                        if key.lower() not in ('connection', 'transfer-encoding', 'content-length', 'server', 'date'):
                            self.send_header(key, value)
                    self.send_header('Content-Length', str(len(content)))
                    self.end_headers()
                    self.wfile.write(content)
                finally:
                    connection.close()
            do_GET = relay
            do_POST = relay
        tls_server = ThreadingHTTPServer(('127.0.0.1', 0), Proxy)
        tls_server.daemon_threads = True
        tls_origin = 'https://127.0.0.1:' + str(tls_server.server_port)
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1', '-subj', '/CN=localhost', '-addext', 'subjectAltName=IP:127.0.0.1', '-keyout', str(home / 'key.pem'), '-out', str(home / 'cert.pem')], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(home / 'cert.pem', home / 'key.pem')
        tls_server.socket = server_context.wrap_socket(tls_server.socket, server_side=True)
        trust = ssl.create_default_context(cafile=str(home / 'cert.pem'))
        threading.Thread(target=tls_server.serve_forever, daemon=True).start()
    services = {'store': {'image': 'mysql:8.4' if mysql else 'postgres:17',
                          'environment': {'MYSQL_ROOT_PASSWORD': '${RIVOMESH_NODES_DB_PASSWORD}', 'MYSQL_DATABASE': 'rivomesh_control'} if mysql else
                          {'POSTGRES_PASSWORD': '${RIVOMESH_NODES_DB_PASSWORD}', 'POSTGRES_DB': 'rivomesh_control'},
                          'tmpfs': ['/var/lib/mysql'] if mysql else ['/var/lib/postgresql/data']}}
    for node in ('node-a', 'node-b'):
        config = {'bind': '0.0.0.0', 'port': 8080, 'authToken': '${RIVOMESH_NODES_TOKEN}',
                  'tasksDirectory': '/fixture/' + node + '/tasks', 'dataDirectory': '/fixture/' + node + '/controller',
                  'pollMillis': 500, 'sessions': {'secureCookie': args.tls, **({'publicOrigin': tls_origin} if args.tls else {})},
                  'cluster': {'id': 'native-registry', 'nodeId': node, 'namespace': 'rivomesh_control',
                              'driver': 'com.mysql.cj.jdbc.Driver' if mysql else 'org.postgresql.Driver',
                              'url': 'jdbc:mysql://store:3306/rivomesh_control?allowPublicKeyRetrieval=true&sslMode=DISABLED' if mysql else 'jdbc:postgresql://store:5432/rivomesh_control',
                              'username': 'root' if mysql else 'postgres', 'password': '${RIVOMESH_NODES_DB_PASSWORD}',
                              'heartbeatMillis': 250, 'leaseMillis': 3000, 'sharedAccess': args.shared_access}}
        if args.shared_tasks:
            config['cluster'].update({'sharedTasks':True,'taskKey':'${RIVOMESH_NODES_TASK_KEY}'})
        (home / (node + '.json')).write_text(json.dumps(config))
        services[node] = {'image': 'eclipse-temurin:21-jre-jammy', 'entrypoint': ['java', '-Xmx512m' if args.shared_tasks else '-Xmx128m', '-jar', '/app/rivomesh.jar', 'serve', '/fixture/' + node + '.json'],
                          'environment': {'RIVOMESH_NODES_TOKEN': '${RIVOMESH_NODES_TOKEN}', 'RIVOMESH_NODES_DB_PASSWORD': '${RIVOMESH_NODES_DB_PASSWORD}','RIVOMESH_NODES_TASK_KEY':'${RIVOMESH_NODES_TASK_KEY}'},
                          'volumes': [str(jar) + ':/app/rivomesh.jar:ro', str(home) + ':/fixture'], 'ports': ['127.0.0.1::8080'], 'stop_grace_period': '30s'}
    if args.shared_access:
        observer = json.loads((home / 'node-b.json').read_text())
        observer['cluster']['sharedAccess'] = False
        observer['cluster'].pop('sharedTasks',None)
        observer['cluster'].pop('taskKey',None)
        observer['cluster']['nodeId'] = 'registry-only'
        observer['tasksDirectory'] = '/fixture/registry-only/tasks'
        observer['dataDirectory'] = '/fixture/registry-only/controller'
        (home / 'registry-only.json').write_text(json.dumps(observer))
        services['registry-only'] = {**services['node-b'], 'entrypoint': ['java', '-Xmx128m', '-jar', '/app/rivomesh.jar', 'serve', '/fixture/registry-only.json']}
    composition = home / 'compose.json'
    composition.write_text(json.dumps({'services': services}))

    def compose(*arguments, capture=False, check=True):
        return subprocess.run(['docker', 'compose', '-p', project, '-f', str(composition), *arguments], env=env, cwd=ROOT, check=check,
                              stdout=subprocess.PIPE if capture else subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, timeout=150)

    def request(base, path, body=None, authenticated=True):
        headers = {'Authorization': 'Bearer ' + env['RIVOMESH_NODES_TOKEN']} if authenticated else {}
        if body is not None:
            headers.update({'Content-Type': 'application/json', 'X-Request-ID': 'native_nodes_' + uuid.uuid4().hex})
        try:
            response = urllib.request.urlopen(urllib.request.Request(base + path, data=json.dumps(body).encode() if body is not None else None, headers=headers), timeout=20)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return response.status, json.loads(response.read())

    def browser_request(browser, base, path='/api/session', token=None, logout=False):
        controller_base = base
        direct = urllib.parse.urlsplit(base)
        browser_origin = direct.scheme + '://' + direct.netloc
        if args.tls:
            node = next(node for node, url in addresses.items() if url == base)
            base = tls_origin + '/_node/' + node
            browser_origin = tls_origin
        headers = {'Origin': browser_origin} if token is not None or logout else {}
        if token is not None:
            headers['Authorization'] = 'Bearer ' + token
        if logout:
            headers['X-CSRF-Token'] = browser_request(browser, controller_base)[1]['csrfToken']
        req = urllib.request.Request(base + path, data=b'' if token is not None or logout else None, headers=headers)
        try:
            response = browser.open(req, timeout=20)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return response.status, json.loads(response.read())

    shared_browser = None
    try:
        compose('up', '-d', 'store')
        # pg_isready/mysqladmin use only localhost inside this isolated fixture.
        probe = ['mysqladmin', 'ping', '-h', '127.0.0.1', '--silent'] if mysql else ['pg_isready', '-h', '127.0.0.1', '-U', 'postgres', '-d', 'rivomesh_control']
        wait_for(lambda: compose('exec', '-T', 'store', *probe, capture=True, check=False).returncode == 0)
        compose('up', '-d', 'node-a', 'node-b')  # Concurrent DDL bootstrap and registration.
        addresses = {node: 'http://' + compose('port', node, '8080', capture=True).stdout.strip() for node in ('node-a', 'node-b')}
        for base in addresses.values():
            wait_for(lambda: request(base, '/readyz', authenticated=False)[0] == 200)
        for base in addresses.values():
            wait_for(lambda: len(request(base, '/api/nodes')[1].get('nodes', [])) == 2)
            status, page = request(base, '/api/nodes')
            assert status == 200 and {row['id'] for row in page['nodes']} == {'node-a', 'node-b'}
            assert page['taskControlScope'] == ('GLOBAL' if args.shared_tasks else 'LOCAL')
            assert all(row['artifactSha256'] == sha and row['jvm'] and row['architecture'] for row in page['nodes'])
        print(args.product + ': PASS concurrent two-node registration and actual artifact/runtime metadata', flush=True)
        if args.shared_access:
            import http.cookiejar
            shared_browser = urllib.request.build_opener(urllib.request.HTTPSHandler(context=trust) if args.tls else urllib.request.HTTPHandler(), urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
            assert browser_request(shared_browser, addresses['node-a'], token=env['RIVOMESH_NODES_TOKEN'])[0] == 200
            assert browser_request(shared_browser, addresses['node-b'])[0] == 200
            status, accounts = request(addresses['node-a'], '/api/accounts')
            assert status == 200 and accounts['authority'] == 'SHARED'
            viewer_token, rotated = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
            assert request(addresses['node-a'], '/api/accounts', {'expectedRevision': accounts['revision'], 'id': 'native-viewer', 'role': 'VIEWER', 'enabled': True, 'taskIds': [], 'token': viewer_token})[0] == 201
            assert browser_request(shared_browser, addresses['node-b'])[0] == 401
            assert browser_request(shared_browser, addresses['node-a'], token=viewer_token)[0] == 200
            assert browser_request(shared_browser, addresses['node-b'])[1]['role'] == 'VIEWER'
            status, accounts = request(addresses['node-b'], '/api/accounts')
            assert request(addresses['node-b'], '/api/accounts/native-viewer/token', {'expectedRevision': accounts['revision'], 'token': rotated})[0] == 200
            assert browser_request(shared_browser, addresses['node-a'])[0] == 401
            assert browser_request(shared_browser, addresses['node-b'], token=rotated)[0] == 200
            assert browser_request(shared_browser, addresses['node-a'])[0] == 200
            audit_one = request(addresses['node-a'], '/api/audit?limit=1000')[1]
            audit_two = request(addresses['node-b'], '/api/audit?limit=1000')[1]
            assert audit_one['storeId'] == audit_two['storeId'] and audit_two['authority'] == 'SHARED'
            for page in (audit_one, audit_two):
                previous = page['previousHash']
                for item in page['events']:
                    saved_hash = item['hash']
                    assert item['previousHash'] == previous
                    payload = {key: value for key, value in item.items() if key not in ('hash', 'previousHash')}
                    actual = hashlib.sha256((str(item['sequence']) + '\n' + previous + '\n' + json.dumps(payload, ensure_ascii=False, separators=(',', ':'))).encode()).hexdigest()
                    assert actual == saved_hash
                    previous = saved_hash
                assert not any(secret in json.dumps(page) for secret in (env['RIVOMESH_NODES_TOKEN'], viewer_token, rotated))
            print(args.product + (' HTTPS' if args.tls else '') + ': PASS shared login, cross-node account/token revocation and one verified audit chain', flush=True)

        task = {'jobId': 'node-a-orders', 'source': {'host': 'unused', 'database': 'orders', 'username': 'unused', 'password': 'source-not-used', 'serverId': 918111},
                'target': {'type': 'POSTGRESQL', 'url': 'jdbc:postgresql://127.0.0.1:1/unused?connectTimeout=1&socketTimeout=1', 'username': 'unused', 'password': 'target-not-used', 'schema': 'orders', 'metadataSchema': 'rivomesh_meta'}}
        assert request(addresses['node-a'], '/api/tasks', {'config': task})[0] == (202 if args.shared_tasks else 201)
        if args.shared_tasks:
            wait_for(lambda: request(addresses['node-b'],'/api/tasks/node-a-orders')[1].get('activeConfigurationVersion'))
            other=json.loads(json.dumps(task));other['jobId']='node-b-orders';other['source']['serverId']+=1;other['target']['schema']='orders_b'
            assert request(addresses['node-a'],'/api/tasks',{'config':other,'ownerNodeId':'node-b'})[0]==202
            wait_for(lambda: request(addresses['node-a'],'/api/tasks/node-b-orders')[1].get('activeConfigurationVersion'))
            for base in addresses.values():
                view=request(base,'/api/tasks/node-b-orders')[1]
                assert view['ownerNodeId']=='node-b' and view['authority']=='SHARED'
            print(args.product+': PASS actual independent JVMs accept at A, create at B and read one global task/config authority',flush=True)
        wait_for(lambda: any(t['id'] == 'node-a-orders' for row in request(addresses['node-b'], '/api/nodes')[1]['nodes'] for t in row['tasks']))
        old_epoch = next(row['epoch'] for row in request(addresses['node-b'], '/api/nodes')[1]['nodes'] if row['id'] == 'node-a')
        compose('kill', '-s', 'KILL', 'node-a')
        wait_for(lambda: any(row['id'] == 'node-a' and row['state'] == 'STALE' and row['tasks'] for row in request(addresses['node-b'], '/api/nodes')[1]['nodes']))
        if args.shared_tasks:
            offline=request(addresses['node-b'],'/api/tasks/node-a-orders')[1]
            assert offline['workerState']=='UNKNOWN'
            assert request(addresses['node-b'],'/api/tasks/node-a-orders/actions',{'action':'STOP','expectedRevision':offline['revision']})[0]==202
            pending=request(addresses['node-b'],'/api/tasks/node-a-orders')[1]
            assert pending['workerState']=='UNKNOWN' and any(op.get('action')=='STOP' and op.get('deliveryStatus')=='PENDING' for op in pending['operations'])
        compose('start', 'node-a')
        addresses['node-a'] = 'http://' + compose('port', 'node-a', '8080', capture=True).stdout.strip()
        wait_for(lambda: request(addresses['node-a'], '/readyz', authenticated=False)[0] == 200)
        wait_for(lambda: any(row['id'] == 'node-a' and row['epoch'] > old_epoch and row['state'] == 'HEALTHY' for row in request(addresses['node-b'], '/api/nodes')[1]['nodes']))
        if shared_browser is not None:
            assert browser_request(shared_browser, addresses['node-a'])[0] == 200
            assert browser_request(shared_browser, addresses['node-b'])[0] == 200
        assert any(row['id'] == 'node-a-orders' for row in request(addresses['node-a'], '/api/tasks')[1]['tasks'])
        if args.shared_tasks:
            wait_for(lambda:any(op.get('action')=='STOP' and op.get('status')=='SUCCEEDED' for op in request(addresses['node-b'],'/api/tasks/node-a-orders')[1]['operations']))
            print(args.product+': PASS offline STOP stays pending/UNKNOWN and executes only after fixed-owner directory reopens',flush=True)
        print(args.product + ': PASS remote task observation, kill/stale and persistent-directory rejoin with new epoch', flush=True)
        compose('pause', 'store')
        try:
            time.sleep(4)
            assert request(addresses['node-a'], '/healthz', authenticated=False)[0] == 200
            assert request(addresses['node-a'], '/readyz', authenticated=False)[0] == 503
            assert request(addresses['node-a'], '/api/nodes')[0] == 503
        finally:
            compose('unpause', 'store')
        wait_for(lambda: request(addresses['node-a'], '/readyz', authenticated=False)[0] == 200)
        wait_for(lambda: all(row['state'] == 'HEALTHY' for row in request(addresses['node-a'], '/api/nodes')[1]['nodes']))
        print(args.product + ': PASS store outage returns503, readiness recovers and nodes renew after expiry', flush=True)
        if shared_browser is not None:
            assert browser_request(shared_browser, addresses['node-a'])[0] == 200
            assert browser_request(shared_browser, addresses['node-b'], '/api/session/logout', logout=True)[0] == 200
            assert browser_request(shared_browser, addresses['node-a'])[0] == 401
            print(args.product + ': PASS shared session survives controller kill/reopen and storage outage; logout revokes across nodes', flush=True)
        if mysql and args.shared_access:
            compose('stop', 'node-a', 'node-b')
            compose('exec', '-T', 'store', 'sh', '-c', 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot -e "ALTER TABLE rivomesh_control.rmc_session ENGINE=MyISAM"')
            compose('start', 'node-a')
            wait_for(lambda: 'node-a' in compose('ps', '-a', '--status', 'exited', '--services', capture=True).stdout.splitlines())
            rejected = compose('logs', '--no-color', '--tail', '60', 'node-a', capture=True).stdout
            assert 'CONTROL_STORE_ENGINE_INVALID' in rejected
            compose('exec', '-T', 'store', 'sh', '-c', 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot -e "ALTER TABLE rivomesh_control.rmc_session ENGINE=InnoDB"')
            compose('start', 'node-a', 'node-b')
            for node in ('node-a', 'node-b'):
                addresses[node] = 'http://' + compose('port', node, '8080', capture=True).stdout.strip()
                wait_for(lambda: request(addresses[node], '/readyz', authenticated=False)[0] == 200)
            assert request(addresses['node-a'], '/api/accounts')[1]['authority'] == 'SHARED'
            print(args.product + ': PASS nontransactional session table refused; InnoDB and existing authority recover', flush=True)
        if args.shared_access:
            compose('up', '-d', 'registry-only')
            wait_for(lambda: 'registry-only' in compose('ps', '-a', '--status', 'exited', '--services', capture=True).stdout.splitlines())
            rejected = compose('logs', '--no-color', '--tail', '60', 'registry-only', capture=True).stdout
            assert 'CONTROL_STORE_PROTOCOL_MISMATCH' in rejected
            print(args.product + ': PASS registry-only member refused by the shared-identity protocol', flush=True)
        print('artifactSha256=' + sha, flush=True)
    except Exception:
        diagnostic = compose('logs', '--no-color', '--tail', '80', 'node-a', 'node-b', capture=True, check=False).stdout
        for value in (env['RIVOMESH_NODES_TOKEN'], env['RIVOMESH_NODES_DB_PASSWORD'],env['RIVOMESH_NODES_TASK_KEY']):
            diagnostic = diagnostic.replace(value, '[redacted]')
        (ROOT / ('target/nodes-' + args.product.lower() + '-diagnostic.log')).write_text(diagnostic)
        raise
    finally:
        if tls_server is not None:
            tls_server.shutdown()
            tls_server.server_close()
        compose('unpause', 'store', check=False)
        compose('down', '-v', '--remove-orphans', check=False)
