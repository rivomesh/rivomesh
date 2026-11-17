#!/usr/bin/env python3
"""Two controller JVMs, real MySQL CDC and PostgreSQL/MySQL targets; isolated fixtures."""
import argparse
import base64
import hashlib
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
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--artifact', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--initial-owner-artifact', type=Path, help='Reproduce old multi-target receipt fault, then reopen owner with --artifact')
parser.add_argument('--separate-execution', action='store_true', help='Two management JVMs plus a separate probe-only execution Agent')
parser.add_argument('--control-product', choices=('POSTGRESQL', 'MYSQL'), default='POSTGRESQL')
parser.add_argument('--runtime-image', help='Locally built nonroot runtime image; separate Agent profile only')
parser.add_argument('--failure-diagnostics', action='store_true', help='Check phase-only cross-entry status and persistent SQL failure after Worker restart')
args = parser.parse_args()
if args.initial_owner_artifact and args.separate_execution:
    parser.error('The old receipt regression is protocol3; use separate fixtures for the protocol4 lifecycle')
if args.control_product == 'MYSQL' and not args.separate_execution:
    parser.error('MySQL control-store business acceptance requires --separate-execution')
if args.runtime_image and not args.separate_execution:
    parser.error('--runtime-image requires --separate-execution')
if args.failure_diagnostics and not args.separate_execution:
    parser.error('--failure-diagnostics requires --separate-execution')
jar = args.artifact.resolve()
digest = hashlib.sha256(jar.read_bytes()).hexdigest()
initial_owner = args.initial_owner_artifact.resolve() if args.initial_owner_artifact else jar
initial_digest = hashlib.sha256(initial_owner.read_bytes()).hexdigest()
output = args.output.resolve()
output.relative_to(ROOT)
output.mkdir(parents=True, exist_ok=True)
project = 'rivomesh-global-cdc-' + uuid.uuid4().hex[:10]
env = dict(os.environ, RM_GC_TOKEN=secrets.token_urlsafe(32), RM_GC_CONTROL=secrets.token_urlsafe(24),
           RM_GC_DATA=secrets.token_urlsafe(24), RM_GC_KEY=base64.b64encode(secrets.token_bytes(32)).decode(), RM_GC_STORE_URL='jdbc:postgresql://postgres:5432/sync_test')
owner_node = 'node-worker' if args.separate_execution else 'node-b'
members = ('node-a', 'node-b', 'node-worker') if args.separate_execution else ('node-a', 'node-b')
if args.separate_execution:
    env['RM_GC_STORE_URL'] = 'jdbc:mysql://store:3306/rivomesh_control?allowPublicKeyRetrieval=true&sslMode=DISABLED' if args.control_product == 'MYSQL' else 'jdbc:postgresql://store:5432/rivomesh_control'
checks = []
diagnostics = {}
manifest = {'artifactSha256': digest, 'status': 'RUNNING', 'checks': checks,
            'harnessSourceSha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'executionProfile': 'SEPARATE_AGENT' if args.separate_execution else 'COMBINED',
            'controlProduct': args.control_product,
            'failureDiagnostics': args.failure_diagnostics,
            'notCovered': 'Automatic owner relocation, live Kubernetes/CSI, source promotion and full connector matrix'}
if args.initial_owner_artifact:
    manifest['initialOwnerArtifactSha256'] = initial_digest


def passed(name):
    checks.append(name)
    print('PASS ' + name, flush=True)


def wait(check, label, seconds=120):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, urllib.error.URLError, json.JSONDecodeError, subprocess.CalledProcessError):
            pass
        time.sleep(.3)
    raise AssertionError('Timed out: ' + label)


with tempfile.TemporaryDirectory(prefix='global-cdc-', dir=ROOT / 'target') as temporary:
    home = Path(temporary)
    home.chmod(0o755 if args.runtime_image else 0o700)  # Optional bind holds ENV references only; actual private state stays in named volumes.
    volumes = {}
    services = {
        'mysql': {'image': 'mysql:8.4', 'environment': {'MYSQL_ROOT_PASSWORD': '${RM_GC_DATA}', 'MYSQL_ROOT_HOST': '%'},
                  'command': ['--server-id=1', '--log-bin=mysql-bin', '--binlog-format=ROW', '--binlog-row-image=FULL',
                              '--gtid-mode=ON', '--enforce-gtid-consistency=ON', '--log-replica-updates=ON']},
        'mysql-target': {'image': 'mysql:8.4', 'environment': {'MYSQL_ROOT_PASSWORD': '${RM_GC_DATA}', 'MYSQL_ROOT_HOST': '%'},
                         'command': ['--server-id=90']},
        'postgres': {'image': 'postgres:17', 'environment': {'POSTGRES_PASSWORD': '${RM_GC_CONTROL}', 'POSTGRES_DB': 'sync_test'}}}
    if args.separate_execution:
        services['store'] = {'image': 'mysql:8.4', 'environment': {'MYSQL_ROOT_PASSWORD': '${RM_GC_CONTROL}', 'MYSQL_ROOT_HOST': '%', 'MYSQL_DATABASE': 'rivomesh_control'}} if args.control_product == 'MYSQL' else {'image': 'postgres:17', 'environment': {'POSTGRES_PASSWORD': '${RM_GC_CONTROL}', 'POSTGRES_DB': 'rivomesh_control'}}
    for node in members:
        config = {'bind': '0.0.0.0', 'port': 8080, 'authToken': '${RM_GC_TOKEN}', 'pollMillis': 200,
                  'sessions': {'secureCookie': False}, 'maxRunningTasks': 2, 'workerHeapMiB': 256,
                  'tasksDirectory': '/fixture/' + node + '/tasks', 'dataDirectory': '/fixture/' + node + '/controller',
                  'cluster': {'id': 'global-cdc', 'nodeId': node, 'sharedAccess': True, 'sharedTasks': True,
                              'taskKey': '${RM_GC_KEY}', 'driver': 'org.postgresql.Driver',
                              'url': 'jdbc:postgresql://postgres:5432/sync_test', 'namespace': 'global_control',
                              'username': 'postgres', 'password': '${RM_GC_CONTROL}', 'heartbeatMillis': 300, 'leaseMillis': 5000}}
        if args.separate_execution:
            config['cluster'].update({'separateExecution': True, 'role': 'AGENT' if node == owner_node else 'CONTROLLER', 'url': '${RM_GC_STORE_URL}', 'driver': 'com.mysql.cj.jdbc.Driver' if args.control_product == 'MYSQL' else 'org.postgresql.Driver', 'username': 'root' if args.control_product == 'MYSQL' else 'postgres'})
        if args.runtime_image:
            config.update({'tasksDirectory': '/app/data/tasks', 'dataDirectory': '/app/data/controller'})
        (home / (node + '.json')).write_text(json.dumps(config))
        services[node] = {'image': 'eclipse-temurin:8-jre', 'entrypoint': ['java', '-Xmx512m', '-jar', '/app/rivomesh.jar', 'agent' if args.separate_execution and node == owner_node else 'serve', '/fixture/' + node + '.json'],
                          'environment': {'RM_GC_TOKEN': '${RM_GC_TOKEN}', 'RM_GC_CONTROL': '${RM_GC_CONTROL}', 'RM_GC_KEY': '${RM_GC_KEY}', 'RM_GC_STORE_URL': '${RM_GC_STORE_URL}'},
                          'volumes': [str(jar) + ':/app/rivomesh.jar:ro', str(home) + ':/fixture'],
                          'ports': ['127.0.0.1::8080'], 'stop_grace_period': '30s'}
        if args.runtime_image:
            volumes[node + '-data'] = {}
            services[node]['image'] = args.runtime_image
            services[node]['volumes'] = [str(jar) + ':/app/rivomesh.jar:ro', str(home) + ':/fixture:ro', node + '-data:/app/data']
    composition = home / 'compose.json'
    if args.initial_owner_artifact:
        services['node-b']['volumes'][0] = str(initial_owner) + ':/app/rivomesh.jar:ro'
    composition.write_text(json.dumps({'services': services, 'volumes': volumes}))

    def compose(*arguments, check=True):
        return subprocess.run(['docker', 'compose', '-p', project, '-f', str(composition), *arguments], env=env,
                              cwd=ROOT, check=check, capture_output=True, text=True, timeout=180)

    def sql(service, statement):
        if service == 'postgres' or service == 'store' and args.control_product == 'POSTGRESQL':
            result = compose('exec', '-T', service, 'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'sync_test' if service == 'postgres' else 'rivomesh_control', '-A', '-t', '-F', '|', '-c', statement)
        else:
            result = compose('exec', '-T', '-e', 'MYSQL_PWD=' + env['RM_GC_CONTROL' if service == 'store' else 'RM_GC_DATA'], service, 'mysql', '-uroot', '-N', '-B', '-e', statement)
        return result.stdout.strip().replace('\t', '|')

    bases = {}

    def address(node):
        port = compose('port', node, '8080').stdout.strip().rsplit(':', 1)[1]
        bases[node] = 'http://127.0.0.1:' + port

    def worker_pids():
        container = compose('ps', '-q', owner_node).stdout.strip()
        lines = subprocess.check_output(['docker', 'top', container, '-eo', 'pid,args'], text=True).splitlines()
        return {line.split()[0] for line in lines if 'io.rivomesh.Main run ' in line}

    def http(node, path, body=None, method=None, request=None, expected=200):
        headers = {'Authorization': 'Bearer ' + env['RM_GC_TOKEN']}
        if body is not None:
            headers.update({'Content-Type': 'application/json', 'X-Request-ID': request or 'global_cdc_' + uuid.uuid4().hex})
        call = urllib.request.Request(bases[node] + path, data=json.dumps(body).encode() if body is not None else None, headers=headers, method=method)
        try:
            response = urllib.request.urlopen(call, timeout=20)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            value = json.loads(response.read())
            assert response.status == expected, 'HTTP ' + str(response.status) + ': ' + str(value.get('code', 'unexpected response'))
            return value

    route = '/api/tasks/global_cdc'

    def task(node='node-a'):
        return http(node, route)

    def outcome(request, node='node-a'):
        for operation in task(node).get('operations', []):
            if operation['id'] == request:
                if operation['status'] in ('FAILED', 'REJECTED', 'CANCELLED'):
                    raise AssertionError('Command failed: ' + request + '/' + str(operation.get('code')))
                return operation['status'] == 'SUCCEEDED'
        return False

    def operation(request):
        return next(op for op in task()['operations'] if op['id'] == request)

    def action(name, node='node-a', complete=True, **options):
        request = 'gc_' + name.lower() + '_' + uuid.uuid4().hex
        body = dict(action=name, expectedRevision=task(node)['revision'], **options)
        accepted = http(node, route + '/actions', body, request=request, expected=202)
        if complete:
            wait(lambda: outcome(request, node), name)
        return request, accepted

    def equal_rows():
        for service, prefix in [('postgres', 'rm_pg'), ('mysql-target', 'rm_my')]:
            assert sql(service, 'SELECT id,note,derived FROM ' + prefix + '_sales.items ORDER BY id') == sql('mysql', 'SELECT id,note,derived FROM sales.items ORDER BY id')
            assert sql(service, 'SELECT value,COUNT(*) FROM ' + prefix + '_sales.duplicates GROUP BY value ORDER BY value') == sql('mysql', 'SELECT value,COUNT(*) FROM sales.duplicates GROUP BY value ORDER BY value')
            assert sql(service, 'SELECT id,amount FROM ' + prefix + '_billing.ledger ORDER BY id') == sql('mysql', 'SELECT id,amount FROM billing.ledger ORDER BY id')
            expr = "payload->>'n'" if service == 'postgres' else "JSON_UNQUOTE(JSON_EXTRACT(payload,'$.n'))"
            assert sql(service, 'SELECT id,' + expr + ' FROM ' + prefix + '_sales.items ORDER BY id') == sql('mysql', "SELECT id,JSON_UNQUOTE(JSON_EXTRACT(payload,'$.n')) FROM sales.items ORDER BY id")
        return True

    def rows_match():
        try:
            return equal_rows()
        except AssertionError:
            diagnostics['rowMismatch'] = True
            return False
        except subprocess.CalledProcessError as failure:
            diagnostics['sqlError'] = failure.stderr[-1500:]
            return False

    try:
        databases = ('mysql', 'mysql-target', 'postgres', 'store') if args.separate_execution else ('mysql', 'mysql-target', 'postgres')
        compose('up', '-d', *databases)
        for service in databases:
            wait(lambda s=service: sql(s, 'SELECT 1') == '1', service + ' SQL ready')
        # The business role is distinct from controller credentials and is allowed into Worker configuration.
        sql('postgres', "CREATE ROLE sync_target LOGIN PASSWORD '" + env['RM_GC_DATA'] + "'; GRANT CREATE ON DATABASE sync_test TO sync_target")
        sql('mysql', "CREATE DATABASE sales; CREATE DATABASE billing; CREATE TABLE sales.items(id BIGINT PRIMARY KEY,note VARCHAR(50),payload JSON,derived BIGINT GENERATED ALWAYS AS(id*2) VIRTUAL); INSERT INTO sales.items(id,note,payload) VALUES(1,'before',JSON_OBJECT('n',1)),(2,'second',JSON_OBJECT('n',2)); CREATE TABLE sales.duplicates(value INT,payload JSON); INSERT INTO sales.duplicates VALUES(7,JSON_OBJECT('n',7)),(7,JSON_OBJECT('n',7)); CREATE TABLE billing.ledger(id BIGINT PRIMARY KEY,amount DECIMAL(20,4)); INSERT INTO billing.ledger VALUES(1,123.4567)")
        compose('up', '-d', *members)
        for node in ('node-a', 'node-b'):
            address(node)
            wait(lambda n=node: http(n, '/api/session')['taskControlScope'] == 'GLOBAL', node + ' global admission')
        if args.separate_execution:
            address(owner_node)
            wait(lambda: http(owner_node, '/readyz')['status'] == 'UP', 'Agent ready')
            for path in ('/', '/api/session', '/api/tasks', '/api/nodes'):
                assert http(owner_node, path, expected=404)['code'] == 'AGENT_PROBE_ONLY'
            assert http('node-a', '/api/session')['nodeRole'] == 'CONTROLLER'
            passed('Agent command opens probes only; management roles are visible through controller sessions')
            if args.runtime_image:
                for node in members:
                    assert compose('exec', '-T', node, 'id', '-u').stdout.strip() == '10001'
                manifest['runtimeImage'] = args.runtime_image
                passed('Every controller and Agent runs as UID10001 with independent named data volumes')
        document = {'formatVersion': 3, 'pipelineId': 'global_cdc', 'nodeId': 'cdc-worker',
                    'source': {'clusterId': 'global-cdc-source', 'host': 'mysql', 'username': 'root', 'password': env['RM_GC_DATA'], 'database': 'sales', 'serverId': 992201},
                    'databases': ['sales', 'billing'], 'chunkSize': 1, 'pollIntervalMillis': 100, 'retryDelayMillis': 200, 'leaseMillis': 5000, 'heartbeatMillis': 300,
                    'journal': {'mode': 'LOCAL'}, 'targets': []}
        for product, target, url, username in [('POSTGRESQL', 'pg', 'jdbc:postgresql://postgres:5432/sync_test', 'sync_target'), ('MYSQL', 'my', 'jdbc:mysql://mysql-target:3306/?allowPublicKeyRetrieval=true&sslMode=DISABLED', 'root')]:
            document['targets'].append({'id': target, 'product': product, 'url': url, 'username': username, 'password': env['RM_GC_DATA'], 'metadataSchema': 'rm_' + target + '_meta', 'mappings': {'sales': 'rm_' + target + '_sales', 'billing': 'rm_' + target + '_billing'}})
        created = 'gc_create_' + uuid.uuid4().hex
        http('node-a', '/api/tasks', {'config': document, 'ownerNodeId': owner_node}, request=created, expected=202)
        wait(lambda: outcome(created), 'remote create')
        assert task()['ownerNodeId'] == owner_node
        assert task('node-b')['revision'] == task()['revision']
        passed('Controllers create Agent-owned task and share revision' if args.separate_execution else 'A creates B-owned task; both JVMs share task revision')
        action('CHECK')
        action('START')
        wait(rows_match, 'snapshot exact rows')
        passed('Two source databases to two targets; snapshot keyed/keyless/JSON/generated values')
        sql('mysql', "START TRANSACTION; UPDATE sales.items SET note='updated',payload=JSON_OBJECT('n',11) WHERE id=1; DELETE FROM sales.items WHERE id=2; INSERT INTO sales.items(id,note,payload) VALUES(3,'third',JSON_OBJECT('n',3)); UPDATE sales.duplicates SET value=8 WHERE value=7 LIMIT 1; UPDATE billing.ledger SET amount=456.7890 WHERE id=1; COMMIT")
        wait(rows_match, 'incremental exact rows')
        passed('Cross-database transaction insert/update/delete and keyless multiplicity')
        if args.separate_execution:
            original_workers = worker_pids()
            assert len(original_workers) == 1
            compose('kill', '-s', 'SIGKILL', 'node-a')
            sql('mysql', "INSERT INTO sales.items(id,note,payload) VALUES(4,'entry-a-offline',JSON_OBJECT('n',4))")
            wait(rows_match, 'CDC continues with first management entry dead')
            assert http('node-b', route)['ownerNodeId'] == owner_node
            action('CHECK', node='node-b')
            passed('The surviving management entry executes an authenticated remote command after the first entry dies')
            compose('kill', '-s', 'SIGKILL', 'node-b')
            sql('mysql', "INSERT INTO sales.items(id,note,payload) VALUES(5,'all-entries-offline',JSON_OBJECT('n',5))")
            wait(rows_match, 'CDC continues with all management entries dead')
            running = compose('ps', '--status', 'running', '--services').stdout.split()
            assert owner_node in running and 'node-a' not in running and 'node-b' not in running
            assert worker_pids() == original_workers
            passed('Actual Agent/Worker CDC continues after SIGKILL of one and then every management container')
            clock = 'CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED)' if args.control_product == 'MYSQL' else 'CAST(EXTRACT(EPOCH FROM clock_timestamp())*1000 AS BIGINT)'
            wait(lambda: sql('store', "SELECT COUNT(*) FROM global_control.rmc_node WHERE cluster_id='global-cdc' AND node_id IN ('node-a','node-b') AND lifecycle='RUNNING' AND lease_until>" + clock) == '0', 'old management leases expire before rejoin')
            compose('up', '-d', 'node-a', 'node-b')
            for node in ('node-a', 'node-b'):
                address(node)
                wait(lambda n=node: http(n, '/api/session')['nodeRole'] == 'CONTROLLER', 'management rejoin ' + node)
            assert task()['ownerNodeId'] == owner_node
            passed('Management rejoin observes the original Agent and committed rows without moving owner identity')
            compose('stop', '--timeout', '5', 'store')
            sql('mysql', "INSERT INTO sales.items(id,note,payload) VALUES(6,'control-store-offline',JSON_OBJECT('n',6))")
            wait(rows_match, 'LOCAL journal CDC continues with control store stopped')
            time.sleep(6)  # The database-clock registration lease has expired; Worker identity must still be unchanged.
            assert http('node-a', '/readyz', expected=503)['status'] == 'CONTROL_STORE_UNAVAILABLE'
            assert http('node-b', '/api/tasks', expected=503)['code'] == 'CONTROL_STORE_UNAVAILABLE'
            assert http(owner_node, '/readyz', expected=503)['status'] == 'CONTROL_STORE_UNAVAILABLE'
            assert worker_pids() == original_workers
            compose('start', 'store')
            wait(lambda: sql('store', 'SELECT 1') == '1', 'control store restart')
            wait(lambda: task().get('ownerState') == 'HEALTHY', 'Agent renews registration after store outage')
            assert worker_pids() == original_workers
            passed('LOCAL journal CDC retains the same Worker during control-store outage; management refuses control and recovers after lease expiry')
        sql('postgres', 'ALTER TABLE rm_pg_sales.items ADD CONSTRAINT gc_block CHECK(id<>9)')
        sql('mysql', "INSERT INTO sales.items(id,note,payload) VALUES(9,'sql-error',JSON_OBJECT('n',9))")

        def failed_target():
            for sub in task().get('status', {}).get('subscriptions', []):
                if sub['id'] == 'pg' and sub.get('state') in ('PAUSED', 'ERROR'):
                    return sub
            return None

        failed = wait(failed_target, 'target SQL error pause')
        assert sql('postgres', 'SELECT COUNT(*) FROM rm_pg_sales.items WHERE id=9') == '0'
        if args.failure_diagnostics:
            def phase_matches(node):
                sub = next(s for s in task(node)['status']['subscriptions'] if s['id'] == 'pg')
                return sub.get('failurePhase') == 'TARGET_APPLY' and sub.get('failedSequence') == sub['acknowledged'] + 1 and sub['errorGeneration'] == failed['errorGeneration']
            for node in ('node-a', 'node-b'):
                wait(lambda n=node: phase_matches(n), 'phase diagnostic through ' + node)
                assert 'sql-error' not in json.dumps(task(node)['status'])
            action('STOP', node='node-b')
            action('START')
            sql('mysql', "INSERT INTO sales.items(id,note,payload) VALUES(13,'paused-worker-restarted',JSON_OBJECT('n',13))")
            wait(lambda: sql('mysql-target', 'SELECT COUNT(*) FROM rm_my_sales.items WHERE id=13') == '1', 'new Worker continues healthy destination while pg remains paused')
            for node in ('node-a', 'node-b'):
                wait(lambda n=node: phase_matches(n), 'retained failure after Worker restart through ' + node)
            assert sql('postgres', 'SELECT COUNT(*) FROM rm_pg_sales.items WHERE id IN (9,13)') == '0'
            passed('Both management entries expose value-free TARGET_APPLY diagnostics; Worker restart retains the same error generation and failed sequence while healthy delivery continues')
        sql('postgres', 'ALTER TABLE rm_pg_sales.items DROP CONSTRAINT gc_block')
        action('RESUME_TARGET', errorCount=failed['errorGeneration'], subscriptionId='pg')
        wait(rows_match, 'SQL error replay')
        passed('Native SQL constraint error pauses target; explicit generation-bound replay catches up')
        compose('kill', '-s', 'SIGKILL', owner_node)
        wait(lambda: task().get('ownerState') != 'HEALTHY', 'owner offline')
        sql('mysql', "INSERT INTO sales.items(id,note,payload) VALUES(10,'offline',JSON_OBJECT('n',10))")
        stop, accepted = action('STOP', complete=False)
        assert next(op for op in accepted['operations'] if op['id'] == stop)['deliveryStatus'] == 'PENDING'
        assert not outcome(stop)
        if args.separate_execution:
            compose('kill', '-s', 'SIGKILL', 'node-a')
            assert not outcome(stop, 'node-b')
        compose('up', '-d', owner_node)
        address(owner_node)
        surviving_entry = 'node-b' if args.separate_execution else 'node-a'
        wait(lambda: outcome(stop, surviving_entry), 'offline STOP after reopen')
        wait(lambda: task(surviving_entry).get('workerState') == 'STOPPED', 'worker stopped')
        time.sleep(2)
        assert sql('postgres', 'SELECT COUNT(*) FROM rm_pg_sales.items WHERE id=10') == '0'
        passed('Offline STOP survives SIGKILL/reopen and prevents restoring old RUNNING intent')
        if args.separate_execution:
            passed('A durably accepted STOP completes under the original request while its accepting management entry is dead')
            clock = 'CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3))*1000 AS SIGNED)' if args.control_product == 'MYSQL' else 'CAST(EXTRACT(EPOCH FROM clock_timestamp())*1000 AS BIGINT)'
            wait(lambda: sql('store', "SELECT COUNT(*) FROM global_control.rmc_node WHERE cluster_id='global-cdc' AND node_id='node-a' AND lifecycle='RUNNING' AND lease_until>" + clock) == '0', 'accepting entry lease expires')
            compose('up', '-d', 'node-a')
            address('node-a')
            wait(lambda: http('node-a', '/api/session')['nodeRole'] == 'CONTROLLER', 'accepting management entry rejoin')
        action('START')
        wait(rows_match, 'restart incremental catchup')
        passed('Original private journal resumes after owner restart; missed binlog data catches up')
        action('STOP', node='node-b')
        configuration = http('node-a', route + '/configuration')
        assert env['RM_GC_DATA'] not in json.dumps(configuration)
        edited = configuration['config']
        edited['chunkSize'] = 2
        http('node-b', route + '/configuration/validate', {'config': edited, 'expectedRevision': configuration['revision']})
        edit = 'gc_edit_' + uuid.uuid4().hex
        http('node-b', route + '/configuration', {'config': edited, 'expectedRevision': configuration['revision']}, method='PUT', request=edit, expected=202)
        wait(lambda: outcome(edit), 'global configuration edit')
        assert http('node-a', route + '/configuration')['config']['chunkSize'] == 2
        assert http('node-a', route + '/configuration/history') == http('node-b', route + '/configuration/history')
        passed('Masked configuration references, remote preview/edit and shared history across entries')
        history = http('node-a', route + '/configuration/history')
        original = next(version for version in history['versions'] if version['index'] == 1)
        restore_body = {'reference': original['reference'], 'expectedRevision': task()['revision']}
        http('node-b', route + '/configuration/restore/validate', restore_body)
        restore = 'gc_restore_' + uuid.uuid4().hex
        http('node-a', route + '/configuration/restore', restore_body, request=restore, expected=202)
        wait(lambda: outcome(restore), 'cross-entry configuration restore')
        assert http('node-b', route + '/configuration')['config']['chunkSize'] == 1
        passed('Reviewed configuration history restores as a new globally active version')
        action('START')
        # Existing equal rows do not establish that a newly spawned Worker has claimed the journal lease.
        sql('mysql', "INSERT INTO sales.items(id,note,payload) VALUES(12,'after-config-restart',JSON_OBJECT('n',12))")
        wait(rows_match, 'restored configuration catches up')
        sql('postgres', "UPDATE rm_pg_sales.items SET note='reviewed-drift' WHERE id=1")
        sql('mysql-target', "UPDATE rm_my_sales.items SET note='reviewed-drift' WHERE id=1")
        if args.initial_owner_artifact:
            sql('postgres', 'UPDATE rm_pg_sales.duplicates SET value=99 WHERE value=7')
            sql('mysql-target', 'UPDATE rm_my_sales.duplicates SET value=99 WHERE value=7')
        validation, _ = action('VALIDATE', complete=not args.initial_owner_artifact, validation={'targets': ['pg', 'my'], 'gateMillis': 15000, 'sourceMillis': 60000, 'targetMillis': 30000})
        if args.initial_owner_artifact:
            wait(lambda: operation(validation).get('deliveryStatus') == 'UNKNOWN' and operation(validation).get('code') == 'OWNER_RECEIPT_MISMATCH' and operation(validation).get('result', {}).get('code') == 'VALIDATION_DIFFERENCES', 'old owner receipt fault after actual validation')
            original_report = operation(validation)['validation']['reportId']
            compose('stop', 'node-b')
            services['node-b']['volumes'][0] = str(jar) + ':/app/rivomesh.jar:ro'
            composition.write_text(json.dumps({'services': services}))
            compose('up', '-d', 'node-b')
            address('node-b')
            wait(lambda: outcome(validation), 'reconcile old UNKNOWN receipt after owner upgrade')
            assert operation(validation)['validation']['reportId'] == original_report
            passed('Owner upgrade resolves old UNKNOWN from its original normalized receipt without replacing the report')
        wait(lambda: operation(validation).get('validation', {}).get('reportId'), 'visible report identity')
        report_id = operation(validation)['validation']['reportId']
        report_path = route + '/validations/' + report_id
        report = http('node-a', report_path)
        assert report['code'] == 'VALIDATION_DIFFERENCES'
        assert http('node-b', report_path)['results'] == report['results']
        assert task()['desired'] == 'PAUSED'
        scope = [key for key, row in report['results'].items() if row['missing'] + row['extra'] + row['changed'] > 0]
        assert len(scope) == (4 if args.initial_owner_artifact else 2)
        names = {table['id']: table['name'] for table in report['tables']}
        for key in scope:
            row = report['results'][key]
            page = http('node-a', report_path + '/differences', {'target': row['target'], 'tableId': row['tableId'], 'limit': 1})
            assert len(page['items']) == 1
            values = http('node-b', report_path + '/values', {'target': row['target'], 'tableId': row['tableId'], 'identityHash': page['items'][0]['identityHash']})
            assert ('reviewed-drift' if names[row['tableId']] == 'items' else '99') in json.dumps(values)
        passed('Remote validation pauses CDC and exposes shared report identities, differences and authorized row values')
        plan, _ = action('REPAIR_PLAN', repair={'reportId': report_id, 'scope': scope})
        wait(lambda: operation(plan).get('repair', {}).get('planId'), 'visible plan identity')
        plan_id = operation(plan)['repair']['planId']
        plan_path = report_path + '/repairs/' + plan_id
        reviewed = http('node-a', plan_path)
        assert http('node-b', plan_path)['hash'] == reviewed['hash']
        action('REPAIR_EXECUTE', node='node-b', repair={'reportId': report_id, 'planId': plan_id, 'approvedHash': reviewed['hash']})
        wait(rows_match, 'reviewed SQL repair restores exact rows')
        assert task()['desired'] == 'PAUSED'
        passed('Cross-entry reviewed repair approval restores both targets and leaves replication paused')
        if args.initial_owner_artifact:
            passed('Reviewed repairs include both keyless target tables and restore exact multiplicities')
        action('RESUME', errorCount=task()['status']['error_count'])
        sql('mysql', "INSERT INTO sales.items(id,note,payload) VALUES(11,'after-repair',JSON_OBJECT('n',11))")
        wait(rows_match, 'explicit post-repair CDC resume')
        passed('Explicit resume after reviewed repair continues original binlog progress')
        action('STOP')
        assert hashlib.sha256(jar.read_bytes()).hexdigest() == digest
        assert hashlib.sha256(initial_owner.read_bytes()).hexdigest() == initial_digest
        manifest['status'] = 'PASS'
    except Exception as failure:
        manifest['status'] = 'FAIL'
        manifest['failureType'] = type(failure).__name__
        try:
            observed = task()
            diagnostics['task'] = {key: observed.get(key) for key in ('workerState', 'desired', 'problem', 'statusAvailable', 'status', 'operations')}
        except Exception:
            pass
        for node in members:
            logs = compose('logs', '--no-color', '--tail', '60', node, check=False)
            diagnostics[node] = logs.stdout + logs.stderr
        safe = json.dumps(diagnostics, indent=2)
        for key in ('RM_GC_TOKEN', 'RM_GC_CONTROL', 'RM_GC_DATA', 'RM_GC_KEY'):
            safe = safe.replace(env[key], '[REDACTED]')
        (output / 'diagnostics-redacted.json').write_text(safe + '\n')
        # Never archive raw configuration, passwords or unredacted controller logs.
        raise
    finally:
        result = compose('down', '-v', '--remove-orphans', check=False)
        manifest['cleanupExitCode'] = result.returncode
        (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        if result.returncode:
            raise RuntimeError('Own fixture cleanup failed')
print('GLOBAL_CONTROL_CDC_ACCEPTED artifact=' + digest)
