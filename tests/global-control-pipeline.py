#!/usr/bin/env python3
"""Two controller JVMs, real MySQL CDC and PostgreSQL/MySQL targets; isolated fixtures."""
import argparse
import base64
import hashlib
import json
import os
import re
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
parser.add_argument('--source-image', default='mysql:8.4', help='MySQL source fixture image or pinned digest')
parser.add_argument('--source-expected-version', help='Set the source type and optional version check in the fixture task')
parser.add_argument('--separate-execution', action='store_true', help='Two management JVMs plus a separate probe-only execution Agent')
parser.add_argument('--control-product', choices=('POSTGRESQL', 'MYSQL'), default='POSTGRESQL')
parser.add_argument('--runtime-jdk', type=int, choices=(8, 11, 17, 21, 25),
                    help='Run controller, Agent and Worker snapshot/CDC with the specified Java release')
parser.add_argument('--mixed-runtime-upgrade', action='store_true',
                    help='Replace Controller Java 8 with 25 and drained Agent Java 11 with 17 while Controller Java 21 stays online')
parser.add_argument('--runtime-image', help='Locally built nonroot runtime image; separate Agent profile only')
parser.add_argument('--failure-diagnostics', action='store_true', help='Check phase-only cross-entry status and persistent SQL failure after Worker restart')
parser.add_argument('--source-diagnostics', choices=('connection-ddl','decode'),
                    help='Exercise native source connection, DDL or row-image diagnostics across both entries')
parser.add_argument('--task-views', action='store_true', help='Check global task filters and exact per-target backlog during native SQL failure')
parser.add_argument('--managed-binlog-evidence', action='store_true', help='Retain and replay source evidence through both management entries')
parser.add_argument('--verify-binlog-contents', action='store_true', help='Verify the digest of native binlog bytes against the fixture file')
parser.add_argument('--header-boundary', action='store_true', help='Check native same-file GTID boundary after a row decode fault')
parser.add_argument('--worker-diagnostics', action='store_true',
                    help='Check redacted Worker event pages after owner restart in a source fault profile')
parser.add_argument('--snapshot-diagnostics', action='store_true', help='Inject initial DDL permission and snapshot CHECK faults before normal catch-up')
parser.add_argument('--managed-failure-review', action='store_true', help='Protocol5 durable evidence creation and cross-entry authorized raw review')
parser.add_argument('--checkpoint-inspection', action='store_true', help='Check additive read-only native checkpoint result in managed review')
parser.add_argument('--managed-commit', action='store_true', help='Protocol6 manual confirmation of a native commit leading journal ACK')
parser.add_argument('--managed-skip', action='store_true', help='Protocol7 complete-transaction skip and lost source-ACK recovery with durable gaps')
parser.add_argument('--compensation', action='store_true', help='Exercise real CDC before-image compensation across two management entries')
parser.add_argument('--correction', action='store_true', help='Check administrator typed correction and JSON import against a real failed CDC transaction')
parser.add_argument('--recovery-jobs', action='store_true', help='Cancel a blocked recovery and restart the Agent on both sides of the native commit')
parser.add_argument('--test-classes', type=Path, help='Frozen application test classes, required for --managed-commit or --recovery-jobs')
args = parser.parse_args()
if args.task_views and not args.separate_execution:
    parser.error('--task-views requires --separate-execution')
if args.managed_binlog_evidence and not args.verify_binlog_contents:
    parser.error('--managed-binlog-evidence requires --verify-binlog-contents')
if args.verify_binlog_contents and not args.header_boundary:
    parser.error('--verify-binlog-contents requires --header-boundary')
if args.header_boundary and args.source_diagnostics != 'decode':
    parser.error('--header-boundary requires --source-diagnostics decode')
if args.worker_diagnostics and not args.source_diagnostics:
    parser.error('--worker-diagnostics requires --source-diagnostics')
if args.control_product == 'MYSQL' and not args.separate_execution:
    parser.error('MySQL control-store business acceptance requires --separate-execution')
if args.runtime_jdk and (not args.separate_execution or any((args.runtime_image, args.recovery_jobs,
        args.compensation, args.correction, args.failure_diagnostics, args.snapshot_diagnostics,
        args.managed_failure_review, args.managed_commit))):
    parser.error('--runtime-jdk uses the separate Agent and baseline SQL process smoke profile')
if args.mixed_runtime_upgrade and (not args.separate_execution or any((args.runtime_jdk, args.runtime_image,
        args.failure_diagnostics, args.source_diagnostics, args.snapshot_diagnostics, args.recovery_jobs,
        args.compensation, args.correction))):
    parser.error('--mixed-runtime-upgrade uses the separate Agent and baseline SQL process profile')
if args.runtime_image and not args.separate_execution:
    parser.error('--runtime-image requires --separate-execution')
if args.source_diagnostics and (not args.separate_execution or any((args.runtime_jdk, args.runtime_image,
        args.recovery_jobs, args.compensation, args.correction, args.failure_diagnostics,
        args.snapshot_diagnostics, args.managed_failure_review, args.managed_commit))):
    parser.error('--source-diagnostics uses the separate Agent and baseline native source profile')
if args.failure_diagnostics and not args.separate_execution:
    parser.error('--failure-diagnostics requires --separate-execution')
if args.snapshot_diagnostics and not args.separate_execution:
    parser.error('--snapshot-diagnostics requires --separate-execution')
if args.managed_failure_review and not (args.separate_execution and args.failure_diagnostics):
    parser.error('--managed-failure-review requires --separate-execution --failure-diagnostics')
if args.checkpoint_inspection and not args.managed_failure_review:
    parser.error('--checkpoint-inspection requires --managed-failure-review')
if args.managed_commit and not (args.checkpoint_inspection and args.test_classes and args.test_classes.resolve().is_dir()):
    parser.error('--managed-commit requires --checkpoint-inspection and --test-classes')
if args.managed_skip and not args.managed_commit:
    parser.error('--managed-skip requires --managed-commit and its original evidence/fixture dependencies')
if args.recovery_jobs and not (args.separate_execution and (args.compensation or args.correction)
                               and not any((args.runtime_image, args.failure_diagnostics, args.snapshot_diagnostics,
                                            args.managed_failure_review, args.managed_commit))
                               and args.test_classes and args.test_classes.resolve().is_dir()):
    parser.error('--recovery-jobs requires --separate-execution, --compensation or --correction, and --test-classes')
jar = args.artifact.resolve()
digest = hashlib.sha256(jar.read_bytes()).hexdigest()
output = args.output.resolve()
output.relative_to(ROOT)
output.mkdir(parents=True, exist_ok=True)
project = 'rivomesh-global-cdc-' + uuid.uuid4().hex[:10]
env = dict(os.environ, RM_GC_TOKEN=secrets.token_urlsafe(32), RM_GC_CONTROL=secrets.token_urlsafe(24),
           RM_GC_DATA=secrets.token_urlsafe(24), RM_GC_KEY=base64.b64encode(secrets.token_bytes(32)).decode(), RM_GC_STORE_URL='jdbc:postgresql://postgres:5432/sync_test')
if (args.compensation or args.correction) and not args.separate_execution:
    parser.error('--compensation and --correction require --separate-execution')
if args.managed_failure_review or args.compensation or args.correction or args.worker_diagnostics or args.managed_binlog_evidence:
    env.update(RM_GC_OPERATOR=secrets.token_urlsafe(32), RM_GC_VIEWER=secrets.token_urlsafe(32))
owner_node = 'node-worker' if args.separate_execution else 'node-b'
members = ('node-a', 'node-b', 'node-worker') if args.separate_execution else ('node-a', 'node-b')
runtime_majors = dict.fromkeys(members, args.runtime_jdk or 8)
if args.mixed_runtime_upgrade:
    runtime_majors.update({'node-a': 8, 'node-b': 21, owner_node: 11})
if args.separate_execution:
    env['RM_GC_STORE_URL'] = 'jdbc:mysql://store:3306/rivomesh_control?allowPublicKeyRetrieval=true&sslMode=DISABLED' if args.control_product == 'MYSQL' else 'jdbc:postgresql://store:5432/rivomesh_control'
checks = []
diagnostics = {}
manifest = {'artifactSha256': digest, 'status': 'RUNNING', 'checks': checks,
            'harnessSourceSha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'executionProfile': 'SEPARATE_AGENT' if args.separate_execution else 'COMBINED',
            'controlProduct': args.control_product,
            'failureDiagnostics': args.failure_diagnostics,
            'snapshotDiagnostics': args.snapshot_diagnostics,
            'managedFailureReview': args.managed_failure_review,
            'checkpointInspection': args.checkpoint_inspection,
            'managedCommit': args.managed_commit,
            'managedSkip': args.managed_skip,
            'compensation': args.compensation,
            'correction': args.correction,
            'recoveryJobs': args.recovery_jobs,
            'runtimeJdk': args.runtime_jdk,
            'mixedRuntimeUpgrade': args.mixed_runtime_upgrade,
            'workerDiagnostics': args.worker_diagnostics,
            'headerBoundary': args.header_boundary,
            'verifyBinlogContents': args.verify_binlog_contents,
            'managedBinlogEvidence': args.managed_binlog_evidence,
            'taskViews': args.task_views,
            'sourceDiagnostics': args.source_diagnostics,
            'sourceImage': args.source_image,
            'notCovered': 'Automatic owner relocation, live Kubernetes/CSI, source promotion and full connector matrix'}
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
        'mysql': {'image': args.source_image, 'environment': {'MYSQL_ROOT_PASSWORD': '${RM_GC_DATA}', 'MYSQL_ROOT_HOST': '%'},
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
        if args.managed_binlog_evidence:
            config['cluster'].update(heartbeatMillis=1000,leaseMillis=15000)
        if args.managed_failure_review or args.compensation or args.correction:
            config['cluster']['recoveryReview'] = True
        if args.managed_failure_review or args.compensation or args.correction or args.worker_diagnostics or args.managed_binlog_evidence:
            config['users'] = [{'id': 'review-operator', 'role': 'OPERATOR', 'token': '${RM_GC_OPERATOR}'}, {'id': 'review-viewer', 'role': 'VIEWER', 'token': '${RM_GC_VIEWER}'}]
        if args.managed_commit or args.compensation or args.correction:
            config['cluster']['recoveryCommit'] = True
        if args.managed_skip or args.compensation or args.correction:
            config['cluster']['recoverySkip'] = True
        if args.separate_execution:
            config['cluster'].update({'separateExecution': True, 'role': 'AGENT' if node == owner_node else 'CONTROLLER', 'url': '${RM_GC_STORE_URL}', 'driver': 'com.mysql.cj.jdbc.Driver' if args.control_product == 'MYSQL' else 'org.postgresql.Driver', 'username': 'root' if args.control_product == 'MYSQL' else 'postgres'})
        if args.runtime_image:
            config.update({'tasksDirectory': '/app/data/tasks', 'dataDirectory': '/app/data/controller'})
        (home / (node + '.json')).write_text(json.dumps(config))
        services[node] = {'image': f'eclipse-temurin:{runtime_majors[node]}-jre', 'entrypoint': ['java', '-Xmx512m', '-jar', '/app/rivomesh.jar', 'agent' if args.separate_execution and node == owner_node else 'serve', '/fixture/' + node + '.json'],
                          'environment': {'RM_GC_TOKEN': '${RM_GC_TOKEN}', 'RM_GC_CONTROL': '${RM_GC_CONTROL}', 'RM_GC_KEY': '${RM_GC_KEY}', 'RM_GC_STORE_URL': '${RM_GC_STORE_URL}'},
                          'volumes': [str(jar) + ':/app/rivomesh.jar:ro', str(home) + ':/fixture'],
                          'ports': ['127.0.0.1::8080'], 'stop_grace_period': '30s'}
        if args.managed_failure_review or args.compensation or args.correction or args.worker_diagnostics or args.managed_binlog_evidence:
            services[node]['environment'].update(RM_GC_OPERATOR='${RM_GC_OPERATOR}', RM_GC_VIEWER='${RM_GC_VIEWER}')
        if args.runtime_image:
            volumes[node + '-data'] = {}
            services[node]['image'] = args.runtime_image
            services[node]['volumes'] = [str(jar) + ':/app/rivomesh.jar:ro', str(home) + ':/fixture:ro', node + '-data:/app/data']
    if args.managed_commit or args.recovery_jobs:
        classes=args.test_classes.resolve();classes.relative_to(ROOT)
        services[owner_node]['volumes'].append(str(classes)+':/fixture-tests:ro')
        test_hash=hashlib.sha256()
        for entry in sorted(classes.rglob('*')):
            if entry.is_file():test_hash.update(str(entry.relative_to(classes)).encode()+b'\0'+entry.read_bytes())
        manifest['testClassesSha256']=test_hash.hexdigest()
    if args.recovery_jobs:
        services[owner_node]['entrypoint'] = [
            'java', '-Xmx512m', '-cp', '/app/rivomesh.jar:/fixture-tests',
            'io.github.rivomesh.platform.RecoveryAgentFixture',
            '/fixture/' + owner_node + '.json', '/fixture/kill-after-recovery-commit']
    composition = home / 'compose.json'
    composition.write_text(json.dumps({'services': services, 'volumes': volumes}))

    def compose(*arguments, check=True):
        return subprocess.run(['docker', 'compose', '-p', project, '-f', str(composition), *arguments], env=env,
                              cwd=ROOT, check=check, capture_output=True, text=True, timeout=180)

    def sql(service, statement):
        if service == 'postgres' or service == 'store' and args.control_product == 'POSTGRESQL':
            result = compose('exec', '-T', service, 'psql', '-h', '127.0.0.1', '-X', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'sync_test' if service == 'postgres' else 'rivomesh_control', '-A', '-t', '-F', '|', '-c', statement)
        else:
            result = compose('exec', '-T', '-e', 'MYSQL_PWD=' + env['RM_GC_CONTROL' if service == 'store' else 'RM_GC_DATA'], service, 'mysql', '-h127.0.0.1', '-uroot', '-N', '-B', '-e', statement)
        return result.stdout.strip().replace('\t', '|')

    bases = {}

    def address(node):
        port = compose('port', node, '8080').stdout.strip().rsplit(':', 1)[1]
        bases[node] = 'http://127.0.0.1:' + port

    def worker_pids():
        container = compose('ps', '-q', owner_node).stdout.strip()
        lines = subprocess.check_output(['docker', 'top', container, '-eo', 'pid,args'], text=True).splitlines()
        return {line.split()[0] for line in lines if 'io.github.rivomesh.Main run ' in line}

    def observe_jvms():
        java = []
        for node in members:
            binary = compose('exec', '-T', node, 'readlink', '/proc/1/exe').stdout.strip()
            java.append((node, 'AGENT' if node == owner_node else 'CONTROLLER', binary))
        worker_binary = compose('exec', '-T', owner_node, 'sh', '-ec',
            r'''for file in /proc/[0-9]*/cmdline; do
                if tr '\000' '\n' < "$file" 2>/dev/null | grep -qx io.github.rivomesh.Main; then
                    readlink "${file%/cmdline}/exe"
                fi
            done''').stdout.strip()
        assert len(worker_pids()) == 1 and worker_binary, 'one real CDC Worker process is required'
        java.append((owner_node, 'WORKER', worker_binary))
        processes = []
        for node, role, binary in java:
            runtime_id = compose('images', '-q', node).stdout.strip()
            image = json.loads(subprocess.check_output(['docker', 'image', 'inspect', runtime_id], text=True))[0]
            assert binary.endswith('/bin/java') and '\n' not in binary, 'expected a single JVM executable'
            settings = compose('exec', '-T', node, binary, '-XshowSettings:properties', '-version').stderr
            version = re.search(r'(?m)^\s*java\.version = (\S+)$', settings).group(1)
            architecture = re.search(r'(?m)^\s*os\.arch = (\S+)$', settings).group(1)
            major = int(version.split('.')[1] if version.startswith('1.') else re.match(r'\d+', version).group())
            assert major == runtime_majors[node], 'unexpected active JVM release'
            assert architecture in ({'aarch64','arm64'} if image['Architecture']=='arm64' else {'amd64','x86_64'}), architecture
            processes.append(dict(role=role, node=node, version=version, architecture=architecture, imageId=image['Id']))
        if args.runtime_jdk:
            manifest['runtimeImageId'] = processes[0]['imageId']
            manifest['runtimeArchitecture'] = image['Architecture']
            manifest['runtimeProcesses'] = processes
            passed('Observed the real Controller, Agent and Worker JVM executables on Java ' + str(args.runtime_jdk))
        return processes

    def http(node, path, body=None, method=None, request=None, expected=200, token=None):
        headers = {'Authorization': 'Bearer ' + (token or env['RM_GC_TOKEN'])}
        if body is not None:
            headers.update({'Content-Type': 'application/json', 'X-Request-ID': request or 'global_cdc_' + uuid.uuid4().hex})
        call = urllib.request.Request(bases[node] + path, data=json.dumps(body).encode() if body is not None else None, headers=headers, method=method)
        allowed = expected if isinstance(expected, tuple) else (expected,)
        attempts = 5 if args.managed_binlog_evidence and 503 not in allowed else 1
        for attempt in range(attempts):
            try:
                response = urllib.request.urlopen(call, timeout=20)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                value = json.loads(response.read())
                if response.status==503 and value.get('code')=='AUDIT_UNAVAILABLE' and attempt+1<attempts:
                    time.sleep(1)
                    continue
                assert response.status in allowed, 'HTTP ' + str(response.status) + ': ' + str(value.get('code', 'unexpected response'))
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

    def terminate_owner():
        compose('kill', '-s', 'SIGKILL', owner_node)
        wait(lambda: task().get('ownerState') != 'HEALTHY', 'owner offline before replay')

    def restart_owner():
        compose('up', '-d', owner_node)
        address(owner_node)
        wait(lambda: http(owner_node, '/readyz')['status'] == 'UP', 'original Agent ready after recovery fault')

    def recovery_write(name, original_ack, unchanged, **options):
        if not args.recovery_jobs:
            return action(name, node='node-a', **options)

        command = ['docker', 'compose', '-p', project, '-f', str(composition), 'exec', '-T', 'postgres',
                   'psql', '-h', '127.0.0.1', '-X', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'sync_test',
                   '-c', "SET application_name='rm_fixture_recovery_lock'; BEGIN; "
                         "LOCK TABLE rm_pg_sales.items IN SHARE MODE; SELECT pg_sleep(120); COMMIT"]
        blocked = " FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'LOCK TABLE %SHARE ROW EXCLUSIVE MODE%' "
        marker = home / 'kill-after-recovery-commit'
        metadata = 'rm_pg_meta.rm3_sink_compensation'

        def receipt(request):
            return sql('postgres', f"SELECT payload_hash FROM {metadata} WHERE operation_id='{request}'")

        def block_target():
            process = subprocess.Popen(command, env=env, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            wait(lambda: sql('postgres', "SELECT COUNT(*) FROM pg_stat_activity WHERE "
                       "application_name='rm_fixture_recovery_lock' AND wait_event='PgSleep'") == '1',
                 'fixture holds target table lock')
            return process

        def release_target(process):
            try:
                sql('postgres', "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='rm_fixture_recovery_lock'")
            finally:
                process.wait(timeout=10)

        def check_paused():
            def paused():
                observed = task()
                sub = next((item for item in observed.get('status',{}).get('subscriptions',[]) if item['id'] == 'pg'), None)
                return observed['desired'] == 'PAUSED' and sub and sub['acknowledged'] == original_ack
            wait(paused, 'Agent publishes the original pause and ACK after recovery')

        # While the Agent is offline the command store can cancel a queued request without touching business rows.
        terminate_owner()
        queued, _ = action(name, node='node-a', complete=False, **options)
        cancel_queued, _ = action('RECOVERY_CANCEL', node='node-b', complete=False, parentRequestId=queued)
        restart_owner()
        wait(lambda: outcome(cancel_queued) and operation(queued)['status'] == 'CANCELLED', 'queued cancellation after restart')
        assert operation(queued)['code'] == 'CANCELLED_BEFORE_DISPATCH' and unchanged()
        check_paused()
        passed(name + ': offline request cancelled by either controller before dispatch')

        # Prove that a running write is interrupted and resolved read-only with neither a receipt nor a row mutation.
        runner = block_target()
        try:
            stopped, _ = action(name, node='node-a', complete=False, **options)
            wait(lambda: sql('postgres', 'SELECT COUNT(*)' + blocked) == '1', 'running recovery waits on real business lock')
            invalid = dict(action='RECOVERY_CANCEL', expectedRevision=task()['revision'], parentRequestId=queued)
            assert http('node-b', route+'/actions', invalid, expected=409)['code'] == 'CANCEL_SCOPE_CHANGED'
            invalid['parentRequestId'] = stopped
            assert http('node-b', route+'/actions', invalid, expected=403, token=env['RM_GC_OPERATOR'])['code'] == 'FORBIDDEN'
            cancel_running, _ = action('RECOVERY_CANCEL', node='node-b', parentRequestId=stopped)
            wait(lambda: operation(stopped)['status'] == 'CANCELLED', 'running recovery cancellation resolved', seconds=60)
            assert runner.poll() is None, 'native lock wait expired before the held business lock was released'
            assert operation(cancel_running)['parentRequestId'] == stopped
            assert operation(stopped)['code'] == 'RECOVERY_CANCELLED' and not receipt(stopped) and unchanged()
            check_paused()
            passed(name + ': another controller cancels blocked SQL under the exact original request; target and ACK unchanged')
        finally:
            release_target(runner)

        # Leave the same request pending after a pre-commit SIGKILL, then crash the retry just after its native commit.
        runner = block_target()
        original = 'gc_recover_' + uuid.uuid4().hex
        body = dict(action=name, expectedRevision=task()['revision'], **options)
        try:
            accepted = http('node-a', route+'/actions', body, request=original, expected=202)
            wait(lambda: sql('postgres', 'SELECT COUNT(*)' + blocked) == '1', 'original write reaches pre-commit lock')
            terminate_owner()
            assert not receipt(original) and unchanged()
            wait(lambda: operation(original)['status'] == 'PENDING', 'original request retained after pre-commit kill')
            passed(name + ': pre-commit SIGKILL keeps exactly the original request pending without a partial commit')
            marker.write_text(original+'\n')
        finally:
            release_target(runner)

        compose('up', '-d', owner_node)
        wait(lambda: not marker.exists() and receipt(original), 'original write commits and the fixture halts the Agent')
        wait(lambda: 'Exited (137)' in compose('ps', '-a', owner_node).stdout, 'Agent exits before local owner receipt')
        native = receipt(original)
        controller = json.loads((home / owner_node / 'controller' / 'controller.json').read_text())
        owner_operation = next(op for op in controller['tasks']['global_cdc']['operations'] if op['id'] == original)
        assert owner_operation['status'] == 'PENDING'
        assert not unchanged()
        compose('kill', '-s', 'SIGKILL', 'node-a')
        restart_owner()
        wait(lambda: outcome(original, 'node-b'), 'surviving controller observes original replay receipt')
        def visible_receipt():
            settled = next(op for op in task('node-b')['operations'] if op['id'] == original)
            return settled.get('result') if settled.get('result', {}).get('operation') == original else None
        settled = wait(visible_receipt, 'original native result published through the surviving controller')
        assert receipt(original) == native
        assert sql('postgres', f"SELECT COUNT(*) FROM {metadata} WHERE operation_id='{original}'") == '1'
        http('node-b', route+'/actions', body, request=original, expected=202)
        assert visible_receipt() == settled
        compose('up', '-d', 'node-a')
        address('node-a')
        wait(lambda: outcome(original), 'original management entry sees the same result after restart')
        check_paused()
        passed(name + ': post-commit Agent loss and controller loss preserve one native receipt and the same cross-entry result')
        return original, accepted

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

    def run_task_views():
        for node in ('node-a','node-b'):
            exact = http(node, '/api/tasks?limit=1&ownerNodeId='+owner_node+'&taskId=global_cdc')
            assert exact['total']==1 and [row['id'] for row in exact['tasks']]==['global_cdc'] and not exact['next']
            assert http(node, '/api/tasks?ownerNodeId=node-a&taskId=global_cdc')['total']==0
            assert http(node, '/api/tasks?ownerNodeId='+owner_node+'&taskId=unavailable')['total']==0
        passed('Both management entries filter exact task and owner inside the shared pagination query')

        sql('postgres', 'ALTER TABLE rm_pg_sales.items ADD CONSTRAINT view_backlog CHECK(id<>99)')
        sql('mysql', "INSERT INTO sales.items(id,note,payload) VALUES(99,'backlog',JSON_OBJECT('n',99))")

        def backlog(node):
            observed = task(node)
            state = observed['status']
            if not observed.get('statusAvailable') or not state.get('subscriptions'):return None
            targets = {value['id']:value for value in state['subscriptions']}
            pg, my = targets['pg'], targets['my']
            if pg['state']!='PAUSED' or my['pendingTransactions']!='0':return None
            remaining = int(state['captured_sequence'])-int(pg['safeCoveredSequence'])
            if remaining<=0 or pg['pendingTransactions']!=str(remaining):return None
            return pg

        pg = wait(lambda:backlog('node-a') and backlog('node-b'), 'exact per-target backlog through both entries')
        assert pg['errorCode']=='SQL_23514'
        assert sql('postgres','SELECT COUNT(*) FROM rm_pg_sales.items WHERE id=99')=='0'
        assert sql('mysql-target','SELECT COUNT(*) FROM rm_my_sales.items WHERE id=99')=='1'
        passed('Only the SQL-blocked destination has a pending transaction and both management entries agree')
        sql('postgres', 'ALTER TABLE rm_pg_sales.items DROP CONSTRAINT view_backlog')
        action('RESUME_TARGET', node='node-b', subscriptionId='pg', errorCount=pg['errorGeneration'])
        wait(rows_match, 'all actual SQL rows match after repair')
        wait(lambda: all(all(sub.get('pendingTransactions')=='0' for sub in task(node)['status']['subscriptions'])
                         for node in ('node-a','node-b')), 'both destinations caught up')
        passed('Generation-bound resume clears only the repaired destination backlog and all native rows agree')

    def run_mixed_upgrade():
        rolls = manifest['runtimeRollout'] = {}
        rolls['initial'] = observe_jvms()
        passed('Actual Controllers Java 8 and 21 and Agent/Worker Java 11 share the task, commands and initial CDC')

        def replace_runtime(node, major):
            runtime_majors[node] = major
            services[node]['image'] = f'eclipse-temurin:{major}-jre'
            composition.write_text(json.dumps({'services': services, 'volumes': volumes}))
            compose('up', '-d', '--no-deps', '--force-recreate', node)
            address(node)

        def snapshot_set():
            return tuple(sql(product, f'SELECT table_id,snapshot_id FROM rm_{suffix}_meta.rm3_sink_table ORDER BY table_id')
                         for product, suffix in (('postgres', 'pg'), ('mysql-target', 'my')))

        before = snapshot_set()
        assert all(len(snapshots.splitlines()) == 3 for snapshots in before)
        original_workers = worker_pids()
        assert len(original_workers) == 1
        compose('stop', '--timeout', '30', 'node-a')
        sql('mysql', "INSERT INTO billing.ledger VALUES(93,31.4159)")
        wait(rows_match, 'native CDC during Java Controller replacement')
        check, _ = action('CHECK', node='node-b')
        replace_runtime('node-a', 25)
        wait(lambda: outcome(check, 'node-a'), 'replaced Controller reads original durable command')
        assert before == snapshot_set() and worker_pids() == original_workers
        rolls['controllerReplaced'] = observe_jvms()
        passed('Replacing Controller Java 8 with 25 leaves Java 21 management and the original Worker available')

        version = task('node-b')['configurationVersion']
        stop, _ = action('STOP', node='node-b')
        wait(lambda: not worker_pids() and task('node-b').get('workerState') == 'STOPPED', 'drained Java 11 Worker exits')
        wait(lambda: task('node-b').get('captureLeaseState') in ('UNCLAIMED', 'EXPIRED'), 'old capture lease has stopped')
        stopped_state = task('node-b')
        stopped = stopped_state['status']['capture_gtid']
        compose('stop', '--timeout', '30', owner_node)
        sql('mysql', "UPDATE sales.items SET note='agent staged',payload=JSON_OBJECT('n',17) WHERE id=3")
        assert sql('postgres', 'SELECT note FROM rm_pg_sales.items WHERE id=3') == 'third'
        replace_runtime(owner_node, 17)
        wait(lambda: http(owner_node, '/readyz')['status'] == 'UP' and task('node-b').get('ownerState') == 'HEALTHY',
             'replacement Java 17 Agent returns from the same directory')
        assert not worker_pids(), 'STOPPED intent must survive Agent replacement'
        wait(lambda: outcome(stop, 'node-a') and task('node-a')['revision'] == stopped_state['revision'],
             'replacement preserves the original STOP receipt and its task revision')
        assert task('node-b')['status']['capture_gtid'] == stopped
        passed('The drained Agent opens the same task directory on Java 17 with the prior GTID and STOP receipt')
        action('START', node='node-a')
        wait(rows_match, 'Java 17 Worker resumes both targets without resnapshot')
        wait(lambda: task('node-b')['status']['capture_gtid'] != stopped, 'original GTID continues after restart')
        assert snapshot_set() == before
        left, right = task('node-a'), task('node-b')
        assert left['configurationVersion'] == right['configurationVersion'] == version
        assert left['revision'] == right['revision']
        rolls['agentReplaced'] = observe_jvms()
        passed('Java 17 Worker replays native CDC to both SQL targets from original snapshots and checkpoint')

    def run_source_diagnostics():
        def phase(expected):
            result = None
            for node in ('node-a','node-b'):
                record = task(node)
                status = record.get('status',{})
                stage = status.get('error_stage') or status.get('worker_error_stage') or record.get('workerErrorStage')
                if stage != expected:
                    return None
                result = status
            return result

        if args.source_diagnostics == 'connection-ddl':
            checkpoint = task()['status']['capture_gtid']
            compose('stop','--timeout','5','mysql')
            unavailable = wait(lambda: phase('SOURCE_CONNECT'), 'both entries classify unavailable source connection')
            assert unavailable['capture_gtid'] == checkpoint
            compose('start','mysql')
            wait(lambda: sql('mysql','SELECT 1')=='1', 'source reconnects')
            sql('mysql', "INSERT INTO sales.items(id,note,payload) VALUES(101,'source connected',JSON_OBJECT('n',101))")
            wait(rows_match, 'recovered source delivers subsequent transactions')
            wait(lambda: not task()['status'].get('worker_error_stage'), 'healthy capture clears transient diagnostic')
            passed('Native source loss reports a connection phase in both entries and resumes without changing checkpoints while unavailable')
            checkpoint = task()['status']['capture_gtid']
            sql('mysql','ALTER TABLE sales.items ADD COLUMN planned_field INT')
            expected, code = 'DDL','DDL_PLAN_REQUIRED'
        else:
            checkpoint = task()['status']['capture_gtid']
            sql('mysql',"SET SESSION binlog_row_image='MINIMAL'; UPDATE sales.items SET note='incomplete image' WHERE id=1")
            expected, code = 'SOURCE_DECODE','BINLOG_SCHEMA_WIDTH_CHANGED'

        observed = wait(lambda: phase(expected), 'both entries see original pipeline failure stage')
        assert observed['error_code'] == code and observed['run_state'] == 'ERROR'
        assert observed['capture_gtid'] == checkpoint
        assert all(sub['state'] == 'RUNNING' for sub in observed['subscriptions'])
        endpoint = route+'/diagnostics'
        if args.worker_diagnostics:
            event = wait(lambda: next((event for event in http('node-a',endpoint+'?limit=25')['events']
                                       if event.get('event')=='CAPTURE_FAILURE' and event.get('phase')==expected),None),
                         'worker captured the native failure event')
        terminate_owner()
        restart_owner()
        restored = wait(lambda: phase(expected), 'persisted source fault survives Agent restart')
        assert restored['capture_gtid'] == checkpoint and restored['error_code'] == code
        assert restored['error_count'] == observed['error_count']
        passed(expected + ': native source failure preserves GTID and error generation across Agent restart and both management entries')
        reviews=[http(node,route+'/capture-recovery') for node in ('node-a','node-b')]
        assert reviews[0]==reviews[1]
        assert reviews[0]['scope']=='UNPROVEN_SOURCE_RANGE' and reviews[0]['failureCode']==code
        assert reviews[0]['sourceAdmission']=='SOURCE_CHECKPOINT_AVAILABLE'
        assert set(reviews[0]['affectedSubscriptions'])=={sub['id'] for sub in observed['subscriptions']}
        assert 'REPAIR_CAPTURE_CAUSE' in reviews[0]['actions']
        if args.header_boundary:
            boundary = reviews[0]['nextTransactionHeaders']
            assert boundary['scope']=='BINLOG_HEADERS_ONLY' and boundary['tables']==['sales.items']
            assert len(boundary['boundaryHash'])==64 and int(boundary['endOffset'])>int(boundary['startOffset'])
            assert sql('mysql', "SELECT GTID_SUBSET('"+boundary['transactionId']+"',@@GLOBAL.gtid_executed)")=='1'
            assert sql('mysql', "SELECT GTID_SUBSET('"+boundary['transactionId']+"','"+checkpoint+"')")=='0'
            assert 'incomplete image' not in json.dumps(boundary)
            passed('Both entries show the same native GTID and complete header boundary while preserving the original checkpoint')
        verified=task()['status']
        assert verified['capture_gtid']==checkpoint and verified['error_count']==observed['error_count']
        assert verified['run_state']=='ERROR'
        passed(expected + ': both entries route a read-only recovery review to the owner without changing the checkpoint or pause')
        if args.worker_diagnostics:
            for node in ('node-a','node-b'):
                page = http(node,endpoint+'?limit=25')
                assert page['ownerNodeId']==owner_node and page['observedAt'] and event in page['events']
                assert 'incomplete image' not in json.dumps(page) and env['RM_GC_DATA'] not in json.dumps(page)
            assert set(event)=={'at','id','event','phase'}
            query = endpoint+'?limit=1&after='+event['id']
            older = [http(node,query) for node in ('node-a','node-b')]
            assert len(older[0]['events'])==1 and older[0]['events']==older[1]['events']
            assert older[0]['events'][0]['id']!=event['id']
            http('node-b',endpoint,token=env['RM_GC_VIEWER'],expected=403)
            http('node-b',endpoint+'?limit=26',expected=400)
            passed(expected + ': bounded and redacted diagnostic pages agree across management entries after Agent restart')

        if args.verify_binlog_contents:
            wait(lambda: task().get('captureLeaseState') in ('EXPIRED','UNCLAIMED'), 'capture lease released after decode fault')
            worker_config='/fixture/'+owner_node+'/controller/worker-configs/global_cdc.yml'
            command=['exec','-T',owner_node,'java','-Xmx128m','-jar','/app/rivomesh.jar','review-source',worker_config,'--verify-contents']
            inspection=json.loads(compose(*command).stdout)
            proof=inspection['nextTransactionContents']
            assert proof['scope']=='BINLOG_WIRE_SHA256' and proof['boundaryHash']==boundary['boundaryHash']
            start,end=int(boundary['startOffset']),int(boundary['endOffset'])
            assert re.fullmatch(r'mysql-bin\.\d+', boundary['logFile']) and int(proof['bytes'])==end-start
            raw=subprocess.run(['docker','compose','-p',project,'-f',str(composition),'exec','-T','mysql','dd',
                                'if=/var/lib/mysql/'+boundary['logFile'],'bs=1','skip='+str(start),'count='+str(end-start)],
                               env=env,cwd=ROOT,capture_output=True,check=True,timeout=30).stdout
            assert len(raw)==end-start and hashlib.sha256(raw).hexdigest()==proof['contentSha256']
            assert task()['status']['capture_gtid']==checkpoint and 'incomplete image' not in json.dumps(inspection)
            passed('CRC32-checked wire proof hashes the original file bytes through the commit without decoding rows')
            evidence=home/'source-evidence'
            evidence.mkdir(mode=0o700)
            archive_command=command[:-1]+['--retain-contents','/fixture/source-evidence']
            retained=json.loads(compose(*archive_command).stdout)
            assert retained['nextTransactionContents']==proof
            filename=retained['retainedEvidence']['file']
            assert re.fullmatch(r'[a-f0-9]{64}\.json',filename)
            saved=evidence/filename
            content=saved.read_bytes()
            assert hashlib.sha256(content).hexdigest()+'.json'==filename
            assert saved.stat().st_mode&0o777==0o600
            archive=json.loads(content)
            assert base64.b64decode(archive['base64Frames'])==raw
            assert archive['review']['checkpointHash']==reviews[0]['checkpointHash']
            assert json.loads(compose(*archive_command).stdout)['retainedEvidence']==retained['retainedEvidence']
            assert list(evidence.glob('*.json'))==[saved]
            passed('Retaining twice atomically reuses one private checkpoint-bound archive with exact original bytes')
        if args.managed_binlog_evidence:
            command_path=route+'/actions'
            retain_body={'action':'CAPTURE_RETAIN','expectedRevision':task()['revision'],
                         'errorCount':reviews[0]['errorGeneration'],'boundaryHash':boundary['boundaryHash']}
            for credential in ('RM_GC_OPERATOR','RM_GC_VIEWER'):
                assert http('node-b',command_path,retain_body,request='gc_capture_denied_'+uuid.uuid4().hex,
                            token=env[credential],expected=403)['code']=='FORBIDDEN'
            bad = 'gc_capture_bad_'+uuid.uuid4().hex
            http('node-a',command_path,dict(retain_body,boundaryHash='a'*64),request=bad,expected=202)
            wait(lambda:operation(bad)['status']=='FAILED','stale capture boundary is refused')
            assert operation(bad)['code']=='SOURCE_REVIEW_STALE'
            archive_dir=home/owner_node/'controller'/'capture-evidence'/'global_cdc'
            abandoned=archive_dir/'.capture-31337.tmp'
            abandoned.write_bytes(b'synthetic interrupted archive')
            abandoned.chmod(0o600)
            managed_id='gc_retain_'+uuid.uuid4().hex
            retain_body['expectedRevision']=task()['revision']
            response=http('node-b',command_path,retain_body,request=managed_id,expected=202)
            assert 'base64Frames' not in json.dumps(response)
            wait(lambda:outcome(managed_id,'node-a'),'durable capture evidence result')
            receipt=wait(lambda:operation(managed_id).get('result',{}).get('retainedEvidence'),'capture evidence receipt')
            assert re.fullmatch(r'[a-f0-9]{64}\.json',receipt['file'])
            assert not abandoned.exists(), 'abandoned private staging file survived publication'
            managed=archive_dir/receipt['file']
            managed_bytes=managed.read_bytes()
            assert hashlib.sha256(managed_bytes).hexdigest()==receipt['archiveSha256']
            assert base64.b64decode(json.loads(managed_bytes)['base64Frames'])==raw
            assert task()['status']['capture_gtid']==checkpoint and 'base64Frames' not in json.dumps(operation(managed_id))
            passed('Only an administrator can retain the reviewed generation and exact original binlog bytes through another management entry')
        if args.header_boundary:
            sql('mysql', 'FLUSH BINARY LOGS')
            status_command='SHOW BINARY LOG STATUS' if manifest['sourceVersion'].startswith('8.4.') else 'SHOW MASTER STATUS'
            log = sql('mysql', status_command).split('|')[0]
            assert re.fullmatch(r'mysql-bin\.\d+', log) and log != boundary['logFile']
            sql('mysql', "PURGE BINARY LOGS TO '" + log + "'")
            gone = [http(node, route + '/capture-recovery') for node in ('node-a','node-b')]
            assert gone[0] == gone[1] and 'nextTransactionHeaders' not in gone[0]
            assert gone[0]['sourceAdmission'] == 'SOURCE_LOG_GAP' and gone[0]['checkpointHash'] == reviews[0]['checkpointHash']
            assert task()['status']['capture_gtid'] == checkpoint and task()['status']['run_state'] == 'ERROR'
            passed('Purging the fixture log removes header evidence, reports the gap and retains the exact checkpoint')
            if args.verify_binlog_contents:
                refused=compose(*command,check=False)
                assert refused.returncode and 'MYSQL_BINLOG_CONTENT_REVIEW_UNAVAILABLE' in refused.stderr
                assert 'contentSha256' not in refused.stdout
                assert saved.read_bytes()==content
                passed('Content proof refuses the purged transaction and retained evidence remains byte-identical')
            if args.managed_binlog_evidence:
                terminate_owner()
                restart_owner()
                http('node-a',command_path,retain_body,request=managed_id,expected=202)
                wait(lambda:outcome(managed_id,'node-b'),'original retained receipt after Agent restart')
                assert operation(managed_id)['result']['retainedEvidence']==receipt
                repeated='gc_retain_existing_'+uuid.uuid4().hex
                http('node-b',command_path,dict(retain_body,expectedRevision=task('node-b')['revision']),request=repeated,expected=202)
                wait(lambda:outcome(repeated,'node-a'),'retention reuses the native archive after source purge')
                assert wait(lambda:operation(repeated).get('result',{}).get('retainedEvidence'),'persisted receipt replay')==receipt
                assert managed.read_bytes()==managed_bytes and task()['status']['capture_gtid']==checkpoint
                passed('After Agent restart and log purge both a durable request replay and a fresh retention request reuse the original archive')



    def run_recovery_cases():
        if args.compensation:
            baseline_note = sql('mysql', 'SELECT note FROM sales.items WHERE id=1')
            baseline_amount = sql('mysql', 'SELECT amount FROM billing.ledger WHERE id=1')
            sql('postgres', """INSERT INTO rm_pg_sales.items(id,note,payload,derived) VALUES(97,'conflicting key','{"n":97}',194);
                    UPDATE rm_pg_sales.items SET note='destination drift' WHERE id=1;
                    UPDATE rm_pg_billing.ledger SET amount=999 WHERE id=1;
                    UPDATE rm_pg_sales.duplicates SET value=90 WHERE value=7""")
            sql('mysql', """START TRANSACTION;
                UPDATE sales.items SET note='before primary-key move',payload=JSON_OBJECT('n',12) WHERE id=1;
                UPDATE sales.items SET id=97,note='after primary-key move' WHERE id=1;
                UPDATE sales.duplicates SET value=98 WHERE value=7 LIMIT 1;
                UPDATE billing.ledger SET amount=987.65 WHERE id=1;
                COMMIT""")

            def compensation_failure():
                sub = next(s for s in task()['status']['subscriptions'] if s['id']=='pg')
                return sub if sub.get('state')=='PAUSED' and sub.get('failurePhase')=='TARGET_APPLY' else None

            sub = wait(compensation_failure, 'duplicate-key transaction fails atomically on PostgreSQL')
            wait(lambda:sql('mysql-target','SELECT note FROM rm_my_sales.items WHERE id=97')=='after primary-key move','healthy peer applies the original transaction')
            action('PAUSE', node='node-b')
            options = dict(subscriptionId='pg',errorCount=sub['errorGeneration'])
            evidence_request,_ = action('FAILURE_REVIEW',node='node-a',**options)
            evidence = operation(evidence_request)
            scope = dict(evidenceId=evidence['failureEvidenceId'],evidenceHash=evidence['result']['hash'])
            plan_request,_ = action('COMPENSATION_PLAN',node='node-b',compensation=scope,**options)
            wait(lambda:operation(plan_request).get('result',{}).get('hash'),'owner preview visible across entries')
            plan = operation(plan_request)['result']
            assert plan['code']=='COMPENSATION_PLAN_READY' and len(plan['tables'])==3
            assert sum(t['identities'] for t in plan['tables'].values())==4
            assert sum(t['deletes'] for t in plan['tables'].values())==4
            assert sum(t['inserts'] for t in plan['tables'].values())==3
            assert sql('postgres','SELECT note FROM rm_pg_sales.items WHERE id=97')=='conflicting key'
            approved = dict(scope,approvedHash=plan['hash'],reason='Restore four reviewed identities before replaying the original cross-database transaction')
            revision = task()['revision']
            replay = dict(action='COMPENSATION_EXECUTE',expectedRevision=revision,compensation=approved,**options)
            assert http('node-a',route+'/actions',replay,request='denied_compensation',expected=403,token=env['RM_GC_OPERATOR'])['code']=='FORBIDDEN'
            applied,_ = recovery_write('COMPENSATION_EXECUTE', sub['acknowledged'],
                                       lambda: sql('postgres','SELECT note FROM rm_pg_sales.items WHERE id=97')=='conflicting key',
                                       compensation=approved,**options)
            wait(lambda:operation(applied).get('result',{}).get('hash'),'owner compensation receipt visible across entries')
            receipt = operation(applied)['result']
            assert receipt['code']=='COMPENSATION_COMPLETED'
            assert task()['desired']=='PAUSED' and compensation_failure()['acknowledged']==sub['acknowledged']
            if args.recovery_jobs:
                # The helper already retries the original body. This deliberately uses the revision before cancellation.
                assert http('node-b',route+'/actions',replay,request=applied,expected=409)['code']=='REQUEST_ID_REUSED'
            else:
                http('node-b',route+'/actions',replay,request=applied,expected=202)
            assert operation(applied)['result']==receipt
            assert sql('postgres','SELECT COUNT(*) FROM rm_pg_sales.items WHERE id=97')=='0'
            assert sql('postgres','SELECT note FROM rm_pg_sales.items WHERE id=1')==baseline_note
            assert sql('postgres','SELECT amount FROM rm_pg_billing.ledger WHERE id=1')==baseline_amount
            assert sql('postgres','SELECT COUNT(*) FROM rm_pg_sales.duplicates WHERE value=7')=='1'
            action('RESUME_TARGET',**options)
            action('RESUME',errorCount=task()['status']['error_count'])
            wait(rows_match,'original cross-database transaction replays after compensation')
            passed('Cross-entry compensation restores keyed/keyless before-images and original primary key,retains pause/ACK,then normal CDC reproduces the original transaction')
        if args.correction:
            if args.compensation:
                sql('mysql', "INSERT INTO sales.duplicates VALUES(7,JSON_OBJECT('n',7))")
            sql('mysql', """INSERT INTO sales.items(id,note,payload)
                VALUES(98,'reviewed pre-image',JSON_OBJECT('n',18446744073709551615))""")
            wait(rows_match,'baseline for manual correction')
            occurrence=sql('postgres','SELECT _rivomesh_row_id FROM rm_pg_sales.duplicates WHERE value=7')
            assert len(occurrence)==64
            sql('postgres', """DELETE FROM rm_pg_sales.items WHERE id=98;
                INSERT INTO rm_pg_sales.items(id,note,derived) VALUES(99,'private extra row',198);
                UPDATE rm_pg_sales.duplicates SET value=96 WHERE value=7""")
            sql('mysql', """START TRANSACTION;
                UPDATE sales.items SET note='original replay' WHERE id=98;
                UPDATE sales.duplicates SET value=97 WHERE value=7 LIMIT 1;
                COMMIT""")

            def manual_failure():
                subscriber=next(s for s in task()['status']['subscriptions'] if s['id']=='pg')
                return subscriber if subscriber.get('state')=='PAUSED' and subscriber.get('failurePhase')=='TARGET_APPLY' else None

            failed=wait(manual_failure,'the original transaction fails on a missing target row')
            wait(lambda:sql('mysql-target','SELECT note FROM rm_my_sales.items WHERE id=98')=='original replay','healthy target delivers the original transaction')
            action('PAUSE')
            options=dict(subscriptionId='pg',errorCount=failed['errorGeneration'])
            reviewed,_=action('FAILURE_REVIEW',node='node-a',**options)
            wait(lambda:operation(reviewed).get('result',{}).get('hash'),'restored evidence visible on both controllers')
            evidence=operation(reviewed)
            scope=dict(evidenceId=evidence['failureEvidenceId'],evidenceHash=evidence['result']['hash'])
            request=lambda value:dict(expectedRevision=task()['revision'],correction=value,**options)
            assert http('node-a',route+'/corrections/schema',request(scope),expected=403,token=env['RM_GC_VIEWER'])['code']=='FORBIDDEN'
            schema=http('node-b',route+'/corrections/schema',request(scope))
            ids={item['name']:item['id'] for item in schema['tables']}
            edits=[
                dict(tableId=ids['sales.items'],action='INSERT',values=dict(id='98',note='reviewed pre-image',payload='{"n":18446744073709551615}',derived='196')),
                dict(tableId=ids['sales.items'],action='DELETE',key=dict(id='99')),
                dict(tableId=ids['sales.duplicates'],action='UPDATE',rowId=occurrence,values=dict(value='7')),
            ]
            malformed=dict(scope,importJson='{"edits":[],"edits":[]}')
            assert http('node-a',route+'/corrections/preview',request(malformed),expected=409)['code']=='CORRECTION_IMPORT_INVALID'
            patch=dict(scope,importJson=json.dumps(dict(edits=edits)))
            preview=http('node-b',route+'/corrections/preview',request(patch))
            assert preview['code']=='CORRECTION_PLAN_READY' and len(preview['rows'])==3
            assert any(row['before'] and row['before'].get('note')=='private extra row' for row in preview['rows'])
            assert any(row['after'] and '18446744073709551615' in (row['after'].get('payload') or '') for row in preview['rows'])
            assert sql('postgres','SELECT COUNT(*) FROM rm_pg_sales.items WHERE id=98')=='0'
            approved=dict(patch,approvedHash=preview['hash'],reason='Restore the reviewed missing row and correct the exact target occurrences')
            applied,accepted=recovery_write('CORRECTION_EXECUTE', failed['acknowledged'],
                                             lambda: sql('postgres','SELECT COUNT(*) FROM rm_pg_sales.items WHERE id=98')=='0',
                                             correction=approved,**options)
            assert 'reviewed pre-image' not in json.dumps(accepted)
            wait(lambda:operation(applied).get('result',{}).get('code')=='CORRECTION_COMPLETED','target receipt visible through both entries')
            assert 'reviewed pre-image' not in json.dumps(operation(applied))
            assert task()['desired']=='PAUSED' and manual_failure()['acknowledged']==failed['acknowledged']
            assert sql('postgres','SELECT note FROM rm_pg_sales.items WHERE id=98')=='reviewed pre-image'
            assert sql('postgres','SELECT COUNT(*) FROM rm_pg_sales.items WHERE id=99')=='0'
            assert sql('postgres',"SELECT value FROM rm_pg_sales.duplicates WHERE _rivomesh_row_id='"+occurrence+"'")=='7'
            action('RESUME_TARGET',**options)
            action('RESUME',errorCount=task()['status']['error_count'])
            wait(rows_match,'full source and target equality after correction and normal CDC replay')
            passed('Typed JSON import previews and approves exact inserts, updates and deletes across entries, preserves business-value privacy and resumes real CDC')

    try:
        databases = ('mysql', 'mysql-target', 'postgres', 'store') if args.separate_execution else ('mysql', 'mysql-target', 'postgres')
        compose('up', '-d', *databases)
        for service in databases:
            wait(lambda s=service: sql(s, 'SELECT 1') == '1', service + ' SQL ready')
        manifest['sourceVersion'] = sql('mysql','SELECT VERSION()')
        manifest['sourceServerProfile'] = sql('mysql','SELECT @@global.gtid_mode,@@global.binlog_format,@@global.binlog_row_image')
        manifest['sourceImageId'] = subprocess.check_output(['docker','image','inspect','--format','{{.Id}}',args.source_image],text=True).strip()
        assert manifest['sourceServerProfile'] == 'ON|ROW|FULL'
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
        if args.source_expected_version:
            document['source'].update(type='MYSQL', expectedVersion=args.source_expected_version)
        for product, target, url, username in [('POSTGRESQL', 'pg', 'jdbc:postgresql://postgres:5432/sync_test', 'sync_target'), ('MYSQL', 'my', 'jdbc:mysql://mysql-target:3306/?allowPublicKeyRetrieval=true&sslMode=DISABLED', 'root')]:
            document['targets'].append({'id': target, 'product': product, 'url': url, 'username': username, 'password': env['RM_GC_DATA'], 'metadataSchema': 'rm_' + target + '_meta', 'mappings': {'sales': 'rm_' + target + '_sales', 'billing': 'rm_' + target + '_billing'}})
        created = 'gc_create_' + uuid.uuid4().hex
        http('node-a', '/api/tasks', {'config': document, 'ownerNodeId': owner_node}, request=created, expected=202)
        wait(lambda: outcome(created), 'remote create')
        assert task()['ownerNodeId'] == owner_node
        assert task('node-b')['revision'] == task()['revision']
        passed('Controllers create Agent-owned task and share revision' if args.separate_execution else 'A creates B-owned task; both JVMs share task revision')
        action('CHECK')
        if args.snapshot_diagnostics:
            sql('postgres', 'CREATE SCHEMA rm_pg_meta AUTHORIZATION sync_target; CREATE SCHEMA rm_pg_sales; GRANT USAGE ON SCHEMA rm_pg_sales TO sync_target')
        action('START')
        if args.snapshot_diagnostics:
            def initial_fault(phase, generation=None):
                result = None
                for node in ('node-a', 'node-b'):
                    sub = next((s for s in task(node).get('status', {}).get('subscriptions', []) if s['id'] == 'pg'), {})
                    if sub.get('state') != 'PAUSED' or sub.get('failurePhase') != phase or 'failedSequence' in sub or sub.get('acknowledged') != 0:
                        return None
                    if generation is not None and sub.get('errorGeneration') != generation:
                        return None
                    result = sub
                return result
            preparation = wait(lambda: initial_fault('TARGET_PREPARE'), 'initial DDL permission phase through both entries')
            assert preparation['errorCode'] == 'SQL_42501'
            wait(lambda: sql('mysql-target', 'SELECT COUNT(*) FROM rm_my_sales.items') == '2', 'healthy target snapshots while PostgreSQL DDL is paused')
            sql('postgres', 'CREATE TABLE rm_pg_sales.items(id BIGINT PRIMARY KEY,note VARCHAR(50),payload JSONB,derived BIGINT,CONSTRAINT snapshot_block CHECK(id<>1)); ALTER TABLE rm_pg_sales.items OWNER TO sync_target; GRANT CREATE ON SCHEMA rm_pg_sales TO sync_target')
            action('RESUME_TARGET', errorCount=preparation['errorGeneration'], subscriptionId='pg')
            snapshot_failure = wait(lambda: initial_fault('TARGET_SNAPSHOT', preparation['errorGeneration'] + 1), 'snapshot constraint phase through both entries')
            assert snapshot_failure['errorCode'] == 'SQL_23514'
            assert sql('postgres', 'SELECT COUNT(*) FROM rm_pg_sales.items') == '0'
            action('STOP', node='node-b')
            action('START')
            wait(lambda: initial_fault('TARGET_SNAPSHOT', snapshot_failure['errorGeneration']), 'snapshot pause survives Worker restart')
            sql('postgres', 'ALTER TABLE rm_pg_sales.items DROP CONSTRAINT snapshot_block')
            action('RESUME_TARGET', errorCount=snapshot_failure['errorGeneration'], subscriptionId='pg')
            passed('DDL permission and snapshot CHECK failures remain distinct across entries/restart,carry no incremental sequence,and recover under their original error generations')
        wait(rows_match, 'snapshot exact rows')
        passed('Two source databases to two targets; snapshot keyed/keyless/JSON/generated values')
        wait(lambda: all(task(n).get('captureLeaseState')=='CLAIMED' for n in ('node-a','node-b')),
             'both entries observe the live capture lease')
        sql('mysql', "START TRANSACTION; UPDATE sales.items SET note='updated',payload=JSON_OBJECT('n',11) WHERE id=1; DELETE FROM sales.items WHERE id=2; INSERT INTO sales.items(id,note,payload) VALUES(3,'third',JSON_OBJECT('n',3)); UPDATE sales.duplicates SET value=8 WHERE value=7 LIMIT 1; UPDATE billing.ledger SET amount=456.7890 WHERE id=1; COMMIT")
        wait(rows_match, 'incremental exact rows')
        passed('Cross-database transaction insert/update/delete and keyless multiplicity')
        if args.task_views:
            run_task_views()
            action('STOP')
            assert hashlib.sha256(jar.read_bytes()).hexdigest() == digest
            manifest['status'] = 'PASS'
            print('GLOBAL_CONTROL_TASK_VIEWS_ACCEPTED artifact=' + digest)
            raise SystemExit(0)
        if args.source_diagnostics:
            run_source_diagnostics()
            action('STOP')
            assert hashlib.sha256(jar.read_bytes()).hexdigest() == digest
            manifest['status'] = 'PASS'
            print('GLOBAL_CONTROL_SOURCE_DIAGNOSTICS_ACCEPTED artifact=' + digest)
            raise SystemExit(0)
        if args.mixed_runtime_upgrade:
            run_mixed_upgrade()
        if args.runtime_jdk or args.mixed_runtime_upgrade:
            if args.runtime_jdk:
                observe_jvms()
            action('STOP')
            assert hashlib.sha256(jar.read_bytes()).hexdigest() == digest
            manifest['status'] = 'PASS'
            print('GLOBAL_CONTROL_RUNTIME_ACCEPTED artifact=' + digest)
            raise SystemExit(0)
        if args.recovery_jobs:
            run_recovery_cases()
            action('STOP')
            assert hashlib.sha256(jar.read_bytes()).hexdigest() == digest
            manifest['status'] = 'PASS'
            print('GLOBAL_CONTROL_RECOVERY_ACCEPTED artifact=' + digest)
            raise SystemExit(0)
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
            wait(lambda: http('node-a', route, expected=(200, 503)).get('ownerState') == 'HEALTHY',
                 'Agent renews registration after store outage')
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
        if args.managed_failure_review:
            review_revision = task('node-b')['revision']
            checkpoint_epoch_before = sql('postgres', 'SELECT epoch FROM rm_pg_meta.rm3_sink_job') if args.checkpoint_inspection else None
            review_request, accepted = action('FAILURE_REVIEW', node='node-b', errorCount=failed['errorGeneration'], subscriptionId='pg')
            review_operation = operation(review_request)
            evidence_id = review_operation['failureEvidenceId']
            evidence_uri = route + '/failure-evidence/' + evidence_id
            evidence_hash = review_operation['result']['hash']
            if args.checkpoint_inspection:
                observation = review_operation['result']['checkpointObservation']
                assert sql('postgres', 'SELECT epoch FROM rm_pg_meta.rm3_sink_job') == checkpoint_epoch_before
                assert observation['evidenceId'] == evidence_id and observation['evidenceHash'] == evidence_hash
                assert observation['verdict'] == 'CHECKPOINT_NOT_ADVANCED' and observation['subscriptionId'] == 'pg'
                assert observation['sequence'] == failed['acknowledged'] + 1 and observation['acknowledged'] == failed['acknowledged']
                assert observation['errorGeneration'] == failed['errorGeneration'] and 'sql-error' not in json.dumps(observation)
                assert observation['targetCheckpoint']['sequence'] == failed['acknowledged']
                passed('Managed review compares the real PostgreSQL native checkpoint to retained normalized MySQL binlog content without activating or acknowledging the paused subscriber')
            for node in ('node-a', 'node-b'):
                summary = http(node, evidence_uri)
                assert summary['id'] == evidence_id and summary['hash'] == evidence_hash and summary['rawEvents'] > 0
                assert summary['failurePhase'] == 'TARGET_APPLY' and 'sql-error' not in json.dumps(summary)
                for mode in ('RAW', 'NORMALIZED'):
                    page = http(node, evidence_uri + '/events', {'view': mode, 'from': 0, 'limit': 1})
                    assert page['id'] == evidence_id and page['hash'] == evidence_hash and 'sql-error' in json.dumps(page['events'])
                    assert page['events'][0]['table'] == 'sales.items' and any(column['name'] == 'note' for column in page['events'][0]['columns'])
                for role in ('RM_GC_OPERATOR', 'RM_GC_VIEWER'):
                    assert http(node, evidence_uri, expected=403, token=env[role])['code'] == 'FORBIDDEN'
                    assert http(node, evidence_uri + '/events', {'view': 'RAW', 'from': 0, 'limit': 1}, expected=403, token=env[role])['code'] == 'FORBIDDEN'
            http('node-a', route + '/actions', {'action': 'FAILURE_REVIEW', 'expectedRevision': review_revision, 'errorCount': failed['errorGeneration'], 'subscriptionId': 'pg'}, request=review_request, expected=202)
            assert operation(review_request)['failureEvidenceId'] == evidence_id
            assert next(s for s in task()['status']['subscriptions'] if s['id'] == 'pg')['state'] == 'PAUSED'
            passed('Protocol5 evidence creation retains one durable ID across entries/retry; real raw/normalized events are admin-only and never auto-resume the paused target')
        sql('postgres', 'ALTER TABLE rm_pg_sales.items DROP CONSTRAINT gc_block')
        if args.managed_commit:
            worker_config='/app/data/controller/worker-configs/global_cdc.yml' if args.runtime_image else '/fixture/'+owner_node+'/controller/worker-configs/global_cdc.yml'
            injected=compose('exec','-T',owner_node,'java','-Xmx128m','-cp','/app/rivomesh.jar:/fixture-tests','io.github.rivomesh.cdc.ManagedCommitFault',worker_config,'pg')
            assert 'COMMITTED_WITHOUT_ACK' in injected.stdout
            assert next(s for s in task()['status']['subscriptions'] if s['id']=='pg')['acknowledged']==failed['acknowledged']
            action('PAUSE',node='node-b')
            fresh_review,_=action('FAILURE_REVIEW',node='node-a',errorCount=failed['errorGeneration'],subscriptionId='pg')
            fresh_operation=operation(fresh_review);fresh_observation=fresh_operation['result']['checkpointObservation']
            assert fresh_observation['verdict']=='COMMITTED_UNACKNOWLEDGED' and not fresh_observation['targetFenceRequired']
            confirmation={'evidenceId':fresh_operation['failureEvidenceId'],'evidenceHash':fresh_operation['result']['hash'],'reason':'Verified native commit after deliberately withheld journal ACK'}
            confirm_revision=task('node-b')['revision']
            confirm_body={'action':'CONFIRM_COMMIT','expectedRevision':confirm_revision,'errorCount':failed['errorGeneration'],'subscriptionId':'pg','commit':confirmation}
            for role in ('RM_GC_OPERATOR','RM_GC_VIEWER'):
                assert http('node-b',route+'/actions',confirm_body,expected=403,token=env[role])['code']=='FORBIDDEN'
            confirm_request,_=action('CONFIRM_COMMIT',node='node-b',errorCount=failed['errorGeneration'],subscriptionId='pg',commit=confirmation)
            confirmed=operation(confirm_request);receipt=confirmed['result']['receipt'];sub=next(s for s in task()['status']['subscriptions'] if s['id']=='pg')
            assert sub['state']=='PAUSED' and sub['acknowledged']==fresh_observation['sequence'] and sub['errorGeneration']==failed['errorGeneration']+1
            assert task()['desired']=='PAUSED' and task()['status']['run_state']=='PAUSED'
            assert receipt['evidenceHash']==confirmation['evidenceHash'] and receipt['sequence']==fresh_observation['sequence']
            assert sql('postgres','SELECT COUNT(*) FROM rm_pg_sales.items WHERE id=9')=='1'
            http('node-a',route+'/actions',confirm_body,request=confirm_request,expected=202)
            assert operation(confirm_request)['result']['hash']==confirmed['result']['hash']
            passed('Protocol6 cross-entry administrator confirmation restores exactly the proven native commit ACK;durable request retry keeps its receipt,business rows are not reapplied,and both task and destination stay paused')
            action('RESUME_TARGET',errorCount=sub['errorGeneration'],subscriptionId='pg')
            action('RESUME',errorCount=task()['status']['error_count'])
        else:
            action('RESUME_TARGET', errorCount=failed['errorGeneration'], subscriptionId='pg')
        wait(rows_match, 'SQL error replay')
        passed('Native SQL constraint error pauses target; explicit generation-bound replay catches up')
        compose('kill', '-s', 'SIGKILL', owner_node)
        wait(lambda: task().get('ownerState') != 'HEALTHY', 'owner offline')
        assert task().get('captureLeaseState','UNKNOWN')=='UNKNOWN'
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
        wait(lambda: task(surviving_entry).get('captureLeaseState') in ('UNCLAIMED','EXPIRED'),
             'capture lease inactive after offline STOP')
        passed('Both entries report the capture lease separately from process liveness; stale owner state reports unknown')
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
        if args.managed_failure_review:
            wait(lambda: task()['status'].get('pendingBytes') == 0, 'original journal payload collected after all targets ACK')
            retained = http('node-b', evidence_uri)
            assert retained['hash'] == evidence_hash
            page = http('node-a', evidence_uri + '/events', {'view': 'RAW', 'from': 0, 'limit': 1})
            assert page['hash'] == evidence_hash and 'sql-error' in json.dumps(page['events'])
            passed('Original Agent directory retains evidence across SIGKILL/reopen and actual journal payload GC; admin raw review still reads the same historical transaction')
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
        validation, _ = action('VALIDATE', complete=True, validation={'targets': ['pg', 'my'], 'gateMillis': 15000, 'sourceMillis': 60000, 'targetMillis': 30000})
        wait(lambda: operation(validation).get('validation', {}).get('reportId'), 'visible report identity')
        report_id = operation(validation)['validation']['reportId']
        report_path = route + '/validations/' + report_id
        report = http('node-a', report_path)
        assert report['code'] == 'VALIDATION_DIFFERENCES'
        assert http('node-b', report_path)['results'] == report['results']
        assert task()['desired'] == 'PAUSED'
        scope = [key for key, row in report['results'].items() if row['missing'] + row['extra'] + row['changed'] > 0]
        assert len(scope) == 2
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
        action('RESUME', errorCount=task()['status']['error_count'])
        sql('mysql', "INSERT INTO sales.items(id,note,payload) VALUES(11,'after-repair',JSON_OBJECT('n',11))")
        wait(rows_match, 'explicit post-repair CDC resume')
        passed('Explicit resume after reviewed repair continues original binlog progress')
        if args.managed_skip:
            def pg_sub():
                return next(s for s in task()['status']['subscriptions'] if s['id'] == 'pg')

            def failed_skip():
                sub = pg_sub()
                return sub if sub.get('state') == 'PAUSED' and sub.get('failurePhase') == 'TARGET_APPLY' else None

            def quote(value):
                return "'" + value.replace("'", "''") + "'"

            baseline_note = sql('mysql', 'SELECT note FROM sales.items WHERE id=1')
            baseline_amount = sql('mysql', 'SELECT amount FROM billing.ledger WHERE id=1')
            sql('postgres', "ALTER TABLE rm_pg_sales.items ADD CONSTRAINT managed_skip_block CHECK(note NOT IN ('skip_forbidden_one','skip_forbidden_two'))")
            for index, uncertain in enumerate((False, True), start=1):
                forbidden = 'skip_forbidden_two' if uncertain else 'skip_forbidden_one'
                sql('mysql', "START TRANSACTION; UPDATE sales.items SET note=" + quote(forbidden) + " WHERE id=1; UPDATE billing.ledger SET amount=amount+1 WHERE id=1; COMMIT")
                failure = wait(failed_skip, 'whole-transaction target failure before skip')
                wait(lambda: sql('mysql-target', 'SELECT note FROM rm_my_sales.items WHERE id=1') == forbidden, 'healthy subscriber receives original transaction')
                action('PAUSE', node='node-b')
                reviewed, _ = action('FAILURE_REVIEW', node='node-a', errorCount=failure['errorGeneration'], subscriptionId='pg')
                evidence = operation(reviewed)
                observation = evidence['result']['checkpointObservation']
                assert observation['verdict'] == 'CHECKPOINT_NOT_ADVANCED' and not observation['targetFenceRequired']
                approval = {'evidenceId': evidence['failureEvidenceId'], 'evidenceHash': evidence['result']['hash'], 'reason': 'Reviewed the complete cross-database transaction and explicitly accept this destination gap', 'acceptDataLoss': True}
                for role in ('RM_GC_OPERATOR', 'RM_GC_VIEWER'):
                    forbidden_body = {'action': 'SKIP_TRANSACTION', 'expectedRevision': task()['revision'], 'subscriptionId': 'pg', 'errorCount': failure['errorGeneration'], 'skip': approval}
                    assert http('node-b', route + '/actions', forbidden_body, expected=403, token=env[role])['code'] == 'FORBIDDEN'
                if uncertain:
                    injected = compose('exec', '-T', owner_node, 'java', '-Xmx128m', '-cp', '/app/rivomesh.jar:/fixture-tests', 'io.github.rivomesh.cdc.ManagedSkipFault', worker_config, 'pg', 'BLOCK_ACK')
                    assert 'MANAGED_SKIP_BLOCK_ACK' in injected.stdout
                original_revision = task()['revision']
                skipped, _ = action('SKIP_TRANSACTION', node='node-b', complete=not uncertain, errorCount=failure['errorGeneration'], subscriptionId='pg', skip=approval)
                plan_path = route + '/recovery-plans/' + skipped
                if uncertain:
                    wait(lambda: operation(skipped)['status'] == 'FAILED', 'injected journal ACK failure')
                    prepared = http('node-a', plan_path)
                    assert prepared['state'] == 'PREPARED' and pg_sub()['acknowledged'] == observation['acknowledged']
                    proof = http('node-b', plan_path + '/inspect', {})
                    assert proof['nativeProof']['state'] == 'COMMITTED'
                    resume, _ = action('RESUME', complete=False, errorCount=task()['status']['error_count'])
                    wait(lambda: operation(resume)['status'] == 'FAILED', 'pending plan refuses ordinary resume')
                    assert task()['desired'] == 'PAUSED'
                    compose('kill', '-s', 'SIGKILL', owner_node)
                    wait(lambda: task().get('ownerState') != 'HEALTHY', 'prepared-plan Agent offline')
                    compose('up', '-d', owner_node)
                    wait(lambda: task().get('ownerState') == 'HEALTHY', 'original Agent reopens prepared plan')
                    proof = http('node-a', plan_path + '/inspect', {})
                    assert proof['state'] == 'PREPARED' and proof['nativeProof']['state'] == 'COMMITTED'
                    injected = compose('exec', '-T', owner_node, 'java', '-Xmx128m', '-cp', '/app/rivomesh.jar:/fixture-tests', 'io.github.rivomesh.cdc.ManagedSkipFault', worker_config, 'pg', 'CLEAR_ACK')
                    assert 'MANAGED_SKIP_CLEAR_ACK' in injected.stdout
                    recovered, _ = action('SKIP_RECOVER', node='node-a', errorCount=failure['errorGeneration'], subscriptionId='pg', skip={'planId': skipped, 'planHash': prepared['planHash'], 'reason': 'Verified native committed receipt after original Agent restart and recover only the source gap acknowledgment', 'acceptDataLoss': False})
                    assert operation(recovered)['result']['receipt']['executionRequest'] == skipped
                plan = http('node-b', plan_path)
                assert plan['state'] == 'COMPLETED' and len(plan['schemaVersions']) == 2
                assert pg_sub()['acknowledged'] == observation['sequence'] and pg_sub()['state'] == 'PAUSED'
                wait(lambda: task()['status'].get('gapCount') == index, 'published durable gap count through the controller')
                assert task()['desired'] == 'PAUSED' and task()['status']['gapCount'] == index
                assert sql('postgres', 'SELECT note FROM rm_pg_sales.items WHERE id=1') == baseline_note
                assert sql('postgres', 'SELECT amount FROM rm_pg_billing.ledger WHERE id=1') == baseline_amount
                assert sql('postgres', 'SELECT skip_operation FROM rm_pg_meta.rm3_sink_job') == skipped
                page = http('node-a', route + '/gaps', {'subscriptionId': 'pg', 'after': '0', 'limit': 20})
                assert len(page['items']) == index and page['items'][-1]['sequence'] == str(observation['sequence'])
                for role in ('RM_GC_OPERATOR', 'RM_GC_VIEWER'):
                    assert http('node-a', plan_path, expected=403, token=env[role])['code'] == 'FORBIDDEN'
                if not uncertain:
                    replay = {'action': 'SKIP_TRANSACTION', 'expectedRevision': original_revision, 'subscriptionId': 'pg', 'errorCount': failure['errorGeneration'], 'skip': approval}
                    http('node-a', route + '/actions', replay, request=skipped, expected=202)
                    assert http('node-a', plan_path)['receipt'] == plan['receipt']
                passed('Protocol7 ' + ('native skip commit survives source ACK failure and original Agent SIGKILL;another entry recovers exactly one gap' if uncertain else 'administrator skips one complete cross-database transaction;healthy subscriber receives original events and business rows stay unchanged'))
                action('RESUME_TARGET', errorCount=pg_sub()['errorGeneration'], subscriptionId='pg')
                action('RESUME', errorCount=task()['status']['error_count'])
                sql('mysql', 'START TRANSACTION; UPDATE sales.items SET note=' + quote(baseline_note) + ' WHERE id=1; UPDATE billing.ledger SET amount=' + baseline_amount + ' WHERE id=1; COMMIT')
                wait(rows_match, 'next original APPLY after explicit resume')
                assert sql('postgres', "SELECT COALESCE(skip_operation,'') FROM rm_pg_meta.rm3_sink_job") == ''
                assert task()['status']['hasGaps'] and task()['status']['gapCount'] == index
            sql('postgres', 'ALTER TABLE rm_pg_sales.items DROP CONSTRAINT managed_skip_block')
            passed('Normal APPLY clears only current native skip markers;both durable gaps remain visible after data converges again')
            validation,_=action('VALIDATE',node='node-b',validation={'targets':['pg','my'],'gateMillis':15000,'sourceMillis':60000,'targetMillis':30000})
            report_id=operation(validation)['validation']['reportId']
            report=http('node-a',route+'/validations/'+report_id)
            assert report['state']=='COMPLETE' and report['code']=='VALIDATION_MATCH'
            gaps=http('node-b',route+'/gaps',{'subscriptionId':'pg','after':'0','limit':20})['items']
            assert len(gaps)==2 and all(row['state']=='OPEN' for row in gaps)
            gap_generation=task()['status']['error_count']
            review_request,_=action('GAP_VERIFY_PLAN',node='node-b',subscriptionId='pg',errorCount=gap_generation,gap={'reportId':report_id,'gaps':[row['planId'] for row in gaps],'maximumMillis':30000,'reason':'Review both complete cross-database skip gaps against the new sealed matching report'})
            verify_path=route+'/gap-verifications/'+review_request
            review=http('node-a',verify_path)
            assert review['state']=='PREPARED' and len(review['plan']['gaps'])==2 and len(review['plan']['tables'])==2
            for role in ('RM_GC_OPERATOR','RM_GC_VIEWER'):
                assert http('node-a',verify_path,expected=403,token=env[role])['code']=='FORBIDDEN'
            gap_approval={'planId':review_request,'approvedHash':review['hash'],'reason':'Approved both reviewed gaps and exclusively service-written target tables'}
            verified,_=action('GAP_VERIFY_EXECUTE',node='node-a',subscriptionId='pg',errorCount=review['plan']['errorGeneration'],gap=gap_approval)
            receipt=operation(verified)['result']['receipt']
            assert receipt['outcome']=='VERIFIED_AT_COMMON_CUT'
            assert http('node-b',verify_path)['receipt']['hash']==receipt['hash']
            wait(lambda:task()['status'].get('verifiedGapCount')==2,'published verified history')
            state=task()['status']
            assert state['hasGaps'] and not state['hasOpenGaps'] and state['openGapCount']==0 and task()['desired']=='PAUSED'
            after=http('node-a',route+'/gaps',{'subscriptionId':'pg','after':'0','limit':20})['items']
            assert len(after)==2 and all(row['state']=='VERIFIED_AT_COMMON_CUT' for row in after)
            assert all(row['verificationId']==review_request for row in after)
            passed('Cross-entry reviewed gap verification freshly scans two affected tables,atomically records both gaps and preserves SKIP history and the whole-task pause')

        run_recovery_cases()
        action('STOP')
        assert hashlib.sha256(jar.read_bytes()).hexdigest() == digest
        manifest['status'] = 'PASS'
    except Exception as failure:
        manifest['status'] = 'FAIL'
        manifest['failureType'] = type(failure).__name__
        try:
            observed = task()
            diagnostics['task'] = {key: observed.get(key) for key in ('workerState', 'captureLeaseState', 'desired', 'problem', 'statusAvailable', 'status', 'operations')}
        except Exception:
            pass
        for node in members:
            logs = compose('logs', '--no-color', '--tail', '60', node, check=False)
            diagnostics[node] = logs.stdout + logs.stderr
        safe = json.dumps(diagnostics, indent=2)
        for key in ('RM_GC_TOKEN', 'RM_GC_CONTROL', 'RM_GC_DATA', 'RM_GC_KEY', 'RM_GC_OPERATOR', 'RM_GC_VIEWER'):
            if key in env:
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
