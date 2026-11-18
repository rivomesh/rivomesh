#!/usr/bin/env python3
"""Verify MySQL fan-out, RabbitMQ quorum failure and manual consumer receipts."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
MEMBERS = ('rabbit-a', 'rabbit-b', 'rabbit-c')
QUEUES = {'orders.items', 'orders.__schema', 'orders.__transactions'}
IMAGES = dict(rabbit='rabbitmq:4.1.4-alpine', mysql='mysql:8.4',
              postgres='postgres:17.12', runtime='eclipse-temurin:21-jre-jammy')
READER = ROOT / 'tests/rabbit/ReplicatedRabbitAcceptance.java'
PIPELINE = 'rabbit_quorum'


def sha256(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def require(check, label, seconds=100):
    end = time.monotonic() + seconds
    last_failure = None
    while time.monotonic() < end:
        try:
            if result := check():
                return result
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as failure:
            last_failure = failure
        time.sleep(.4)
    raise AssertionError('Timed out: ' + label) from last_failure


def setup(home, artifact):
    tasks = home / 'tasks'
    tasks.mkdir()
    connection = dict(url='jdbc:postgresql://postgres:5432/fixture?connectTimeout=3',
                      username='postgres', password='${RM_RABBIT_DB_PASSWORD}')
    pipeline = dict(formatVersion=3, pipelineId=PIPELINE, nodeId='rabbit_worker', databases=['sales'],
                    source=dict(type='MYSQL', clusterId='quorum_fixture', topology='AUTO',
                                host='mysql', port=3306, username='root',
                                password='${RM_RABBIT_DB_PASSWORD}', serverId=450001),
                    targets=[dict(id='pg', product='POSTGRESQL', **connection,
                                  mappings=dict(sales='copy_sales'), metadataSchema='copy_meta'),
                             dict(id='rabbit', product='RABBITMQ', url='amqp://rabbit-a:5672',
                                  username='rabbit_fixture', password='${RM_RABBIT_PASSWORD}',
                                  mappings=dict(sales='orders'), messaging=dict(
                                      queueType='quorum', replicas=3, autoCreate=True,
                                      endpoints=['amqp://rabbit-b:5672', 'amqp://rabbit-c:5672'],
                                      connectTimeoutMillis=2000, requestTimeoutMillis=3000))],
                    journal=dict(mode='EXTERNAL', product='POSTGRESQL', **connection, namespace='proof_journal'),
                    chunkSize=2, leaseMillis=5000, heartbeatMillis=500, pollIntervalMillis=150,
                    retryDelayMillis=300)
    (tasks / (PIPELINE + '.yml')).write_text(json.dumps(pipeline))
    platform = dict(bind='0.0.0.0', port=8080, authToken='${RM_RABBIT_TOKEN}',
                    tasksDirectory='/fixture/tasks', dataDirectory='/fixture/platform',
                    autoStart=True, pollMillis=250, workerHeapMiB=256)
    (home / 'platform.json').write_text(json.dumps(platform))

    services = dict(
        mysql=dict(image=IMAGES['mysql'], environment={
            'MYSQL_ROOT_PASSWORD': '${RM_RABBIT_DB_PASSWORD}', 'MYSQL_ROOT_HOST': '%'},
            command=['--server-id=1', '--log-bin=mysql-bin', '--binlog-format=ROW',
                     '--binlog-row-image=FULL', '--binlog-transaction-compression=OFF',
                     '--gtid-mode=ON', '--enforce-gtid-consistency=ON']),
        postgres=dict(image=IMAGES['postgres'], environment={
            'POSTGRES_DB': 'fixture', 'POSTGRES_PASSWORD': '${RM_RABBIT_DB_PASSWORD}'}),
        runner=dict(image=IMAGES['runtime'], entrypoint=['java', '-Xmx384m', '-jar', '/app/rivomesh.jar'],
                    command=['serve', '/fixture/platform.json'],
                    environment={key: '${'+key+'}' for key in
                                 ('RM_RABBIT_TOKEN', 'RM_RABBIT_PASSWORD', 'RM_RABBIT_DB_PASSWORD')},
                    volumes=[str(artifact) + ':/app/rivomesh.jar:ro', str(home) + ':/fixture'],
                    ports=['127.0.0.1::8080']))
    broker_conf = home / 'rabbitmq.conf'
    broker_conf.write_text('queue_leader_locator = client-local\n')
    for member in MEMBERS:
        services[member] = dict(
            image=IMAGES['rabbit'], hostname=member, ports=['127.0.0.1::5672'],
            environment={'RABBITMQ_DEFAULT_USER': 'rabbit_fixture',
                         'RABBITMQ_DEFAULT_PASS': '${RM_RABBIT_PASSWORD}',
                         'RM_RABBIT_COOKIE': '${RM_RABBIT_COOKIE}',
                         'RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS': '+S 2:2 +A 4',
                         'RABBITMQ_CTL_ERL_ARGS': '+S 1:1 +A 2'},
            entrypoint=['sh', '-ec', """
                cookie=/var/lib/rabbitmq/.erlang.cookie
                if [ ! -s "$$cookie" ]; then
                    umask 077
                    printf '%s' "$$RM_RABBIT_COOKIE" > "$$cookie"
                    chown rabbitmq:rabbitmq "$$cookie"
                    chmod 400 "$$cookie"
                fi
                exec docker-entrypoint.sh rabbitmq-server
            """],
            volumes=[member + ':/var/lib/rabbitmq', str(broker_conf) + ':/etc/rabbitmq/conf.d/95-fixture.conf:ro'])
    return dict(services=services, volumes={member: {} for member in MEMBERS})


def verify_deliveries(deliveries, snapshot, transactions):
    entries = {}
    for delivery in deliveries:
        raw = base64.b64decode(delivery['bodyBase64'], validate=True)
        envelope = json.loads(raw)
        assert envelope['id'] == delivery['id']
        assert envelope['pipelineId'] == PIPELINE and envelope['subscriptionId'] == 'rabbit'
        assert envelope['namespace'] == 'orders' and envelope['version'] == 1
        queue = {'SCHEMA': 'orders.__schema', 'SNAPSHOT_ROW': 'orders.items',
                 'ROW': 'orders.items'}.get(envelope['kind'], 'orders.__transactions')
        assert delivery['queue'] == queue
        previous = entries.setdefault(delivery['id'], (envelope, raw))
        assert previous[1] == raw, 'Repeated event ID with different broker bytes'

    events = [event for event, _ in entries.values()]
    generations = {event['generation'] for event in events}
    assert len(generations) == 1 and all(item for item in generations)
    schema, = [e for e in events if e['kind'] == 'SCHEMA']
    assert [column['name'] for column in schema['schema']['columns']] == ['id', 'label']
    for kind in ('SNAPSHOT_BEGIN', 'SNAPSHOT_ROW', 'SNAPSHOT_END'):
        observed, = [event for event in events if event['kind'] == kind]
        assert observed['snapshotId'] == snapshot and observed['tableId'] == schema['tableId']
        if kind == 'SNAPSHOT_ROW':
            assert [value['value'] for value in observed['change']['after']] == ['1', 'snapshot']
        else:
            assert observed['rows'] == 1 and observed['parts'] == 1

    for gtid, (source_receipt, expected_rows) in transactions.items():
        seq, expected_hash = source_receipt
        group = [event for event in events if event['transactionId'] == gtid]
        assert len(group) == len(expected_rows) + 2
        assert all(event['sequence'] == seq and event['position']['sourceType'] == 'MYSQL' for event in group)
        begin, = [event for event in group if event['kind'] == 'BEGIN']
        commit, = [event for event in group if event['kind'] == 'COMMIT']
        assert commit['contentHash'] == expected_hash
        for control in (begin, commit):
            assert control['totalChanges'] == len(expected_rows)
            assert control['expectedChanges'] == {'orders': len(expected_rows)}
        rows = sorted((event for event in group if event['kind'] == 'ROW'), key=lambda event: event['ordinal'])
        assert [event['ordinal'] for event in rows] == list(range(len(rows)))
        actual = [(event['change']['operation'], [value['value'] for value in event['change']['after']])
                  for event in rows]
        assert actual == expected_rows
    assert len(events) == 4 + sum(len(expected_rows) + 2 for _, expected_rows in transactions.values())
    return {event_id: hashlib.sha256(raw).hexdigest() for event_id, (_, raw) in entries.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--java-home', type=Path, default=os.environ.get('JAVA_HOME'),
                        help='Temurin 21 JDK for the independent AMQP consumer')
    args = parser.parse_args()
    artifact, output = args.artifact.resolve(), args.output.resolve()
    for path in (artifact, output):
        path.relative_to(ROOT)
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    project = 'rivomesh-rabbit-quorum-' + secrets.token_hex(5)
    secret_values = {key: secrets.token_hex(24) for key in
                     ('RM_RABBIT_TOKEN', 'RM_RABBIT_COOKIE', 'RM_RABBIT_PASSWORD', 'RM_RABBIT_DB_PASSWORD')}
    environment = dict(os.environ, **secret_values)
    java = str(args.java_home / 'bin/java') if args.java_home else 'java'
    javac = str(args.java_home / 'bin/javac') if args.java_home else 'javac'
    report = dict(status='FAIL', artifactSha256=sha256(artifact), checks=[], images={},
                  consumerSourceSha256=sha256(READER), harnessSha256=sha256(Path(__file__)))
    local_http = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def run(*command, check=True, timeout=180):
        return subprocess.run(command, cwd=ROOT, env=environment, text=True, capture_output=True,
                              check=check, timeout=timeout)

    def passed(label):
        report['checks'].append(label)
        print('PASS ' + label, flush=True)

    def redact(value):
        for secret in secret_values.values():
            value = value.replace(secret, '[REDACTED]')
        return value

    with tempfile.TemporaryDirectory(prefix='rabbit-quorum-', dir=output) as temporary:
        home = Path(temporary)
        compose_file = home/'compose.json'
        compose_file.write_text(json.dumps(setup(home, artifact)))
        docker_compose = ('docker', 'compose', '--env-file', '/dev/null', '-p', project, '-f', str(compose_file))

        def compose(*command, **options):
            return run(*docker_compose, *command, **options)

        def mysql(statement, local=False):
            host = '127.0.0.1' if local else 'localhost'
            return compose('exec','-T','mysql','sh','-ec',
                           'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -h "$1" -uroot -N -B -r -e "$2"',
                           'sh', host, statement, timeout=20).stdout.strip()

        def pg(statement, check=True):
            result = compose('exec','-T','postgres','psql','-X','-v','ON_ERROR_STOP=1','-d','fixture',
                             '-U','postgres','-At','-F','|','-c',statement, check=check, timeout=20)
            return result.stdout.strip() if result.returncode == 0 else ''

        def broker(member, tool, *arguments):
            return compose('exec','-T','-u','rabbitmq',member,tool,*arguments, timeout=15).stdout.strip()

        def running(member):
            return broker(member,'rabbitmq-diagnostics','-q','check_running') is not None

        def queues(member, online):
            raw = broker(member,'rabbitmqctl','-q','list_queues','--online','--formatter=json',
                         'name','type','durable','auto_delete','leader','members','online')
            details = {item['name']: item for item in json.loads(raw)}
            members = {'rabbit@' + node for node in MEMBERS}
            active = {'rabbit@' + node for node in online}
            if set(details) != QUEUES or any(
                    row['type'] != 'quorum' or not row['durable'] or row['auto_delete']
                    or set(row['members']) != members or set(row['online']) != active
                    or row['leader'] not in active for row in details.values()):
                return None
            return details

        def port(service, internal):
            return compose('port',service,str(internal)).stdout.strip().rsplit(':',1)[1]

        def status():
            request = urllib.request.Request(runner_url+'/api/tasks/'+PIPELINE,
                                             headers={'Authorization':'Bearer '+secret_values['RM_RABBIT_TOKEN']})
            with local_http.open(request,timeout=4) as response:
                return json.load(response).get('status', {})

        def rabbit_state():
            return next((item for item in status().get('subscriptions', []) if item['id']=='rabbit'), {})

        def published():
            return pg("SELECT stream_seq,transaction_id,transaction_hash FROM proof_journal.rm3_publication "
                      "WHERE pipeline_id='rabbit_quorum' AND subscription_id='rabbit'",check=False)

        def snapshot_id():
            return pg("SELECT snapshot_id FROM proof_journal.rm3_publication_snapshot "
                      "WHERE pipeline_id='rabbit_quorum' AND subscription_id='rabbit' AND run_state='COMPLETE'",
                      check=False)

        def consumer(member, mode, stem):
            receipt = output / (stem + '-consumer.json')
            run(java,'-Xmx128m','-cp',os.pathsep.join((str(home),str(artifact))),
                'io.github.rivomesh.acceptance.ReplicatedRabbitAcceptance',
                port(member,5672),mode,str(receipt),timeout=30)
            return json.loads(receipt.read_text())

        def mutation(statement):
            previous = mysql('SELECT @@GLOBAL.gtid_executed')
            assert re.fullmatch(r'[0-9a-f:,-]+', previous)
            mysql('START TRANSACTION; '+statement+'; COMMIT;')
            gtid = mysql("SELECT GTID_SUBTRACT(@@GLOBAL.gtid_executed,'"+previous+"')")
            assert re.fullmatch(r'[0-9a-f-]{36}:[1-9][0-9]*', gtid), 'Expected one source GTID'
            return gtid

        def source_receipt(gtid):
            def lookup():
                return pg("SELECT stream_seq,payload_hash FROM proof_journal.rm3_receipt "
                          f"WHERE pipeline_id='rabbit_quorum' AND transaction_id='{gtid}'")
            sequence, digest = require(lookup,'retained source receipt '+gtid).split('|')
            assert re.fullmatch(r'[0-9a-f]{64}',digest)
            return int(sequence), digest

        def accepted(gtid, receipt):
            seq, digest = receipt
            return published() == f'{seq}|{gtid}|{digest}' and rabbit_state().get('pendingTransactions') == '0'

        def sql_row(id_value, label):
            return pg(f'SELECT label FROM copy_sales.items WHERE id={id_value}',check=False)==label

        started = False
        try:
            report['consumerJava'] = run(java,'-version').stderr.splitlines()[0]
            assert re.match(r'openjdk version "21(?:\.|")',report['consumerJava']), 'Temurin 21 required'
            run(javac,'--release','21','-cp',str(artifact),'-d',str(home),str(READER))
            started = True
            compose('up','-d','mysql','postgres',*MEMBERS)
            require(lambda:mysql('SELECT 1',local=True)=='1','MySQL TCP ready')
            require(lambda:pg('SELECT 1',check=False)=='1','PostgreSQL ready')
            for member in MEMBERS:
                require(lambda:running(member),'RabbitMQ ready '+member)
            for member in MEMBERS[1:]:
                broker(member,'rabbitmqctl','join_cluster','rabbit@'+MEMBERS[0])
            report['rabbitVersion'] = broker(MEMBERS[0],'rabbitmq-diagnostics','-q','server_version')
            for role, tag in IMAGES.items():
                report['images'][role] = {'tag':tag, 'id':run('docker','image','inspect','--format','{{.Id}}',tag).stdout.strip()}
            mysql("CREATE DATABASE sales; CREATE TABLE sales.items (id INT PRIMARY KEY,label VARCHAR(80) NOT NULL); "
                  "INSERT INTO sales.items VALUES (1,'snapshot')")
            compose('run','--rm','--no-deps','runner','check',f'/fixture/tasks/{PIPELINE}.yml')
            compose('up','-d','runner')
            runner_url = 'http://127.0.0.1:'+port('runner',8080)
            report['runnerJava'] = compose('exec','-T','runner','java','-version').stderr.splitlines()[0]
            assert re.match(r'openjdk version "21(?:\.|")',report['runnerJava'])
            snapshot = require(lambda:snapshot_id() if status().get('phase')=='STREAMING' and sql_row(1,'snapshot') else '',
                               'initial snapshot committed across both destinations')
            initial = require(lambda:queues(MEMBERS[0],MEMBERS),'three durable replicas per route')
            report['initialQueues'] = initial
            leaders = {row['leader'] for row in initial.values()}
            assert len(leaders)==1, 'Resource creation must use the same broker connection'
            leader = leaders.pop().split('@',1)[1]
            assert 'rivomesh-rabbit' in broker(leader,'rabbitmqctl','-q','list_connections','client_properties')

            transactions = {}
            before = mutation("INSERT INTO sales.items VALUES (2,'pre-leader')")
            transactions[before] = (source_receipt(before), [('INSERT',['2','pre-leader'])])
            require(lambda:accepted(before,transactions[before][0]),'confirmed pre-leader publication')
            initial_deliveries = consumer(leader,'STAGE','initial')
            original = verify_deliveries(initial_deliveries,snapshot,transactions)
            passed('Three durable quorum replicas confirm the original snapshot and GTID; the consumer stages persistent bytes before ACK')

            compose('kill','-s','SIGKILL',leader)
            surviving = tuple(member for member in MEMBERS if member!=leader)
            promoted = require(lambda:queues(surviving[0],surviving),'successor leaders on two surviving members')
            report['promotedQueues'] = promoted
            row_leader = promoted['orders.items']['leader'].split('@',1)[1]
            assert row_leader != leader
            moved = mutation("UPDATE sales.items SET label='leader-moved' WHERE id=1; "
                             "INSERT INTO sales.items VALUES (3,'leader-up')")
            transactions[moved] = (source_receipt(moved), [
                ('UPDATE',['1','leader-moved']), ('INSERT',['3','leader-up'])])
            require(lambda:accepted(moved,transactions[moved][0]) and sql_row(3,'leader-up'),
                    'publisher automatically reconnects and confirms on a new leader')
            moved_deliveries = consumer(row_leader,'STAGE','leader-failover')
            elected = verify_deliveries(moved_deliveries,snapshot,transactions)
            assert original.items() <= elected.items()
            assert set(original) <= {item['id'] for item in moved_deliveries if item['redelivered']}
            passed('Killing the connected leader elects new leaders and preserves exact original delivery bytes and IDs')

            stable = published()
            other, = set(surviving) - {row_leader}
            compose('kill','-s','SIGKILL',other)
            blocked = mutation("INSERT INTO sales.items VALUES (4,'quorum-gap'),(5,'same-transaction')")
            transactions[blocked] = (source_receipt(blocked), [
                ('INSERT',['4','quorum-gap']), ('INSERT',['5','same-transaction'])])

            def blocked_at_receipt():
                sub = rabbit_state()
                return (sql_row(5,'same-transaction') and sub.get('state')=='RUNNING' and sub.get('errorCode')
                        and int(sub.get('pendingTransactions','0'))>0 and published()==stable and sub)

            stalled = require(blocked_at_receipt,'without majority SQL proceeds while broker receipt remains frozen')
            assert stalled['progressSemantics']=='BROKER_ACCEPTED'
            assert int(stalled['acknowledged'])==transactions[moved][0][0]
            report['withoutMajority'] = dict(confirmed=stable,retryCode=stalled['errorCode'],sourceGtid=blocked)
            passed('One surviving replica leaves the RabbitMQ receipt frozen while the SQL destination applies the native source GTID')

            compose('kill','-s','SIGKILL','runner')
            offline = mutation("INSERT INTO sales.items VALUES (6,'offline-worker')")
            compose('start','runner')
            runner_url = 'http://127.0.0.1:'+port('runner',8080)
            transactions[offline] = (source_receipt(offline), [('INSERT',['6','offline-worker'])])
            require(lambda:sql_row(6,'offline-worker') and blocked_at_receipt(),
                    'worker restarts on the same log while the broker remains without a majority')
            assert snapshot_id()==snapshot
            passed('Controller and Worker SIGKILL keep the same publication receipt and snapshot while SQL resumes')

            compose('start',other)
            require(lambda:running(other),'first restored broker '+other)
            require(lambda:queues(row_leader,(row_leader,other)),'majority restored on original volumes')
            compose('start',leader)
            require(lambda:running(leader),'second restored broker '+leader)
            restored = require(lambda:queues(row_leader,MEMBERS),'all original replicas rejoin')
            report['restoredQueues'] = restored
            require(lambda:accepted(offline,transactions[offline][0]) and not rabbit_state().get('errorCode'),
                    'automatic catch-up after majority restoration')
            assert pg('SELECT snapshot_id FROM copy_meta.rm3_sink_table')==snapshot and snapshot_id()==snapshot
            final = consumer(row_leader,'ACK','restored')
            recovered = verify_deliveries(final,snapshot,transactions)
            assert elected.items() <= recovered.items()
            assert set(elected) <= {item['id'] for item in final if item['redelivered']}
            assert consumer(row_leader,'STAGE','after-ack')==[], 'Acknowledged fixture messages returned'
            passed('Restored majority resumes the same GTIDs and exact content, then manual ACK clears durably staged deliveries')

            expected = ['1|leader-moved','2|pre-leader','3|leader-up','4|quorum-gap','5|same-transaction','6|offline-worker']
            assert mysql('SELECT CONCAT(id,CHAR(124),label) FROM sales.items ORDER BY id').splitlines()==expected
            assert pg('SELECT id,label FROM copy_sales.items ORDER BY id').splitlines()==expected
            report['sourceReceipts'] = {gtid: receipt for gtid,(receipt,_) in transactions.items()}
            report['snapshotId'] = snapshot
            report['consumerEventIds'] = recovered
            report['deliveredCount'] = len(final)
            assert sha256(artifact)==report['artifactSha256']
            passed('MySQL and PostgreSQL rows match, and every unique AMQP transaction envelope matches its retained native source receipt')
            report['status'] = 'PASS'
        except subprocess.CalledProcessError as failure:
            (output/'failure.log').write_text(redact((failure.stdout or '')+'\n'+(failure.stderr or '')))
            raise
        finally:
            try:
                if started:
                    try:
                        logs = compose('logs','--no-color','--tail','70',check=False,timeout=45)
                        (output/'fixture-redacted.log').write_text(redact(logs.stdout+'\n'+logs.stderr))
                    finally:
                        cleanup = compose('down','-v','--remove-orphans',check=False,timeout=90)
                        report['cleanupExitCode'] = cleanup.returncode
                        (output/'cleanup.log').write_text(redact(cleanup.stdout+'\n'+cleanup.stderr))
            finally:
                if started and report.get('cleanupExitCode')!=0:
                    report['status'] = 'FAIL'
                (output/'manifest.json').write_text(json.dumps(report,indent=2)+'\n')
            if report.get('cleanupExitCode'):
                raise RuntimeError('RabbitMQ quorum fixture cleanup failed')


if __name__=='__main__':
    main()
