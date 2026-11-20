#!/usr/bin/env python3
"""Compare the v3 Gauss index review with the catalog of a disposable openGauss server."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import socket
import subprocess
import tempfile
import time
import zipfile


def execute(*command, env=None, cwd=None, statement=None, checked=True, timeout=120):
    return subprocess.run(command, env=env, cwd=cwd, input=statement, text=True,
                          capture_output=True, check=checked, timeout=timeout)


def required(check, label, timeout=100):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(1)
    raise AssertionError(label + ' timed out')


def loopback_ports():
    with socket.socket() as source, socket.socket() as target:
        source.bind(('127.0.0.1',0))
        target.bind(('127.0.0.1',0))
        return source.getsockname()[1], target.getsockname()[1]


def compose_services(options, source_port, target_port):
    return {
        'source': {
            'image': options.mysql_image, 'pull_policy': 'never', 'mem_limit': '1600m',
            'ports': [f'127.0.0.1:{source_port}:3306'],
            'environment': {'MYSQL_ROOT_PASSWORD': '${RM_INDEX_SOURCE_PASSWORD}', 'MYSQL_ROOT_HOST': '%'},
            'command': ['--server-id=101', '--log-bin=mysql-bin', '--binlog-format=ROW',
                        '--binlog-row-image=FULL', '--gtid-mode=ON', '--enforce-gtid-consistency=ON',
                        '--log-replica-updates=ON', '--binlog-transaction-compression=OFF'],
            'healthcheck': {
                'test': ['CMD-SHELL', 'MYSQL_PWD="$$MYSQL_ROOT_PASSWORD" mysql -h127.0.0.1 '
                                      '-uroot -e "SELECT 1" >/dev/null 2>&1'],
                'interval': '3s', 'timeout': '5s', 'retries': 65,
            },
        },
        'target': {
            'image': options.gauss_image, 'pull_policy': 'never', 'mem_limit': '2600m', 'shm_size': '512m',
            'ports': [f'127.0.0.1:{target_port}:5432'],
            'environment': {'GS_PASSWORD': '${RM_INDEX_TARGET_PASSWORD}', 'GS_DB': 'sync_test',
                            'GS_USERNAME': 'sync_test', 'GS_HOST_AUTH_METHOD': 'sha256',
                            'OTHER_PG_CONF': 'password_encryption_type = 2'},
            'healthcheck': {
                'test': ['CMD-SHELL', 'test "$$(cat /proc/1/comm)" = gaussdb && '
                        'su - omm -c \'gsql -d sync_test -c "SELECT 1"\' >/dev/null 2>&1'],
                'interval': '3s', 'timeout': '6s', 'retries': 65,
            },
        },
    }


def pipeline_configuration(source_port, target_port, database, schema):
    return {
        'formatVersion': 3, 'pipelineId': f'gauss-index-{database}', 'nodeId': 'native-worker',
        'databases': [database],
        'source': {'type': 'MYSQL', 'clusterId': 'native-index-source', 'host': '127.0.0.1',
                   'port': source_port, 'username': 'root', 'password': '${RM_INDEX_SOURCE_PASSWORD}',
                   'serverId': 652113 if database == 'sales' else 652114},
        'journal': {'mode': 'LOCAL', 'path': f'{database}-journal'},
        'targets': [{'id': 'gauss', 'product': 'GAUSS', 'uniquePolicy': 'DOWNGRADE_BUSINESS',
                     'url': f'jdbc:postgresql://127.0.0.1:{target_port}/sync_test'
                            '?prepareThreshold=0&connectTimeout=5&socketTimeout=15',
                     'username': 'sync_test', 'password': '${RM_INDEX_TARGET_PASSWORD}',
                     'metadataSchema': f'{schema}_meta', 'mappings': {database: schema}}],
        'chunkSize': 2, 'leaseMillis': 9000, 'heartbeatMillis': 1500,
        'pollIntervalMillis': 150, 'retryDelayMillis': 1000,
    }


def catalog_rows(gauss):
    result = gauss("""
        SELECT ns.nspname, tbl.relname, idx.relname, idx.oid, am.amname,
            CASE WHEN i.indisprimary THEN 'Y' ELSE 'N' END,
            CASE WHEN i.indisunique THEN 'Y' ELSE 'N' END,
            CASE WHEN i.indisvalid AND i.indisready THEN 'Y' ELSE 'N' END,
            pg_get_indexdef(i.indexrelid,1,true),
            CASE WHEN i.indnatts>=2 THEN pg_get_indexdef(i.indexrelid,2,true) ELSE '-' END,
            CASE WHEN i.indnatts>=3 THEN pg_get_indexdef(i.indexrelid,3,true) ELSE '-' END
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class tbl ON tbl.oid=i.indrelid
        JOIN pg_catalog.pg_namespace ns ON ns.oid=tbl.relnamespace
        JOIN pg_catalog.pg_class idx ON idx.oid=i.indexrelid
        JOIN pg_catalog.pg_am am ON am.oid=idx.relam
        WHERE (ns.nspname='replica' AND tbl.relname='customers')
           OR (ns.nspname='replica_meta' AND tbl.relname IN ('rm3_sink_job','rm3_sink_table'))
        ORDER BY ns.nspname,tbl.relname,idx.relname;
    """)
    indexes = []
    for line in result.stdout.splitlines():
        schema, table, name, oid, method, primary, unique, ready, *columns = line.split('|')
        indexes.append({'schema': schema, 'table': table, 'name': name, 'oid': int(oid),
                        'method': method, 'primary': primary == 'Y', 'unique': unique == 'Y',
                        'ready': ready == 'Y',
                        'columns': [column.strip('"') for column in columns if column != '-']})
    return indexes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact', type=Path, required=True)
    parser.add_argument('--java', default='java')
    parser.add_argument('--mysql-image', default='mysql:8.4')
    parser.add_argument('--gauss-image', required=True, help='Locally available openGauss image')
    parser.add_argument('--gauss-version', default='7.0.0-RC3', help='Required server version substring')
    parser.add_argument('--output', type=Path, required=True)
    options = parser.parse_args()
    artifact, output = options.artifact.resolve(), options.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    environment = dict(os.environ, RM_INDEX_SOURCE_PASSWORD='Ri9!' + secrets.token_hex(20),
                       RM_INDEX_TARGET_PASSWORD='Ri8!' + secrets.token_hex(12))
    project = 'rivomesh-opengauss-index-' + secrets.token_hex(5)
    report = {'status': 'FAIL', 'project': project, 'checks': [], 'images': {},
              'artifactSha256': hashlib.sha256(artifact.read_bytes()).hexdigest(),
              'scriptSha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'runtimeVersion': execute(options.java, '-version').stderr.splitlines()[0]}
    driver = 'io.github.rivomesh.gaussdb.jdbc.Driver'
    with zipfile.ZipFile(artifact) as package:
        pom = package.read('META-INF/maven/org.opengauss/opengauss-jdbc/pom.properties').decode()
        report['gaussDriver'] = {'class':driver,
                                 'version':next(line[8:] for line in pom.splitlines() if line.startswith('version=')),
                                 'classSha256':hashlib.sha256(package.read(driver.replace('.','/')+'.class')).hexdigest()}

    def observed(message):
        report['checks'].append(message)
        print('PASS ' + message, flush=True)

    def evidence(name, data):
        (output / (name + '.json')).write_text(json.dumps(data, indent=2) + '\n')

    with tempfile.TemporaryDirectory(prefix='rivomesh-opengauss-index-') as directory:
        directory = Path(directory)
        profile = directory / 'compose.json'
        source_port, target_port = loopback_ports()
        profile.write_text(json.dumps({'services': compose_services(options,source_port,target_port)}))
        compose_program = ('docker', 'compose', '--env-file', os.devnull,
                           '-p', project, '-f', str(profile))
        program = (options.java, '-Xmx384m', '-jar', str(artifact))
        worker = None
        compose_created = False

        def compose(*args, **kwargs):
            return execute(*compose_program, *args, cwd=directory, env=environment, **kwargs)

        def mysql(statement, checked=True):
            return compose('exec', '-T', 'source', 'sh', '-ec',
                           'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -h127.0.0.1 -uroot -N -B',
                           statement=statement, checked=checked)

        def gauss(statement, checked=True):
            return compose('exec', '-T', 'target', 'su', '-', 'omm', '-c',
                           'gsql -X -q -A -t -v ON_ERROR_STOP=1 -d sync_test',
                           statement=statement, checked=checked)

        def native_rows():
            result = gauss("""SELECT tenant_id, customer_id, coalesce(email,'<null>'),tier,region
                              FROM replica.customers ORDER BY tenant_id,customer_id;""", checked=False)
            return result.stdout.strip().splitlines() if result.returncode == 0 else None

        def snapshot():
            result = gauss("""SELECT snapshot_id,snapshot_state,next_part,row_count,last_part_hash
                              FROM replica_meta.rm3_sink_table;""", checked=False)
            rows = result.stdout.strip().split('|')
            return rows if result.returncode == 0 and len(rows) == 5 and rows[1] == 'COMPLETE' else None

        def receipt():
            line = gauss("""SELECT epoch,stream_seq,transaction_id,transaction_hash
                             FROM replica_meta.rm3_sink_job;""").stdout.strip()
            epoch, sequence, tx, digest = line.split('|')
            return {'epoch': int(epoch), 'sequence': int(sequence), 'transaction': tx, 'digest': digest}

        def start_worker(config, log_name):
            with (output / log_name).open('w') as stream:
                return subprocess.Popen((*program, 'run', str(config)), env=environment, cwd=directory,
                                        stdout=stream, stderr=subprocess.STDOUT)

        def stop_worker(*, abrupt=False):
            if worker is None or worker.poll() is not None:
                return
            if abrupt:
                worker.kill()
            else:
                worker.terminate()
            try:
                worker.wait(timeout=28)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait(timeout=6)

        try:
            for name in (options.mysql_image, options.gauss_image):
                image, arch = execute('docker', 'image', 'inspect', '-f',
                                      '{{.Id}}|{{.Architecture}}', name).stdout.strip().split('|')
                report['images'][name] = {'id': image, 'architecture': arch}
            compose_created = True
            compose('up', '-d', '--wait', '--wait-timeout', '200', timeout=225)
            assert compose('port','source','3306').stdout.strip() == f'127.0.0.1:{source_port}'
            assert compose('port','target','5432').stdout.strip() == f'127.0.0.1:{target_port}'
            native_container = compose('ps', '-q', 'target').stdout.strip()
            report['targetContainer'] = native_container
            report['versions'] = {'source': mysql('SELECT VERSION();').stdout.strip(),
                                  'target': gauss('SELECT version();').stdout.strip(),
                                  'targetMode': gauss('SHOW sql_compatibility;').stdout.strip()}
            assert report['versions']['source'].startswith('8.4.')
            assert options.gauss_version in report['versions']['target'], report['versions']
            observed('Exact openGauss version and SQL compatibility mode recorded')

            mysql("""
                CREATE DATABASE sales;
                CREATE TABLE sales.customers (
                    tenant_id BIGINT NOT NULL, customer_id BIGINT NOT NULL, email VARCHAR(64),
                    tier INT NOT NULL, region INT NOT NULL,
                    PRIMARY KEY (tenant_id,customer_id), UNIQUE KEY uk_email (email),
                    KEY ix_rank (tier,region));
                INSERT INTO sales.customers VALUES (1,10,'first@example.test',1,2),
                    (2,10,NULL,3,4),(2,11,NULL,3,5);
                CREATE DATABASE prefix_scope;
                CREATE TABLE prefix_scope.prefix_customer
                    (id BIGINT NOT NULL PRIMARY KEY,email VARCHAR(64), KEY ix_prefix (email(12)));
                INSERT INTO prefix_scope.prefix_customer VALUES (12,'prefix@example.test');
            """)
            source_duplicate=mysql("""INSERT INTO sales.customers
                VALUES (9,10,'first@example.test',5,6);""", checked=False)
            assert source_duplicate.returncode != 0 and 'ERROR 1062' in source_duplicate.stderr
            observed('Source MySQL continues enforcing UNIQUE(email) while accepting multiple SQL NULLs')
            config = directory / 'sales.yml'
            config.write_text(json.dumps(pipeline_configuration(source_port,target_port,'sales','replica')))
            blocked_config = directory / 'blocked.yml'
            blocked_config.write_text(json.dumps(pipeline_configuration(
                source_port,target_port,'prefix_scope','blocked_replica')))

            def check(file):
                return json.loads(execute(*program, 'check', str(file), env=environment,
                                          cwd=directory).stdout)['gaussIndexReview']

            review = check(config)
            evidence('index-check', review)
            assert review['state'] == 'REVIEWED' and review['tableMappings'] == 1
            assert review['indexCounts'] == {'supported':1,'downgradedUnique':1,'replacedGsi':0,
                                             'requiresPlan':0,'distributionPlanRequired':0}
            supported_indexes = {index['sourceIndex']: index for index in review['entries']}
            assert set(supported_indexes) == {'uk_email','ix_rank'}
            assert supported_indexes['uk_email']['action'] == 'DOWNGRADE_UNIQUE'
            assert supported_indexes['ix_rank']['action'] == 'CREATE'
            assert supported_indexes['uk_email']['columns'] == ['email']
            assert supported_indexes['ix_rank']['columns'] == ['tier','region']
            assert not gauss("SELECT nspname FROM pg_namespace WHERE nspname='replica';").stdout.strip()
            observed('Read-only v3 check plans a nonunique email index and ordered two-column secondary index')

            worker = start_worker(config, 'worker-initial.log')
            original_snapshot = required(snapshot, 'First native snapshot')
            assert original_snapshot[2:4] == ['2','3'] and re.fullmatch(r'[0-9a-f]{64}',original_snapshot[4])
            assert native_rows() == ['1|10|first@example.test|1|2','2|10|<null>|3|4','2|11|<null>|3|5']
            indexes = catalog_rows(gauss)
            evidence('native-indexes-initial', indexes)
            assert len(indexes) == 5 and all(index['ready'] and index['method'] == 'btree' for index in indexes)
            business = {index['name']:index for index in indexes if index['table'] == 'customers'}
            assert len(business) == 3
            for plan in supported_indexes.values():
                index = business[plan['targetIndex']]
                assert index['columns'] == plan['columns']
                assert not index['primary'] and not index['unique'] and plan['targetUnique'] is False
            keys = {index['table']:index['columns'] for index in indexes if index['primary'] and index['unique']}
            assert keys == {'customers':['tenant_id','customer_id'],
                            'rm3_sink_job':['pipeline_id','subscription_id'],
                            'rm3_sink_table':['pipeline_id','subscription_id','table_id']}
            observed('Catalog matches the reviewed nonunique indexes and preserves business and receipt primary keys')

            mysql("""START TRANSACTION;
                     UPDATE sales.customers SET email='second@example.test' WHERE tenant_id=1 AND customer_id=10;
                     UPDATE sales.customers SET tenant_id=3 WHERE tenant_id=2 AND customer_id=10;
                     COMMIT;""")
            first_rows = ['1|10|second@example.test|1|2','2|11|<null>|3|5','3|10|<null>|3|4']
            required(lambda: native_rows() == first_rows, 'Composite key CDC')
            first = receipt()
            assert first['sequence'] > 0 and first['transaction'] and re.fullmatch(r'[0-9a-f]{64}',first['digest'])
            observed('One source transaction updates and moves only the intended composite keys')

            compose('stop', '-t', '25', 'target')
            stop_worker(abrupt=True)
            mysql("""START TRANSACTION;
                     DELETE FROM sales.customers WHERE tenant_id=2 AND customer_id=11;
                     INSERT INTO sales.customers VALUES (2,10,'third@example.test',9,8);
                     COMMIT;""")
            compose('up', '-d', '--no-recreate', '--wait', '--wait-timeout', '150', 'target', timeout=175)
            assert compose('ps','-q','target').stdout.strip() == native_container
            assert compose('port','target','5432').stdout.strip() == f'127.0.0.1:{target_port}'
            assert receipt() == first and snapshot() == original_snapshot and native_rows() == first_rows
            assert catalog_rows(gauss) == indexes
            observed('Original openGauss data keeps the native receipt, completed snapshot and physical index OIDs')

            worker = start_worker(config, 'worker-reopen.log')
            final_rows = ['1|10|second@example.test|1|2','2|10|third@example.test|9|8','3|10|<null>|3|4']
            required(lambda: native_rows() == final_rows, 'Receipt continuation after restart')
            second = receipt()
            assert second['sequence'] == first['sequence'] + 1 and second['epoch'] > first['epoch']
            assert second['transaction'] != first['transaction'] and second['digest'] != first['digest']
            assert snapshot() == original_snapshot and catalog_rows(gauss) == indexes
            evidence('native-resume', {'snapshot':original_snapshot,'first':first,'second':second,'rows':final_rows})
            stop_worker()
            observed('Worker reopens the original journal and advances exactly one native transaction without creating indexes')

            blocked_review = check(blocked_config)
            evidence('blocked-index-check', blocked_review)
            assert blocked_review['state'] == 'REQUIRES_PLAN' and len(blocked_review['entries']) == 1
            entry = blocked_review['entries'][0]
            assert entry['sourceIndex'] == 'ix_prefix' and entry['action'] == 'REQUIRES_PLAN'
            assert entry['reason'] == 'EXPRESSION_PREFIX_ORDER_OR_METHOD_UNSUPPORTED'
            blocked_log = output / 'worker-blocked.log'
            worker = start_worker(blocked_config, blocked_log.name)
            required(lambda: 'TARGET_PAUSED gauss INDEX_CONVERSION_REQUIRES_PLAN' in blocked_log.read_text(),
                     'Prefix index worker refusal')
            assert not gauss("SELECT nspname FROM pg_namespace WHERE nspname='blocked_replica';").stdout.strip()
            observed('Unsupported MySQL prefix index is reported and the writer refuses business DDL')
            stop_worker()

            mysql("""
                CREATE DATABASE keyless_source;
                CREATE TABLE keyless_source.events (value INT, payload JSON);
                INSERT INTO keyless_source.events VALUES
                    (7,JSON_OBJECT('tag','original')),(7,JSON_OBJECT('tag','original')),
                    (8,JSON_OBJECT('tag','survivor'));
            """)
            keyless_config = directory / 'keyless.yml'
            keyless_config.write_text(json.dumps(pipeline_configuration(
                source_port, target_port, 'keyless_source', 'keyless_copy')))
            worker = start_worker(keyless_config, 'worker-keyless.log')

            def keyless_rows():
                result=gauss("""SELECT value,payload->>'tag',COUNT(*) FROM keyless_copy.events
                                GROUP BY value,payload->>'tag' ORDER BY value;""",checked=False)
                return result.stdout.strip().splitlines() if result.returncode == 0 else None

            initial=['7|original|2','8|survivor|1']
            required(lambda:keyless_rows()==initial,'Initial duplicate JSON rows in openGauss')
            keyless_snapshot=gauss('SELECT snapshot_id FROM keyless_copy_meta.rm3_sink_table').stdout.strip()
            assert keyless_snapshot
            assert gauss("""SELECT COUNT(*) FROM pg_catalog.pg_index
                            WHERE indrelid='keyless_copy.events'::regclass AND indisprimary AND indisunique""").stdout.strip()=='1'
            observed('openGauss retains two identical source rows with a separate stable row identity')

            mysql("""UPDATE keyless_source.events SET value=9,payload=JSON_OBJECT('tag','changed')
                     WHERE value=7 LIMIT 1;""")
            required(lambda:keyless_rows()==['7|original|1','8|survivor|1','9|changed|1'],
                     'openGauss updates exactly one keyless occurrence')
            before_restart=gauss('SELECT stream_seq FROM keyless_copy_meta.rm3_sink_job').stdout.strip()
            stop_worker(abrupt=True)
            mysql('DELETE FROM keyless_source.events WHERE value=7 LIMIT 1;')
            worker=start_worker(keyless_config,'worker-keyless-reopen.log')
            required(lambda:keyless_rows()==['8|survivor|1','9|changed|1'],
                     'Original keyless journal resumes the pending delete')
            assert gauss('SELECT snapshot_id FROM keyless_copy_meta.rm3_sink_table').stdout.strip()==keyless_snapshot
            after_restart=gauss('SELECT stream_seq FROM keyless_copy_meta.rm3_sink_job').stdout.strip()
            assert int(after_restart)==int(before_restart)+1
            observed('openGauss resumes the original keyless JSON snapshot and applies one retained delete')
            report['status'] = 'PASS'
        except Exception as failure:
            detail = str(failure)
            if isinstance(failure, subprocess.CalledProcessError):
                detail += '\n' + (failure.stdout or '') + '\n' + (failure.stderr or '')
            if compose_created:
                try:
                    logs=compose('logs','--no-color','--tail','40',checked=False,timeout=20)
                    detail+='\n'+logs.stdout
                except (OSError,subprocess.TimeoutExpired):
                    pass
            for key in ('RM_INDEX_SOURCE_PASSWORD','RM_INDEX_TARGET_PASSWORD'):
                detail = detail.replace(environment[key],'[REDACTED]')
            (output / 'failure.log').write_text(detail + '\n')
            raise
        finally:
            try:
                stop_worker()
            finally:
                if compose_created:
                    cleanup = compose('down','--volumes','--remove-orphans',checked=False,timeout=100)
                    report['cleanupExitCode'] = cleanup.returncode
                    if cleanup.returncode:
                        report['status'] = 'FAIL'
                evidence('manifest', report)
            if report.get('cleanupExitCode'):
                raise RuntimeError('Could not clean the openGauss fixture')


if __name__ == '__main__':
    main()
