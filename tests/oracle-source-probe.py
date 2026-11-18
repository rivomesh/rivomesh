#!/usr/bin/env python3
"""Verify Oracle 26ai source identity and SCN flashback with a disposable Free database."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


def execute(*args, stdin=None, env=None, timeout=120, checked=True):
    return subprocess.run(args, input=stdin, env=env, text=True, capture_output=True,
                          timeout=timeout, check=checked)


def required(check, description, timeout=240):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if value := check():
            return value
        time.sleep(2)
    raise AssertionError(description + ' timed out')


def pipeline_profile(options, oracle, sql, root_service, pdb_service, environment, observed):
    target = oracle + '-pg'
    env = dict(environment, POSTGRES_PASSWORD=secrets.token_urlsafe(32), RM_ORACLE_TEST_TOKEN=secrets.token_urlsafe(32))
    if options.date_values:
        env['TZ'] = 'America/New_York'
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    platform = None
    long_transaction = None
    output = options.output
    program = (options.java, '-cp', str(options.artifact.resolve()) + os.pathsep
               + str(options.jdbc_driver.resolve()), 'io.github.rivomesh.Main')

    def target_sql(statement, checked=True):
        return execute('docker','exec',target,'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','sync_test',
                       '-At','-c',statement,checked=checked).stdout.strip()

    def stop_platform(force=False):
        nonlocal platform
        if platform is None:
            return
        if platform.poll() is None:
            os.killpg(platform.pid,signal.SIGKILL if force else signal.SIGTERM)
            try:
                platform.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(platform.pid,signal.SIGKILL)
                platform.wait(timeout=10)
        platform = None

    def start_platform(config, filename):
        nonlocal platform
        with (output/filename).open('w') as logs:
            platform = subprocess.Popen((*program,'serve',str(config)),env=env,stdout=logs,
                                        stderr=subprocess.STDOUT,start_new_session=True)

    def api(path, payload=None):
        headers = {'Authorization':'Bearer '+env['RM_ORACLE_TEST_TOKEN']}
        if payload is not None:
            headers.update({'Content-Type':'application/json','X-Request-ID':'oracle_'+secrets.token_hex(12)})
        request = urllib.request.Request(base+path,headers=headers,
                                         data=json.dumps(payload).encode() if payload is not None else None)
        try:
            with opener.open(request,timeout=10) as response:
                return json.load(response)
        except (OSError,urllib.error.URLError):
            return {}

    def progress():
        return api('/api/tasks/oracle_native').get('status',{})

    def row_note():
        return target_sql('SELECT "NOTE" FROM copy_app."RIVO_ORDERS" WHERE "ID"=1',checked=False)

    def date_rows(table, key=False):
        value = """COALESCE(TO_CHAR("EVENT_AT",'YYYY-MM-DD HH24:MI:SS'),'<NULL>')"""
        column = '"ID"||\'|\'||'+value if key else value
        order = '"ID"' if key else '"EVENT_AT" NULLS LAST'
        return target_sql(f'SELECT {column} FROM copy_app."{table}" ORDER BY {order}',checked=False).splitlines()

    def rotate():
        sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')

    source_tables = ['RIVO_APP.RIVO_ORDERS', 'RIVO_APP.RIVO_KEYLESS']
    if options.text_json:
        sql(r"""ALTER SESSION SET CONTAINER=FREEPDB1;
            CREATE TABLE rivo_app.rivo_json_rows (
                id NUMBER(12) PRIMARY KEY,
                payload VARCHAR2(1000 BYTE) CHECK (payload IS JSON (STRICT WITH UNIQUE KEYS)),
                plain VARCHAR2(100 BYTE),
                CHECK (plain IS JSON (STRICT WITH UNIQUE KEYS)) DISABLE NOVALIDATE);
            CREATE TABLE rivo_app.rivo_json_keyless (
                "payload" VARCHAR2(100 BYTE) CHECK ("payload" IS JSON (STRICT WITH UNIQUE KEYS)));
            CREATE TABLE rivo_app.rivo_native_json (
                id NUMBER(12) PRIMARY KEY, payload JSON);
            CREATE TABLE rivo_app.rivo_json_key (
                payload VARCHAR2(100 BYTE) PRIMARY KEY
                    CHECK (payload IS JSON (STRICT WITH UNIQUE KEYS)));
            INSERT INTO rivo_app.rivo_json_rows VALUES
                (1,'{"name":"O''Brien","number":123456789012345678901,"where":"\u53f0\u5317"}','{value:12}');
            INSERT INTO rivo_app.rivo_json_rows VALUES (2,NULL,NULL);
            INSERT INTO rivo_app.rivo_json_rows VALUES (3,'null',NULL);
            INSERT INTO rivo_app.rivo_json_keyless VALUES ('{"value":7}');
            INSERT INTO rivo_app.rivo_json_keyless VALUES ('{"value":7}');
            COMMIT;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_json_rows TO C##RIVO_PROBE;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_json_keyless TO C##RIVO_PROBE;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_native_json TO C##RIVO_PROBE;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_json_key TO C##RIVO_PROBE;
            ALTER TABLE rivo_app.rivo_json_rows ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
            ALTER TABLE rivo_app.rivo_json_keyless ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;""")
        for invalid in ('{unquoted:1}', '{"same":1,"same":2}'):
            duplicate=sql("ALTER SESSION SET CONTAINER=FREEPDB1;\n"
                          f"INSERT INTO rivo_app.rivo_json_rows VALUES (99,'{invalid}',NULL);",checked=False)
            assert duplicate.returncode and 'ORA-02290' in duplicate.stdout
        observed('The active Oracle text constraint rejects lax syntax and duplicate object keys')
        rotate()
        source_tables += ['RIVO_APP.RIVO_JSON_ROWS', 'RIVO_APP.RIVO_JSON_KEYLESS']
    if options.date_values:
        sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
            CREATE TABLE rivo_app.rivo_date_rows (id NUMBER(12) PRIMARY KEY, event_at DATE);
            CREATE TABLE rivo_app.rivo_date_keyless (event_at DATE);
            INSERT INTO rivo_app.rivo_date_rows VALUES
                (1,TO_DATE('2024-02-29 15:17:19','YYYY-MM-DD HH24:MI:SS'));
            INSERT INTO rivo_app.rivo_date_rows VALUES (2,NULL);
            INSERT INTO rivo_app.rivo_date_keyless VALUES
                (TO_DATE('2024-03-10 02:17:19','YYYY-MM-DD HH24:MI:SS'));
            INSERT INTO rivo_app.rivo_date_keyless VALUES
                (TO_DATE('2024-03-10 03:17:19','YYYY-MM-DD HH24:MI:SS'));
            COMMIT;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_date_rows TO C##RIVO_PROBE;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_date_keyless TO C##RIVO_PROBE;
            ALTER TABLE rivo_app.rivo_date_rows ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
            ALTER TABLE rivo_app.rivo_date_keyless ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;""")
        rotate()
        source_tables += ['RIVO_APP.RIVO_DATE_ROWS','RIVO_APP.RIVO_DATE_KEYLESS']

    started=False
    try:
        execute('docker','run','--rm','-d','--name',target,
                '--label','io.github.rivomesh.test=oracle-source-target','-p','127.0.0.1::5432',
                '-e','POSTGRES_PASSWORD','-e','POSTGRES_DB=sync_test','postgres:17.12',env=env)
        started=True
        required(lambda:target_sql('SELECT 1',checked=False)=='1','PostgreSQL ready',60)
        target_port=execute('docker','port',target,'5432/tcp').stdout.strip().rsplit(':',1)[1]
        sql("""GRANT SELECT ON sys.v_$transaction TO C##RIVO_PROBE;
            GRANT SELECT ON sys.v_$pdb_incarnation TO C##RIVO_PROBE;""")
        with tempfile.TemporaryDirectory(prefix='rivomesh-oracle-pipeline-') as tmp:
            folder=Path(tmp).resolve()
            tasks=folder/'tasks'
            tasks.mkdir()

            def endpoint(url):
                return {'url':url,'username':'C##RIVO_PROBE','password':'${RIVOMESH_ORACLE_PROBE_PASSWORD}'}

            pipeline={
                'formatVersion':3,'pipelineId':'oracle_native','nodeId':'oracle-agent','databases':['RIVO_APP'],
                'source':{'type':'ORACLE','clusterId':'oracle-native','database':'FREEPDB1',
                          'expectedVersion':'23.26','topology':'SINGLE',
                          'oracle':{'root':endpoint(root_service),'pdb':endpoint(pdb_service),
                                    'tables':source_tables,
                                    'archivePollMillis':500}},
                'targets':[{'id':'pg','product':'POSTGRESQL',
                            'url':f'jdbc:postgresql://127.0.0.1:{target_port}/sync_test?connectTimeout=3',
                            'username':'postgres','password':'${POSTGRES_PASSWORD}','mappings':{'RIVO_APP':'copy_app'},
                            'metadataSchema':'copy_meta'}],
                'chunkSize':2,'heartbeatMillis':500,'leaseMillis':5000,'pollIntervalMillis':150,
            }
            task_file=tasks/'oracle_native.yml'
            task_file.write_text(json.dumps(pipeline))
            checked=execute(*program,'check',str(task_file),env=env)
            assert json.loads(checked.stdout)['sourceTables']==len(source_tables)
            if options.text_json:
                invalid_profile=folder/'unsupported.yml'
                source_oracle=pipeline['source']['oracle']
                for table,error in [
                    ('RIVO_NATIVE_JSON','ORACLE_REDO_COLUMN_TYPE_UNSUPPORTED'),
                    ('RIVO_JSON_KEY','ORACLE_TEXT_JSON_PRIMARY_KEY_UNSUPPORTED'),
                ]:
                    source_oracle['tables']=['RIVO_APP.'+table]
                    invalid_profile.write_text(json.dumps(pipeline))
                    denied=execute(*program,'check',str(invalid_profile),env=env,checked=False)
                    assert denied.returncode and error in denied.stderr
                source_oracle['tables']=source_tables
                observed('Preflight refuses native binary JSON and textual JSON source keys')

            # Keep DML started before the snapshot open through the first archive rotation.
            long_transaction=subprocess.Popen(('docker','exec','-i',oracle,'sqlplus','-L','-s','/','as','sysdba'),
                                              stdin=subprocess.PIPE,stdout=subprocess.PIPE,
                                              stderr=subprocess.STDOUT,text=True)
            long_transaction.stdin.write("WHENEVER SQLERROR EXIT FAILURE ROLLBACK\n"
                                         "ALTER SESSION SET CONTAINER=FREEPDB1;\n"
                                         "UPDATE rivo_app.rivo_orders SET note='long transaction' WHERE id=1;\n")
            long_transaction.stdin.flush()
            active_query=("SELECT MIN(start_scn) FROM v$transaction "
                          "WHERE con_id=(SELECT con_id FROM v$pdbs WHERE name='FREEPDB1');")
            before=required(lambda:sql(active_query).stdout.strip() or None,'Oracle transaction active',30)
            assert before.isdecimal()

            with socket.create_server(('127.0.0.1',0)) as reservation:
                bind_port=reservation.getsockname()[1]
            base='http://127.0.0.1:'+str(bind_port)
            config=folder/'platform.json'
            config.write_text(json.dumps({'bind':'127.0.0.1','port':bind_port,'tasksDirectory':str(tasks),
                                          'dataDirectory':str(folder/'state'),'autoStart':True,
                                          'authToken':'${RM_ORACLE_TEST_TOKEN}','pollMillis':250,
                                          'sessions':{'secureCookie':False},'workerHeapMiB':256}))
            try:
                start_platform(config,'worker-platform.log')
                required(lambda:row_note()=='after' and progress().get('capture_scn'),'native Oracle snapshot',90)
                if options.text_json:
                    before_json='SELECT "PAYLOAD"::text FROM copy_app."RIVO_JSON_ROWS" WHERE "ID"=1'
                    snapshot_json=required(lambda:target_sql(before_json,checked=False),'Oracle JSON snapshot',90)
                    assert json.loads(snapshot_json)=={
                        'name': "O'Brien", 'number': 123456789012345678901, 'where': '\u53f0\u5317',
                    }
                    assert target_sql('SELECT pg_typeof("PAYLOAD")::text||\'|\'||pg_typeof("PLAIN")::text||\'|\'||"PLAIN" '
                                      'FROM copy_app."RIVO_JSON_ROWS" WHERE "ID"=1')=='jsonb|character varying|{value:12}'
                    required(lambda:target_sql('SELECT COUNT(*) FROM copy_app."RIVO_JSON_ROWS" '
                                               'WHERE ("ID"=2 AND "PAYLOAD" IS NULL) OR '
                                               '("ID"=3 AND "PAYLOAD"=\'null\'::jsonb)')=='2',
                             'SQL NULL versus JSON null',90)
                    required(lambda:target_sql('SELECT COUNT(*) FROM copy_app."RIVO_JSON_KEYLESS"',checked=False)=='2',
                             'keyless JSON snapshot',90)
                    observed('Flashback materializes strict JSONB, preserves NULL versus JSON null and retains plain VARCHAR2')
                if options.date_values:
                    required(lambda:date_rows('RIVO_DATE_ROWS',key=True)==['1|2024-02-29 15:17:19','2|<NULL>']
                             and date_rows('RIVO_DATE_KEYLESS')==['2024-03-10 02:17:19','2024-03-10 03:17:19'],
                             'Oracle DATE Flashback snapshot',90)
                    assert target_sql('SELECT pg_typeof("EVENT_AT")::text FROM copy_app."RIVO_DATE_ROWS" '
                                      'WHERE "ID"=1')=='timestamp without time zone'
                    observed('Flashback preserves Oracle DATE leap day, NULL, seconds and two distinct local clocks across a JVM DST gap')
                snapshots=target_sql('SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')
                assert len(snapshots.splitlines())==len(source_tables)
                assert target_sql('SELECT COUNT(*) FROM copy_app."RIVO_KEYLESS"')=='1'
                checkpoint=progress()['sourcePosition']['payload']
                assert int(checkpoint['archiveStartScn'])<=int(before)<int(checkpoint['scn'])
                assert len(checkpoint['pdbIncarnation'].split(':'))==2
                observed('Oracle Flashback snapshots keyed and keyless tables while retaining the pre-snapshot transaction floor')

                rotate()
                required(lambda:int(progress().get('capture_scn','0'))>int(checkpoint['scn']),
                         'archive-only progress while the Oracle transaction remains open',45)
                assert row_note()=='after'
                out,_=long_transaction.communicate("COMMIT;\nEXIT\n",timeout=30)
                assert long_transaction.returncode==0,out
                long_transaction=None
                if options.text_json:
                    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                        UPDATE rivo_app.rivo_json_rows SET payload='[true,{"version":2}]' WHERE id=1;
                        DELETE FROM rivo_app.rivo_json_keyless WHERE ROWNUM=1;
                        COMMIT;""")
                if options.date_values:
                    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                        UPDATE rivo_app.rivo_date_rows SET event_at=NULL WHERE id=1;
                        UPDATE rivo_app.rivo_date_rows SET
                            event_at=TO_DATE('2024-03-10 02:17:19','YYYY-MM-DD HH24:MI:SS') WHERE id=2;
                        DELETE FROM rivo_app.rivo_date_keyless
                            WHERE event_at=TO_DATE('2024-03-10 02:17:19','YYYY-MM-DD HH24:MI:SS');
                        COMMIT;""")
                rotate()
                required(lambda:row_note()=='long transaction','long Oracle transaction delivered after commit',60)
                observed('Oracle LogMiner delivers an earlier-started transaction only after its archive contains the commit')
                if options.text_json:
                    required(lambda:json.loads(target_sql(before_json))==[True,{'version':2}]
                             and target_sql('SELECT COUNT(*) FROM copy_app."RIVO_JSON_KEYLESS"')=='1',
                             'Oracle archived JSON update and keyless removal',60)
                    observed('Archived JSON updates and a keyless JSON delete preserve typed values and one occurrence')
                if options.date_values:
                    required(lambda:date_rows('RIVO_DATE_ROWS',key=True)==['1|<NULL>','2|2024-03-10 02:17:19']
                             and date_rows('RIVO_DATE_KEYLESS')==['2024-03-10 03:17:19'],
                             'Oracle DATE archived redo and undo',60)
                    observed('Archived redo and undo preserve Oracle DATE and NULL, and identify the exact keyless row across the DST gap')

                stop_platform(force=True)
                sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                    INSERT INTO rivo_app.rivo_orders VALUES(4,'offline');
                    DELETE FROM rivo_app.rivo_keyless WHERE ROWNUM=1;
                    COMMIT;""")
                if options.text_json:
                    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                        INSERT INTO rivo_app.rivo_json_rows VALUES (4,'{"after":"restart"}',NULL);
                        UPDATE rivo_app.rivo_json_keyless SET "payload"='{"value":8}';
                        COMMIT;""")
                if options.date_values:
                    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                        INSERT INTO rivo_app.rivo_date_rows VALUES(3,DATE '2025-01-01');
                        UPDATE rivo_app.rivo_date_keyless
                            SET event_at=TO_DATE('2024-03-10 02:11:00','YYYY-MM-DD HH24:MI:SS');
                        COMMIT;""")
                rotate()
                required_sql='SELECT "NOTE" FROM copy_app."RIVO_ORDERS" WHERE "ID"=4'
                start_platform(config,'worker-restart.log')
                required(lambda:target_sql(required_sql)=='offline'
                         and target_sql('SELECT COUNT(*) FROM copy_app."RIVO_KEYLESS"')=='0',
                         'Oracle archive catch-up after SIGKILL',75)
                assert snapshots==target_sql('SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')
                observed('Oracle offline archived work replays after Controller and Worker SIGKILL using the original snapshots')
                if options.text_json:
                    required(lambda:target_sql('SELECT "PAYLOAD"->>\'after\' FROM copy_app."RIVO_JSON_ROWS" '
                                               'WHERE "ID"=4')=='restart'
                             and target_sql('SELECT "payload"->>\'value\' FROM copy_app."RIVO_JSON_KEYLESS"')=='8',
                             'Oracle archived JSON restart',60)
                    assert snapshots==target_sql('SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')
                    observed('JSON CDC resumes after SIGKILL with the original Flashback snapshot IDs')
                if options.date_values:
                    required(lambda:date_rows('RIVO_DATE_ROWS',key=True)==
                             ['1|<NULL>','2|2024-03-10 02:17:19','3|2025-01-01 00:00:00']
                             and date_rows('RIVO_DATE_KEYLESS')==['2024-03-10 02:11:00'],
                             'Oracle DATE archived restart',60)
                    assert snapshots==target_sql('SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')
                    observed('Oracle DATE offline insert and keyless update resume after SIGKILL with original snapshot IDs')

                durable=progress()['capture_scn']
                sql('ALTER SESSION SET CONTAINER=FREEPDB1;\nALTER TABLE rivo_app.rivo_orders ADD EXTRA NUMBER(12);')
                rotate()
                result=required(lambda:(p if (p:=progress()).get('error_code')=='ORACLE_REDO_DDL_REQUIRES_PLAN' else None),
                                'Oracle DDL accepted as a typed refusal',60)
                assert result['capture_scn']==durable and result['error_stage']=='DDL'
                observed('Unreviewed Oracle DDL stops at the preserved SCN and exposes the DDL phase')
                if options.date_values:
                    start=sql('SELECT current_scn FROM v$database;').stdout.strip()
                    sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                        INSERT INTO rivo_app.rivo_date_rows VALUES
                            (4,TO_DATE('1900-01-02 03:04:05 BC','YYYY-MM-DD HH24:MI:SS AD','NLS_DATE_LANGUAGE=AMERICAN'));
                        COMMIT;""")
                    end=sql('SELECT current_scn FROM v$database;').stdout.strip()
                    assert start.isdecimal() and end.isdecimal() and int(end)>int(start)
                    rotate()
                    probe=folder/'date-boundary.json'
                    probe.write_text(json.dumps({
                        'sourceId':'oracle-date-boundary','pdbName':'FREEPDB1','root':endpoint(root_service),
                        'pdb':endpoint(pdb_service),'tables':['RIVO_APP.RIVO_DATE_ROWS'],
                        'archiveWindow':{'startScn':start,'endScnExclusive':end,'decodeCommitted':True},
                    }))
                    denied=execute(*program,'probe-oracle',str(probe),env=env,checked=False)
                    assert denied.returncode and 'ORACLE_DATE_VALUE_OUT_OF_RANGE' in denied.stderr, denied.stderr
                    observed('A BCE Oracle DATE is refused in a closed native archive without losing its sign')
            finally:
                stop_platform()
                for path in (folder/'state'/'logs').glob('*.log'):
                    log=path.read_text(errors='replace')
                    for name in ('RIVOMESH_ORACLE_PROBE_PASSWORD','POSTGRES_PASSWORD','RM_ORACLE_TEST_TOKEN'):
                        log=log.replace(env[name],'[REDACTED]')
                    (output/(path.stem+'-redacted.log')).write_text(log)
    finally:
        if long_transaction is not None and long_transaction.poll() is None:
            long_transaction.communicate('ROLLBACK;\nEXIT\n',timeout=30)
        if started:
            execute('docker','stop','--time','30',target)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact', required=True, type=Path)
    parser.add_argument('--jdbc-driver', required=True, type=Path)
    parser.add_argument('--java', default='java')
    parser.add_argument('--image', default='container-registry.oracle.com/database/free@sha256:'
                        'f5a1791bf5628fd1a97e07584ff5ac5537da5021c23e70f5083f48c8fcd6476e')
    parser.add_argument('--logminer', action='store_true', help='Also verify archive redo and committed-only row images')
    parser.add_argument('--pipeline', action='store_true', help='Also verify archive CDC to PostgreSQL after Worker restart')
    parser.add_argument('--text-json', action='store_true', help='Also verify constrained VARCHAR2 to PostgreSQL JSONB')
    parser.add_argument('--date-values', action='store_true', help='Also verify Oracle DATE to PostgreSQL TIMESTAMP')
    parser.add_argument('--output', required=True, type=Path)
    options = parser.parse_args()
    if options.pipeline and not options.logminer:
        parser.error('--pipeline requires --logminer')
    if options.text_json and not options.pipeline:
        parser.error('--text-json requires --pipeline')
    if options.date_values and not options.pipeline:
        parser.error('--date-values requires --pipeline')
    options.output.mkdir(parents=True, exist_ok=True)
    name = 'rivomesh-oracle-source-' + secrets.token_hex(5)
    password = 'Rm9x' + secrets.token_hex(20)
    environment = dict(os.environ, ORACLE_PWD='Rm9s' + secrets.token_hex(20),
                       RIVOMESH_ORACLE_PROBE_PASSWORD=password)
    report = {'artifactSha256': hashlib.sha256(options.artifact.read_bytes()).hexdigest(),
              'driverSha256': hashlib.sha256(options.jdbc_driver.read_bytes()).hexdigest(),
              'image': options.image, 'runtimeVersion': execute(options.java,'-version').stderr.splitlines()[0],
              'textJson': options.text_json, 'dateValues': options.date_values, 'checks': [], 'status': 'FAIL'}

    def sql(statement, checked=True):
        script = ('SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 32767 VERIFY OFF\n'
                  'WHENEVER SQLERROR EXIT FAILURE ROLLBACK\n' + statement + '\nEXIT\n')
        return execute('docker', 'exec', '-i', name, 'sqlplus', '-L', '-s', '/', 'as', 'sysdba',
                       stdin=script, checked=checked)

    def observed(message):
        report['checks'].append(message)
        print('PASS ' + message, flush=True)

    started = False
    try:
        execute('docker', 'run', '-d', '--rm', '--name', name,
                '--label', 'io.github.rivomesh.test=oracle-source', '--cpus=2', '--memory=3g',
                '--shm-size=1g', '-p', '127.0.0.1::1521', '-e', 'ORACLE_PWD',
                '-e', 'ENABLE_ARCHIVELOG=true', '-e', 'ENABLE_FORCE_LOGGING=true',
                options.image, env=environment, timeout=240)
        started = True
        required(lambda: execute('docker', 'inspect', '-f', '{{.State.Health.Status}}', name)
                 .stdout.strip() == 'healthy', 'Oracle database health')
        port = execute('docker', 'port', name, '1521/tcp').stdout.strip().rsplit(':', 1)[1]
        report['sourceVersion'] = sql('SELECT version_full FROM v$instance;').stdout.strip()
        if sql("SELECT log_mode FROM v$database;").stdout.strip() != 'ARCHIVELOG':
            sql('SHUTDOWN IMMEDIATE\nSTARTUP MOUNT\nALTER DATABASE ARCHIVELOG;\n'
                'ALTER DATABASE OPEN;\nALTER PLUGGABLE DATABASE ALL OPEN;')
        if sql('SELECT force_logging FROM v$database;').stdout.strip() != 'YES':
            sql('ALTER DATABASE FORCE LOGGING;')
        supplemental = sql('SELECT supplemental_log_data_min FROM v$database;').stdout.strip()
        if supplemental not in ('YES', 'IMPLICIT'):
            sql('ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;')
        sql(f"""CREATE USER C##RIVO_PROBE IDENTIFIED BY "{password}" CONTAINER=ALL;
            GRANT CREATE SESSION TO C##RIVO_PROBE CONTAINER=ALL;
            GRANT SELECT ON sys.v_$database TO C##RIVO_PROBE CONTAINER=ALL;
            GRANT SELECT ON sys.v_$instance TO C##RIVO_PROBE CONTAINER=ALL;
            GRANT SELECT ON sys.v_$pdbs TO C##RIVO_PROBE CONTAINER=ALL;
            GRANT SELECT ON sys.v_$thread TO C##RIVO_PROBE;
            GRANT SELECT ON sys.v_$archived_log TO C##RIVO_PROBE;
            ALTER USER C##RIVO_PROBE SET CONTAINER_DATA=(CDB$ROOT,FREEPDB1) CONTAINER=CURRENT;
            ALTER SESSION SET CONTAINER=FREEPDB1;
            CREATE USER RIVO_APP NO AUTHENTICATION DEFAULT TABLESPACE USERS QUOTA 20M ON USERS;
            CREATE TABLE rivo_app.rivo_orders(id NUMBER(12) PRIMARY KEY, note VARCHAR2(50));
            CREATE TABLE rivo_app.rivo_keyless(value NUMBER(12));
            INSERT INTO rivo_app.rivo_orders VALUES(1,'before');
            INSERT INTO rivo_app.rivo_keyless VALUES(7);
            INSERT INTO rivo_app.rivo_keyless VALUES(7);
            COMMIT;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_orders TO C##RIVO_PROBE;
            GRANT SELECT, FLASHBACK ON rivo_app.rivo_keyless TO C##RIVO_PROBE;
            """)
        if options.logminer:
            sql("""GRANT LOGMINING TO C##RIVO_PROBE;
                GRANT EXECUTE ON sys.dbms_logmnr TO C##RIVO_PROBE;
                GRANT SELECT ON sys.v_$logmnr_contents TO C##RIVO_PROBE;""")
            sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                ALTER TABLE rivo_app.rivo_orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
                ALTER TABLE rivo_app.rivo_keyless ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;""")
            sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')
        root_service = f'jdbc:oracle:thin:@//127.0.0.1:{port}/FREE'
        pdb_service = f'jdbc:oracle:thin:@//127.0.0.1:{port}/FREEPDB1'
        document = {
            'sourceId': 'oracle-fixture', 'pdbName': 'FREEPDB1',
            'root': {'url': root_service, 'username': 'C##RIVO_PROBE',
                     'password': '${RIVOMESH_ORACLE_PROBE_PASSWORD}'},
            'pdb': {'url': pdb_service, 'username': 'C##RIVO_PROBE',
                    'password': '${RIVOMESH_ORACLE_PROBE_PASSWORD}'},
            'tables': ['RIVO_APP.RIVO_ORDERS', 'RIVO_APP.RIVO_KEYLESS']
        }
        with tempfile.TemporaryDirectory(prefix='rivomesh-oracle-') as directory:
            profile = Path(directory) / 'oracle.json'
            profile.write_text(json.dumps(document))
            program = (options.java, '-cp', str(options.artifact.resolve()) + os.pathsep
                       + str(options.jdbc_driver.resolve()), 'io.github.rivomesh.Main', 'probe-oracle',
                       str(profile))
            result = json.loads(execute(*program, env=environment).stdout)
            anchor = result['snapshotAnchor']
            assert anchor['sourceType'] == 'ORACLE' and anchor['sourceId'] == 'oracle-fixture'
            assert anchor['payload']['scn'].isdecimal() and anchor['payload']['resetlogsScn'].isdecimal()
            assert len(anchor['payload']['pdbGuid']) == 32
            assert report['sourceVersion'] == result['sourceVersion']
            observed('Oracle root and PDB identity produce a typed DBID, incarnation, PDB GUID and SCN anchor')
            assert result['tables'] == [
                {'table': 'RIVO_APP.RIVO_ORDERS', 'primaryKey': True, 'containsRows': True},
                {'table': 'RIVO_APP.RIVO_KEYLESS', 'primaryKey': False, 'containsRows': True}]
            observed('The restricted read role can flash back keyed and keyless source tables at the same SCN')

            scn = anchor['payload']['scn']
            values = sql(f"""ALTER SESSION SET CONTAINER=FREEPDB1;
                UPDATE rivo_app.rivo_orders SET note='after' WHERE id=1;
                DELETE FROM rivo_app.rivo_keyless WHERE ROWNUM=1;
                COMMIT;
                SELECT note FROM rivo_app.rivo_orders AS OF SCN {scn} WHERE id=1;
                SELECT COUNT(*) FROM rivo_app.rivo_keyless AS OF SCN {scn};
                SELECT note FROM rivo_app.rivo_orders WHERE id=1;
                SELECT COUNT(*) FROM rivo_app.rivo_keyless;
                """).stdout.split()
            assert values == ['before', '2', 'after', '1'], values
            observed('Historical SCN keeps the original row and duplicate multiplicity after source changes')

            document['pdb']['url'] = root_service
            profile.write_text(json.dumps(document))
            denied = execute(*program, env=environment, checked=False)
            assert denied.returncode != 0 and 'ORACLE_PDB_AUTHORITY_MISMATCH' in denied.stderr
            observed('A root connection substituted for the PDB is refused before flashback')
            if options.logminer:
                sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                    INSERT INTO rivo_app.rivo_orders VALUES(2,'rolled back');
                    ROLLBACK;""")
                end = sql('SELECT current_scn FROM v$database;').stdout.strip()
                assert end.isdecimal() and int(end) > int(scn)
                sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')
                mined = sql(f"""
                    DECLARE
                        added NUMBER := 0;
                    BEGIN
                        FOR file_row IN (
                            SELECT DISTINCT name FROM v$archived_log
                            WHERE resetlogs_change#=(SELECT resetlogs_change# FROM v$database)
                              AND name IS NOT NULL AND status='A' AND deleted='NO'
                              AND first_change# <= {end} AND next_change# > {scn}
                        ) LOOP
                            dbms_logmnr.add_logfile(
                                logfilename=>file_row.name,
                                options=>CASE WHEN added=0 THEN dbms_logmnr.new ELSE dbms_logmnr.addfile END);
                            added := added + 1;
                        END LOOP;
                        dbms_logmnr.start_logmnr(
                            startscn=>{scn}, endscn=>{end},
                            options=>dbms_logmnr.dict_from_online_catalog + dbms_logmnr.committed_data_only);
                    END;
                    /
                    SELECT operation||'|'||commit_scn||'|'||RAWTOHEX(xid)||'|'||sql_undo||'|'||sql_redo
                    FROM v$logmnr_contents
                    WHERE seg_owner='RIVO_APP' AND table_name='RIVO_ORDERS' AND src_con_name='FREEPDB1'
                    AND operation IN ('INSERT','UPDATE','DELETE') AND status=0;
                    SELECT operation||'|'||commit_scn||'|'||RAWTOHEX(xid)||'|'||sql_undo||'|'||sql_redo
                    FROM v$logmnr_contents
                    WHERE seg_owner='RIVO_APP' AND table_name='RIVO_KEYLESS' AND src_con_name='FREEPDB1'
                    AND operation IN ('INSERT','UPDATE','DELETE') AND status=0;
                    EXEC dbms_logmnr.end_logmnr
                """).stdout.splitlines()
                changes = [line.strip().split('|') for line in mined if '|' in line]
                updates = [row for row in changes if row[0]=='UPDATE']
                deletes = [row for row in changes if row[0]=='DELETE']
                assert len(changes)==2 and len(updates)==len(deletes)==1, changes
                assert updates[0][1:3]==deletes[0][1:3], changes
                assert "'before'" in updates[0][3] and "'after'" in updates[0][4], changes
                assert '"VALUE"' in deletes[0][3].upper() and "'7'" in deletes[0][3], changes
                assert int(updates[0][1]) > int(scn), changes
                observed('LogMiner reads archived before/after and keyless undo with one XID and excludes the rolled-back insert')
                (options.output / 'native-synthetic-redo.json').write_text(json.dumps(changes, indent=2) + '\n')
                document['pdb']['url'] = pdb_service
                document['archiveWindow'] = {'startScn': scn, 'endScnExclusive': end}
                profile.write_text(json.dumps(document))
                coverage = json.loads(execute(*program, env=environment).stdout)['archiveWindow']
                assert coverage['kind']=='CATALOG_CONTIGUOUS'
                assert coverage['startScn']==scn and coverage['endScnExclusive']==end and coverage['segments']>0
                observed('One Oracle redo thread has continuous catalog coverage for the retained SCN window')
                document['archiveWindow']['decodeCommitted'] = True
                profile.write_text(json.dumps(document))
                typed = json.loads(execute(*program, env=environment).stdout)['archiveWindow']['decoded']
                assert typed['committedTransactions']==1 and typed['operations']=={'UPDATE':1,'DELETE':1}
                assert len(typed['contentSha256'])==64
                last=typed['lastCommit']
                assert last['sourceId']==anchor['sourceId'] and last['sourceType']=='ORACLE'
                for field in ('databaseId','resetlogsScn','pdbGuid','pdbUid'):
                    assert last['payload'][field]==anchor['payload'][field]
                assert last['payload']['scn']==updates[0][1] and last['payload']['xid']==updates[0][2]
                replayed = json.loads(execute(*program, env=environment).stdout)['archiveWindow']['decoded']
                assert replayed==typed
                observed('The Java probe decodes one committed Oracle transaction and repeats the same typed content hash')
                sql("""ALTER SESSION SET CONTAINER=FREEPDB1;
                    INSERT INTO rivo_app.rivo_orders VALUES(3,'outside archives');
                    COMMIT;""")
                live_end = sql('SELECT current_scn FROM v$database;').stdout.strip()
                assert live_end.isdecimal() and int(live_end)>int(end)
                document['archiveWindow']['endScnExclusive'] = live_end
                profile.write_text(json.dumps(document))
                missing = execute(*program, env=environment, checked=False)
                assert missing.returncode and 'ORACLE_ARCHIVE_LOG_GAP' in missing.stderr
                observed('An unarchived part of the requested SCN window is refused')
                sql('ALTER SYSTEM ARCHIVE LOG CURRENT;')
                extended = json.loads(execute(*program, env=environment).stdout)['archiveWindow']['decoded']
                assert extended['committedTransactions']==2
                assert extended['operations']=={'UPDATE':1,'DELETE':1,'INSERT':1}
                assert int(extended['lastCommit']['payload']['scn']) > int(last['payload']['scn'])
                assert extended['lastCommit']['payload']['xid']!=last['payload']['xid']
                assert extended['contentSha256']!=typed['contentSha256']
                observed('After archive rotation, a second complete commit has a distinct typed SCN and XID')
        if options.pipeline:
            pipeline_profile(options,name,sql,root_service,pdb_service,environment,observed)
        report['status'] = 'PASS'
    except Exception as failure:
        detail = str(failure)
        if isinstance(failure, subprocess.CalledProcessError):
            detail += '\n' + (failure.stdout or '') + '\n' + (failure.stderr or '')
        for secret in (password, environment['ORACLE_PWD']):
            detail = detail.replace(secret, '[REDACTED]')
        (options.output / 'failure.log').write_text(detail + '\n')
        raise
    finally:
        if started:
            cleanup = execute('docker', 'stop', '--time', '45', name, checked=False)
            report['cleanupExitCode'] = cleanup.returncode
        (options.output / 'manifest.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
