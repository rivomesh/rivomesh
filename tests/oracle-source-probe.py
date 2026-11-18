#!/usr/bin/env python3
"""Verify Oracle 26ai source identity and SCN flashback with a disposable Free database."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time


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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact', required=True, type=Path)
    parser.add_argument('--jdbc-driver', required=True, type=Path)
    parser.add_argument('--java', default='java')
    parser.add_argument('--image', default='container-registry.oracle.com/database/free@sha256:'
                        'f5a1791bf5628fd1a97e07584ff5ac5537da5021c23e70f5083f48c8fcd6476e')
    parser.add_argument('--logminer', action='store_true', help='Also verify archive redo and committed-only row images')
    parser.add_argument('--output', required=True, type=Path)
    options = parser.parse_args()
    options.output.mkdir(parents=True, exist_ok=True)
    name = 'rivomesh-oracle-source-' + secrets.token_hex(5)
    password = 'Rm9x' + secrets.token_hex(20)
    environment = dict(os.environ, ORACLE_PWD='Rm9s' + secrets.token_hex(20),
                       RIVOMESH_ORACLE_PROBE_PASSWORD=password)
    report = {'artifactSha256': hashlib.sha256(options.artifact.read_bytes()).hexdigest(),
              'driverSha256': hashlib.sha256(options.jdbc_driver.read_bytes()).hexdigest(),
              'image': options.image, 'checks': [], 'status': 'FAIL'}

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
