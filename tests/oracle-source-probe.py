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
