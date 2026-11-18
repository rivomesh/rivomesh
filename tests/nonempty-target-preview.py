#!/usr/bin/env python3
"""Native read-only adoption comparison from MySQL 8.4 to PostgreSQL 17."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
import uuid


ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--session-fault', action='store_true', help='Kill the original MySQL view session during pinning')
    parser.add_argument('--only-constraints', action='store_true', help='Verify a managed PostgreSQL UNIQUE review at a common cut')
    args = parser.parse_args()
    artifact = args.artifact.resolve()
    output = args.output.resolve()
    output.relative_to(ROOT)
    output.mkdir(parents=True, exist_ok=True)
    project = 'rivomesh-adoption-' + uuid.uuid4().hex[:10]
    password = secrets.token_hex(20)
    with artifact.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    report = {'artifactSha256': digest, 'checks': [], 'status': 'FAIL',
              'profile': 'CONSTRAINT_REVIEW' if args.only_constraints else 'ADOPTION_PREVIEW'}
    with tempfile.TemporaryDirectory(prefix='adoption-preview-', dir=ROOT / 'target') as temporary:
        directory = Path(temporary)
        environment = dict(os.environ, RM_ADOPTION_PASSWORD=password)
        case = {
            'formatVersion': 3, 'pipelineId': 'adoption-review', 'nodeId': 'preview',
            'databases': ['sales'], 'source': {'type': 'MYSQL', 'clusterId': 'preview-source',
                                             'host': 'source', 'username': 'root',
                                             'password': '${RM_ADOPTION_PASSWORD}', 'serverId': 891201},
            'journal': {'mode': 'LOCAL', 'path': 'journal'},
            'targets': [{'id': 'pg', 'product': 'POSTGRESQL', 'metadataSchema': 'adoption_meta',
                         'url': 'jdbc:postgresql://target:5432/preview?connectTimeout=5&socketTimeout=25',
                         'username': 'reader', 'password': '${RM_ADOPTION_PASSWORD}',
                         'mappings': {'sales': 'review'}}],
        }
        (directory / 'task.yml').write_text(json.dumps(case))
        services = {
            'source': {
                'image': 'mysql:8.4',
                'command': ['--server-id=101', '--log-bin=mysql-bin', '--binlog-format=ROW',
                            '--binlog-row-image=FULL', '--gtid-mode=ON', '--enforce-gtid-consistency=ON',
                            '--log-replica-updates=ON', '--binlog-transaction-compression=OFF'],
                'environment': {'MYSQL_ROOT_PASSWORD': '${RM_ADOPTION_PASSWORD}', 'MYSQL_ROOT_HOST': '%'},
                'healthcheck': {'test': ['CMD-SHELL', 'mysql -h127.0.0.1 -uroot '
                                         '-p"$$MYSQL_ROOT_PASSWORD" -e "SELECT 1"'],
                                'interval': '3s', 'timeout': '5s', 'retries': 45},
            },
            'target': {
                'image': 'postgres:17.12',
                'environment': {'POSTGRES_PASSWORD': '${RM_ADOPTION_PASSWORD}', 'POSTGRES_DB': 'preview'},
                'healthcheck': {'test': ['CMD', 'pg_isready', '-U', 'postgres', '-d', 'preview'],
                                'interval': '3s', 'timeout': '5s', 'retries': 45},
            },
            'runner': {
                'image': 'eclipse-temurin:21-jre-jammy', 'entrypoint': ['java', '-Xmx256m', '-jar', '/app.jar'],
                'environment': {'RM_ADOPTION_PASSWORD': '${RM_ADOPTION_PASSWORD}'},
                'volumes': [f'{artifact}:/app.jar:ro', f'{directory}:/fixture'],
            },
        }
        compose_path = directory / 'compose.json'
        compose_path.write_text(json.dumps({'services': services}))

        def compose(*arguments, statement=None, check=True):
            return subprocess.run(['docker', 'compose', '-p', project, '-f', str(compose_path), *arguments],
                                  cwd=ROOT, env=environment, input=statement, check=check, capture_output=True,
                                  text=True, timeout=180)

        def mysql(statement):
            return compose('exec', '-T', 'source', 'bash', '-c',
                           'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -N -B', statement=statement).stdout.strip()

        def postgres(statement):
            return compose('exec', '-T', 'target', 'psql', '-X', '-q', '-At',
                           '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'preview',
                           statement=statement).stdout.strip()

        def preview(expected_status, target='pg'):
            before = postgres("SELECT to_regnamespace('adoption_meta')::text; "
                              "SELECT to_regclass('review._rivomesh_owner_v3')::text; "
                              "SELECT jsonb_agg(to_jsonb(i) ORDER BY id) FROM review.items i")
            answer = compose('run', '--rm', '-T', 'runner', 'preview-adoption', '/fixture/task.yml', target)
            assert 'sensitive-row-value' not in answer.stdout
            result = json.loads(answer.stdout)
            assert result['status'] == expected_status and result['targetId'] == target
            assert not (directory / 'journal').exists()
            assert before == postgres("SELECT to_regnamespace('adoption_meta')::text; "
                                      "SELECT to_regclass('review._rivomesh_owner_v3')::text; "
                                      "SELECT jsonb_agg(to_jsonb(i) ORDER BY id) FROM review.items i")
            return {table['table']: table for table in result['tables']}

        def passed(label):
            report['checks'].append(label)
            print('PASS ' + label, flush=True)

        def wait_for(check, label='arranging the source-session fault', seconds=25):
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline:
                value = check()
                if value:
                    return value
                time.sleep(.05)
            raise AssertionError('Timed out ' + label)

        def source_session_kill():
            reconnect = dict(case, source=dict(case['source'], jdbcOptions='useSSL=false&allowPublicKeyRetrieval=true'
                                               '&serverTimezone=UTC&connectTimeout=5000&socketTimeout=25000&autoReconnect=true'))
            (directory / 'reconnect.yml').write_text(json.dumps(reconnect))
            blocking_session = "application_name='adoption_fixture_blocker'"
            with ThreadPoolExecutor(max_workers=2) as background:
                blocked = background.submit(compose, 'exec', '-T', '-e', 'PGAPPNAME=adoption_fixture_blocker',
                                            'target', 'psql', '-X', '-q', '-U', 'postgres', '-d', 'preview',
                                            '-c', 'BEGIN; LOCK TABLE review.items IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(40)',
                                            check=False)
                try:
                    wait_for(lambda: postgres("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE "
                                               + blocking_session + " AND wait_event='PgSleep')") == 't')
                    running = background.submit(compose, 'run', '--rm', '-T', 'runner', 'preview-adoption',
                                                '/fixture/reconnect.yml', 'pg', check=False)

                    def view_sessions():
                        ids = mysql("SELECT t.PROCESSLIST_ID FROM performance_schema.user_variables_by_thread v "
                                    "JOIN performance_schema.threads t ON t.THREAD_ID=v.THREAD_ID "
                                    "WHERE v.VARIABLE_NAME='rivomesh_consistent_view_session' ORDER BY t.PROCESSLIST_ID")
                        return ids.splitlines() if len(ids.splitlines()) == 2 else None

                    read_session = int(wait_for(view_sessions)[-1])
                    mysql(f"KILL CONNECTION {read_session}")
                    assert postgres("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE " + blocking_session) == 't'
                    refused = running.result(timeout=60)
                    assert refused.returncode != 0 and not refused.stdout.strip()
                    assert ('SOURCE_VALIDATION_SESSION_CHANGED' in refused.stderr or 'SQL_08S01' in refused.stderr
                            or 'SQL_08003' in refused.stderr)
                    assert not (directory / 'journal').exists()
                finally:
                    postgres("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE " + blocking_session)
                    blocked.result(timeout=60)

        def constraint_review():
            target = dict(case['targets'][0], username='postgres', metadataSchema='unique_meta',
                          mappings={'unique_source': 'unique_copy'})
            managed = dict(case, pipelineId='unique-cut', nodeId='cut-worker', databases=['unique_source'],
                           targets=[target], chunkSize=2, leaseMillis=8000, heartbeatMillis=400,
                           journal={'mode': 'EXTERNAL', 'product': 'POSTGRESQL', 'namespace': 'unique_journal',
                                    'url': target['url'], 'username': 'postgres', 'password': target['password']})
            (directory / 'unique.yml').write_text(json.dumps(managed))
            mysql("""
                CREATE DATABASE unique_source;
                CREATE TABLE unique_source.accounts (
                    id BIGINT PRIMARY KEY, tenant BIGINT,
                    email VARCHAR(80) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
                    nickname VARCHAR(40), UNIQUE KEY uq_email(email), UNIQUE KEY uq_scope(tenant,nickname)
                ) ENGINE=InnoDB;
                INSERT INTO unique_source.accounts VALUES
                    (1,1,'Sensitive-Mail@example.com','one'), (2,1,'sensitive-mail@example.com','two'),
                    (3,1,NULL,NULL), (4,1,NULL,NULL);
            """)
            postgres("""
                CREATE SCHEMA unique_copy;
                CREATE COLLATION unique_copy.case_insensitive
                    (provider=icu,locale='und-u-ks-level2',deterministic=false);
                CREATE TABLE unique_copy.accounts (
                    id BIGINT PRIMARY KEY, tenant BIGINT,
                    email VARCHAR(80) COLLATE unique_copy.case_insensitive, nickname VARCHAR(40)
                );
            """)

            def cli(operation, *parameters, check=True):
                return compose('run', '--rm', '-T', 'runner', operation, '/fixture/unique.yml',
                               *parameters, check=check)

            worker = project + '-cut-worker'
            compose('run', '--rm', '-d', '-T', '--name', worker, 'runner', 'run', '/fixture/unique.yml')
            try:
                wait_for(lambda: postgres("SELECT count(*) FROM unique_copy.accounts") == '4',
                         'waiting for the managed snapshot', seconds=100)
                stage = cli('validate-stage', '/fixture/reports', 'pg').stdout
                report_id = next(line.split()[-1] for line in stage.splitlines()
                                 if line.startswith('VALIDATION_SOURCE_SEALED '))
                review_args = ('/fixture/reports', report_id, 'pg')
                denied = cli('review-unique', *review_args, check=False)
                assert denied.returncode != 0 and 'UNIQUE_REVIEW_REQUIRES_COMPLETE_REPORT' in denied.stderr
                passed('Constraint review refuses a sealed source with an incomplete target validation report')
                complete = json.loads(cli('validate-compare', '/fixture/reports', report_id).stdout)
                assert complete['state'] == 'COMPLETE' and complete['code'] == 'VALIDATION_MATCH'
                passed('The case-sensitive source values match all stored rows at the fenced cut')

                compose('stop', 'source')
                before = postgres("SELECT jsonb_agg(to_jsonb(a) ORDER BY id) FROM unique_copy.accounts a; "
                                  "SELECT epoch,stream_seq FROM unique_meta.rm3_sink_job; "
                                  "SELECT count(*) FROM pg_index WHERE indrelid='unique_copy.accounts'::regclass "
                                  "AND indisunique AND NOT indisprimary")
                review = cli('review-unique', *review_args).stdout
                assert 'Sensitive-Mail@example.com' not in review and 'sensitive-mail@example.com' not in review
                actual = json.loads(review)
                assert actual['status'] == 'DUPLICATES_FOUND' and actual['historicalReportMatched']
                assert actual['nullPolicy'] == 'NULLS_DISTINCT' and not actual['exclusionsRecorded']
                checks = {index['sourceIndex']: index for index in actual['indexes']}
                assert checks['uq_email']['state'] == 'DUPLICATES_FOUND'
                assert checks['uq_email']['duplicateGroups'] == 1 and checks['uq_email']['conflictingRows'] == 2
                assert checks['uq_scope']['state'] == 'NO_DUPLICATES_OBSERVED'
                assert checks['uq_scope']['duplicateGroups'] == checks['uq_scope']['conflictingRows'] == 0
                assert before == postgres("SELECT jsonb_agg(to_jsonb(a) ORDER BY id) FROM unique_copy.accounts a; "
                                          "SELECT epoch,stream_seq FROM unique_meta.rm3_sink_job; "
                                          "SELECT count(*) FROM pg_index WHERE indrelid='unique_copy.accounts'::regclass "
                                          "AND indisunique AND NOT indisprimary")
                passed('Native target collation detects a real collision with no row values disclosed, even with the source offline')
                passed('Composite and single-column checks ignore NULL-containing keys and preserve business rows and checkpoints')

                status = json.loads(cli('status').stdout)
                cli('resume-pipeline', str(status['errorGeneration']))
                stale = cli('review-unique', *review_args, check=False)
                assert stale.returncode != 0 and 'VALIDATION_BARRIER_MOVED' in stale.stderr
                passed('Resuming the pipeline invalidates the old constraint review cut')
            except Exception:
                worker_logs = subprocess.run(['docker', 'logs', '--tail', '120', worker], capture_output=True,
                                             text=True, timeout=15, check=False)
                redacted = (worker_logs.stdout + worker_logs.stderr).replace(password, '[REDACTED]')
                (output / 'worker-redacted.log').write_text(redacted)
                raise
            finally:
                subprocess.run(['docker', 'stop', '-t', '5', worker], capture_output=True, timeout=30, check=False)

        try:
            compose('up', '-d', '--wait', '--wait-timeout', '140', 'source', 'target')
            report['mysqlVersion'] = mysql("SELECT VERSION()")
            report['postgresqlVersion'] = postgres("SHOW server_version")
            assert report['mysqlVersion'].startswith('8.4.') and report['postgresqlVersion'].startswith('17.')
            if args.only_constraints:
                constraint_review()
                report['status'] = 'PASS'
                return
            mysql("""
                CREATE DATABASE sales;
                CREATE TABLE sales.items(id BIGINT PRIMARY KEY,name VARCHAR(80),doc JSON) ENGINE=InnoDB;
                INSERT INTO sales.items VALUES (1,'sensitive-row-value','{"b":2,"a":1.0}'),(2,'same',NULL),(3,'gone','null');
            """)
            postgres(f"""
                CREATE ROLE reader LOGIN PASSWORD '{password}';
                CREATE SCHEMA review;
                CREATE TABLE review.items(id BIGINT PRIMARY KEY,name VARCHAR(80),doc JSONB);
                INSERT INTO review.items VALUES (1,'sensitive-row-value','{{"a":1,"b":2.0}}'),(2,'same',NULL),(3,'gone','null');
                GRANT USAGE ON SCHEMA review TO reader;
                GRANT SELECT ON ALL TABLES IN SCHEMA review TO reader;
            """)
            tables = preview('MATCH_REQUIRES_VERIFIED_BASELINE')
            assert tables['items']['matched'] == 3 and tables['items']['sourceRows'] == tables['items']['targetRows'] == 3
            passed('Matching keyed rows and canonical JSON are reported through a SELECT-only account without initialization')

            if args.session_fault:
                source_session_kill()
                assert preview('MATCH_REQUIRES_VERIFIED_BASELINE')['items']['matched'] == 3
                passed('Disconnecting the original MySQL snapshot invalidates the report despite reconnect and preserves a fresh retry')

            postgres("UPDATE review.items SET doc='null' WHERE id=2; DELETE FROM review.items WHERE id=3; "
                     "INSERT INTO review.items VALUES (4,'extra',NULL)")
            item = preview('DIFFERENCES_FOUND')['items']
            assert (item['matched'], item['missing'], item['extra'], item['changed']) == (1, 1, 1, 1)
            passed('Equal counts still identify missing, extra and changed rows and distinguish SQL NULL from JSON null')

            postgres("DELETE FROM review.items")
            item = preview('DIFFERENCES_FOUND')['items']
            assert item['sourceRows'] == item['missing'] == 3 and item['targetRows'] == item['extra'] == 0
            postgres("INSERT INTO review.items VALUES (1,'sensitive-row-value','{\"b\":2,\"a\":1}'),(2,'same','null'),(4,'extra',NULL)")
            passed('An empty compatible target is reported as entirely missing existing source rows')

            mysql("""
                CREATE TABLE sales.keyless(value_col BIGINT) ENGINE=InnoDB;
                CREATE TABLE sales.not_copied(id BIGINT PRIMARY KEY) ENGINE=InnoDB;
                CREATE TABLE sales.wrong_type(id BIGINT PRIMARY KEY,some_value BIGINT) ENGINE=InnoDB;
            """)
            postgres("CREATE TABLE review.wrong_type(id BIGINT PRIMARY KEY,some_value TEXT); "
                     "GRANT SELECT ON review.wrong_type TO reader")
            tables = preview('BLOCKED')
            assert tables['items']['changed'] == 1
            assert tables['keyless']['blocker'] == 'ADOPTION_SOURCE_PRIMARY_KEY_REQUIRED'
            assert tables['not_copied']['blocker'] == 'ADOPTION_TARGET_TABLE_MISSING'
            assert tables['wrong_type']['blocker'] == 'TARGET_COLUMN_TYPE_MISMATCH'
            passed('Keyless, missing and incompatible tables block independently of the other table comparison')

            postgres('ALTER TABLE review.items ENABLE ROW LEVEL SECURITY')
            assert preview('BLOCKED')['items']['blocker'] == 'ADOPTION_TARGET_RELATION_REQUIRES_PLAN'
            postgres('ALTER TABLE review.items DISABLE ROW LEVEL SECURITY; ALTER TABLE review.items ADD UNIQUE(name)')
            assert preview('BLOCKED')['items']['blocker'] == 'ADOPTION_TARGET_CONSTRAINTS_REQUIRE_PLAN'
            postgres('ALTER TABLE review.items DROP CONSTRAINT items_name_key')
            passed('Row security and extra UNIQUE constraints require an explicit plan')

            postgres("CREATE TABLE review._rivomesh_owner_v3(table_name text PRIMARY KEY); "
                     "INSERT INTO review._rivomesh_owner_v3 VALUES ('items'); "
                     "GRANT SELECT ON review._rivomesh_owner_v3 TO reader")
            assert preview('BLOCKED')['items']['blocker'] == 'ADOPTION_TARGET_ALREADY_OWNED'
            postgres("CREATE SCHEMA adoption_meta; CREATE TABLE adoption_meta.rm3_sink_job(pipeline_id text,subscription_id text); "
                     "INSERT INTO adoption_meta.rm3_sink_job VALUES ('adoption-review','pg'); "
                     "GRANT USAGE ON SCHEMA adoption_meta TO reader; GRANT SELECT ON adoption_meta.rm3_sink_job TO reader")
            assert preview('BLOCKED')['items']['blocker'] == 'ADOPTION_TARGET_CHECKPOINT_EXISTS'
            passed('Existing physical ownership or a target checkpoint cannot be mistaken for fresh adoption')

            unknown = compose('run', '--rm', '-T', 'runner', 'preview-adoption', '/fixture/task.yml', 'other', check=False)
            assert unknown.returncode != 0 and 'ADOPTION_TARGET_UNKNOWN' in unknown.stderr
            assert not (directory / 'journal').exists()
            passed('An unknown target is rejected before the CLI can create task state')

            unsupported = dict(case, source=dict(case['source'], host='unused'),
                               targets=[dict(case['targets'][0], product='MYSQL',
                                             url='jdbc:mysql://unused:3306/unopened?connectTimeout=1000')])
            (directory / 'unsupported.yml').write_text(json.dumps(unsupported))
            denied = compose('run', '--rm', '-T', 'runner', 'preview-adoption', '/fixture/unsupported.yml', 'pg', check=False)
            assert denied.returncode != 0 and 'ADOPTION_TARGET_PRODUCT_UNSUPPORTED' in denied.stderr
            assert not (directory / 'journal').exists()
            passed('A non-PostgreSQL configuration is rejected before either database is opened')
            report['status'] = 'PASS'
        except Exception as failure:
            if isinstance(failure, subprocess.CalledProcessError):
                error = failure.stderr.replace(password, '[REDACTED]')
                (output / 'failure.stderr').write_text(error)
                raise AssertionError('Native command failed, see failure.stderr') from None
            raise
        finally:
            cleanup = compose('down', '-v', '--remove-orphans', check=False)
            report['cleanupExitCode'] = cleanup.returncode
            if cleanup.returncode:
                report['status'] = 'FAIL'
            (output / 'manifest.json').write_text(json.dumps(report, indent=2) + '\n')
            if cleanup.returncode:
                raise AssertionError('Test project cleanup failed')


if __name__ == '__main__':
    main()
