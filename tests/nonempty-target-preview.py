#!/usr/bin/env python3
"""Native read-only adoption comparison from MySQL 8.4 to PostgreSQL 17."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import select
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
    profile = parser.add_mutually_exclusive_group()
    profile.add_argument('--only-adoption-plan', action='store_true', help='Verify the frozen existing-target admission plan')
    profile.add_argument('--only-adoption-execution', action='store_true', help='Verify atomic adoption and first native CDC')
    profile.add_argument('--only-writer-readiness', action='store_true', help='Verify a durable business writer readiness receipt')
    profile.add_argument('--only-constraints', action='store_true', help='Verify a managed PostgreSQL UNIQUE review at a common cut')
    profile.add_argument('--only-restoration-plan', action='store_true', help='Verify read-only UNIQUE restoration planning at a frozen cut')
    profile.add_argument('--only-restore-execution', action='store_true', help='Verify approved PostgreSQL DDL and native receipt recovery')
    parser.add_argument('--verify-unknown-commit', action='store_true',
                        help='Also interrupt the fixture PostgreSQL commit response after local WAL flush')
    args = parser.parse_args()
    if args.verify_unknown_commit and not args.only_restore_execution:
        parser.error('--verify-unknown-commit requires --only-restore-execution')
    artifact = args.artifact.resolve()
    output = args.output.resolve()
    output.relative_to(ROOT)
    output.mkdir(parents=True, exist_ok=True)
    project = 'rivomesh-adoption-' + uuid.uuid4().hex[:10]
    password = secrets.token_hex(20)
    with artifact.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    restoration_profile = args.only_restoration_plan or args.only_restore_execution
    profile_name = ('WRITER_READINESS' if args.only_writer_readiness else
                    'ADOPTION_EXECUTION' if args.only_adoption_execution else
                    'ADOPTION_PLAN' if args.only_adoption_plan else
                    'UNIQUE_RESTORE_EXECUTION' if args.only_restore_execution else
                    'UNIQUE_RESTORE_PLAN' if args.only_restoration_plan else
                    'CONSTRAINT_REVIEW' if args.only_constraints else 'ADOPTION_PREVIEW')
    report = {'artifactSha256': digest, 'checks': [], 'status': 'FAIL', 'profile': profile_name}
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
        compose_command = ['docker', 'compose', '-p', project, '-f', str(compose_path)]

        def compose(*arguments, statement=None, check=True):
            return subprocess.run([*compose_command, *arguments],
                                  cwd=ROOT, env=environment, input=statement, check=check, capture_output=True,
                                  text=True, timeout=180)

        @contextmanager
        def sql_session(*arguments):
            process = subprocess.Popen([*compose_command, 'exec', '-T', *arguments], cwd=ROOT, env=environment,
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                       text=True, bufsize=1)
            try:
                yield process
            finally:
                process.stdin.close()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    process.wait(timeout=10)
                process.stdout.close()

        def exchange_sql(process, statement):
            process.stdin.write(statement + '\n')
            process.stdin.flush()
            assert select.select([process.stdout], [], [], 8)[0], 'Fixture SQL session did not reply'
            value = process.stdout.readline()
            assert value, 'Fixture SQL session closed before replying'
            return value.strip()

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

        def adoption_plan():
            target = dict(case['targets'][0], username='postgres', metadataSchema='planned_sink')
            journal = {'mode': 'EXTERNAL', 'product': 'POSTGRESQL', 'url': target['url'],
                       'username': 'postgres', 'password': '${RM_ADOPTION_PASSWORD}', 'namespace': 'planned_journal'}
            plan_config = dict(case, journal=journal, targets=[target])
            (directory / 'adoption-plan.yml').write_text(json.dumps(plan_config))
            mysql("""
                CREATE DATABASE sales CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
                CREATE TABLE sales.items(id BIGINT PRIMARY KEY,name VARCHAR(80),doc JSON) ENGINE=InnoDB;
                INSERT INTO sales.items VALUES (2,'tail ',NULL),(1,'sensitive-row-value','{"b":2,"a":1.0}');
            """)
            postgres('''
                CREATE SCHEMA review;
                CREATE TABLE review.items(id BIGINT PRIMARY KEY,name VARCHAR(80) COLLATE "C",doc JSONB);
                INSERT INTO review.items VALUES (1,'sensitive-row-value','{"a":1,"b":2.0}'),(2,'tail ',NULL);
            ''')
            target_contents = "SELECT md5(jsonb_agg(to_jsonb(item) ORDER BY id)::text) FROM review.items item"
            original_contents = postgres(target_contents)

            def plan(*options, check=True):
                return compose('run', '--rm', '-T', 'runner', 'plan-adoption', '/fixture/adoption-plan.yml', 'pg',
                               *options, check=check)

            def refused(code, *options):
                answer = plan(*options, check=False)
                assert answer.returncode and not answer.stdout.strip()
                assert code in answer.stderr, (code, re.findall(r'\b(?:ADOPTION|SQL)_[A-Z0-9_]+', answer.stderr))

            def unchanged_target():
                assert postgres("SELECT to_regnamespace('planned_sink') IS NULL AND "
                                "to_regnamespace('planned_journal') IS NULL AND "
                                "to_regclass('review._rivomesh_owner_v3') IS NULL") == 't'
                assert postgres(target_contents) == original_contents
                assert not (directory / 'journal').exists()

            refused('ADOPTION_PLAN_SOURCE_NOT_FROZEN')
            mysql('SET GLOBAL super_read_only=ON')
            refused('ADOPTION_PLAN_SOURCE_FREEZE_NOT_PERSISTED')
            unchanged_target()
            passed('An unfrozen or runtime-only MySQL source cannot produce an adoption plan')

            mysql('SET PERSIST read_only=ON; SET PERSIST super_read_only=ON')
            original = plan()
            assert 'sensitive-row-value' not in original.stdout
            observed = json.loads(original.stdout)
            assert observed['status'] == 'ADOPTION_PLAN_OBSERVED' and observed['plan']['matchedRows'] == 2
            approved = observed['planSha256']
            assert re.fullmatch('[a-f0-9]{64}', approved)
            exact = json.loads(plan('--expect-sha256', approved).stdout)
            assert exact['expectedDigestMatched'] and exact['plan'] == observed['plan'] and exact['planSha256'] == approved
            refused('ADOPTION_PLAN_CHANGED', '--expect-sha256', '0' * 64)
            unchanged_target()
            passed('Matching native rows and exact types produce a repeatable plan digest with no data or metadata writes')

            if args.only_adoption_execution:
                verify_adoption_execution(plan_config, approved, unchanged_target, target_contents, original_contents)
                return
            if args.only_writer_readiness:
                verify_writer_readiness(approved, target_contents, original_contents)
                return

            postgres("UPDATE review.items SET name='tail' WHERE id=2")
            refused('ADOPTION_PLAN_ROWS_DIFFER', '--expect-sha256', approved)
            postgres("UPDATE review.items SET name='tail ' WHERE id=2")
            postgres("ALTER TABLE review.items ALTER COLUMN name TYPE VARCHAR(81) COLLATE \"C\"")
            refused('ADOPTION_PLAN_TARGET_COLUMN_MISMATCH')
            postgres("ALTER TABLE review.items ALTER COLUMN name TYPE VARCHAR(80) COLLATE \"C\"")
            postgres("ALTER TABLE review.items ALTER COLUMN name SET DEFAULT 'x'")
            refused('ADOPTION_PLAN_TARGET_COLUMN_MISMATCH')
            postgres("ALTER TABLE review.items ALTER COLUMN name DROP DEFAULT")
            unchanged_target()
            passed('A changed row, target width, or target column default fails adoption admission')

            postgres("CREATE TABLE review._rivomesh_owner_v3(table_name TEXT PRIMARY KEY); "
                     "INSERT INTO review._rivomesh_owner_v3 VALUES ('items')")
            refused('ADOPTION_TARGET_ALREADY_OWNED')
            postgres("DROP TABLE review._rivomesh_owner_v3; CREATE SCHEMA planned_journal")
            refused('ADOPTION_PLAN_METADATA_PRESENT')
            postgres("DROP SCHEMA planned_journal")
            unchanged_target()
            passed('A registered owner or pre-existing journal namespace fails closed')

            after_edits = json.loads(plan().stdout)['planSha256']
            postgres('REINDEX INDEX review.items_pkey')
            refused('ADOPTION_PLAN_CHANGED', '--expect-sha256', after_edits)
            unchanged_target()
            passed('A recreated physical PostgreSQL primary index invalidates the previously reviewed plan')

            mysql('SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF; '
                  "UPDATE sales.items SET doc=JSON_OBJECT('large',REPEAT('z',1048576)) WHERE id=1; "
                  'SET GLOBAL read_only=ON; SET GLOBAL super_read_only=ON')
            refused('ADOPTION_ROW_TOO_LARGE')
            unchanged_target()
            passed('An oversized source row is rejected by a bounded size probe before streaming business values')

            mysql('SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF; '
                  'UPDATE sales.items SET doc=\'{"b":2,"a":1.0}\' WHERE id=1; '
                  'CREATE TABLE sales.additional(id BIGINT PRIMARY KEY); '
                  'SET GLOBAL read_only=ON; SET GLOBAL super_read_only=ON')
            refused('ADOPTION_PLAN_SOURCE_SCOPE_UNSUPPORTED')
            unchanged_target()
            passed('Discovery of a second mapped source table refuses a partial existing-target bootstrap')

        def verify_adoption_execution(plan_config, approved, unchanged_target, target_contents, original_contents):
            request = str(uuid.uuid4())
            acknowledgement = '--acknowledge-exclusive-freeze'

            def cli(operation, *parameters, check=True):
                return compose('run', '--rm', '-T', 'runner', operation, '/fixture/adoption-plan.yml', *parameters, check=check)

            def adopt(digest=approved, actor='fixture-operator', confirmation=acknowledgement, check=True):
                return cli('adopt-existing', 'pg', 'items', digest, request, actor, confirmation, check=check)

            def refused(answer, *codes):
                assert answer.returncode != 0 and not answer.stdout.strip()
                assert any(code in answer.stderr for code in codes), re.findall(r'\b(?:ADOPTION|SQL)_[A-Z0-9_]+', answer.stderr)

            refused(adopt(confirmation='--no-acknowledgement', check=False), 'ADOPTION_EXCLUSIVE_FREEZE_ACK_REQUIRED')
            refused(adopt(digest='0' * 64, check=False), 'ADOPTION_PLAN_CHANGED')
            unchanged_target()
            passed('An explicit exclusive freeze acknowledgment and original plan digest are required for baseline installation')

            postgres('CREATE SCHEMA review_ack; CREATE TABLE review_ack.items (LIKE review.items INCLUDING ALL); '
                     'INSERT INTO review_ack.items SELECT * FROM review.items')
            confirmed_target = dict(plan_config['targets'][0], metadataSchema='ack_sink', mappings={'sales': 'review_ack'})
            confirmed_config = dict(plan_config, pipelineId='adoption-acknowledged', targets=[confirmed_target],
                                    journal=dict(plan_config['journal'], namespace='ack_journal'))
            (directory / 'adoption-ack.yml').write_text(json.dumps(confirmed_config))
            confirmed_plan = json.loads(compose('run', '--rm', '-T', 'runner', 'plan-adoption', '/fixture/adoption-ack.yml', 'pg').stdout)
            confirmed_args = ('adopt-existing', '/fixture/adoption-ack.yml', 'pg', 'items', confirmed_plan['planSha256'],
                              str(uuid.uuid4()), 'fixture-operator', acknowledgement)
            first = json.loads(compose('run', '--rm', '-T', 'runner', *confirmed_args).stdout)
            second = json.loads(compose('run', '--rm', '-T', 'runner', *confirmed_args).stdout)
            assert first['acknowledged'] and not first['replayed'] and second['acknowledged'] and second['replayed']
            assert first['receipt'] == second['receipt'] and postgres('SELECT count(*) FROM ack_sink.rm3_sink_adoption') == '1'
            passed('A known successful first commit and exact replay return the same single receipt and identify the replay')

            with (ThreadPoolExecutor(max_workers=1) as background,
                  sql_session('target', 'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'preview') as holder,
                  sql_session('source', 'bash', '-c',
                              'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -NB --unbuffered') as killer):
                assert exchange_sql(holder, "BEGIN; LOCK TABLE pg_catalog.pg_class IN SHARE MODE; SELECT 'HELD';") == 'HELD'
                assert exchange_sql(killer, "SELECT 'READY';") == 'READY'
                interrupted = background.submit(adopt, check=False)
                try:
                    session = wait_for(lambda: int(exchange_sql(killer, """
                        SELECT COALESCE(MAX(t.PROCESSLIST_ID),0) FROM performance_schema.user_variables_by_thread v
                          JOIN performance_schema.threads t ON t.THREAD_ID=v.THREAD_ID
                         WHERE v.VARIABLE_NAME='rivomesh_unique_freeze_session';
                        """)) or None, 'pinning the original source session during adoption')
                    wait_for(lambda: exchange_sql(holder, """
                        SELECT EXISTS(SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
                         WHERE l.relation='pg_catalog.pg_class'::regclass AND l.mode='RowExclusiveLock'
                           AND NOT l.granted AND a.query LIKE 'CREATE TABLE "planned_sink"."rm3_sink_adoption"%');
                        """) == 't', 'observing receipt-table DDL after namespace creation')
                    assert exchange_sql(killer, f"KILL CONNECTION {session}; SELECT 'KILLED';") == 'KILLED'
                finally:
                    assert exchange_sql(holder, "ROLLBACK; SELECT 'RELEASED';") == 'RELEASED'
                rejected = interrupted.result(timeout=70)
            unchanged_target()
            refused(rejected, 'ADOPTION_SOURCE_FENCE_LOST', 'ADOPTION_PLAN_SOURCE_IDENTITY_CHANGED')
            passed('Loss of the pinned source session after PostgreSQL namespace DDL rolls back ownership, namespaces and baseline')

            postgres("ALTER SYSTEM SET synchronous_standby_names='fixture_missing'")
            try:
                postgres('SELECT pg_reload_conf()')
                wait_for(lambda: postgres('SHOW synchronous_standby_names') == 'fixture_missing', 'stalling local commit acknowledgement')
                with ThreadPoolExecutor(max_workers=1) as background:
                    lost = background.submit(adopt, check=False)
                    backend = int(wait_for(lambda: postgres("""
                        SELECT a.pid FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid
                         WHERE a.wait_event='SyncRep' AND l.relation='review.items'::regclass
                           AND l.granted AND l.mode='AccessExclusiveLock'
                        """), 'waiting for the locally flushed adoption commit'))
                    assert postgres(f'SELECT pg_terminate_backend({backend})') == 't'
                    refused(lost.result(timeout=60), 'ADOPTION_COMMIT_UNKNOWN')
            finally:
                postgres('ALTER SYSTEM RESET synchronous_standby_names')
                postgres('SELECT pg_reload_conf()')
            wait_for(lambda: postgres('SHOW synchronous_standby_names') == '', 'releasing the fixture commit stall')
            assert postgres(target_contents) == original_contents
            assert postgres("SELECT run_state,epoch,next_seq,normalized_seq FROM planned_journal.rm3_job") == 'PAUSED|0|1|0'
            assert postgres("SELECT snapshot_state,next_part,row_count FROM planned_sink.rm3_sink_table") == 'COMPLETE|0|2'
            assert postgres("SELECT count(*) FROM planned_sink.rm3_sink_adoption") == '1'
            compose('stop', 'source')
            compose('restart', 'target')
            compose('up', '-d', '--wait', '--wait-timeout', '140', 'target')
            answer = adopt()
            assert 'sensitive-row-value' not in answer.stdout
            replayed = json.loads(answer.stdout)
            assert replayed['status'] == 'EXISTING_TARGET_ADOPTED' and replayed['acknowledged'] and replayed['replayed']
            assert replayed['resumeErrorGeneration'] == 1 and replayed['receipt']['rows'] == 2
            denied = adopt(actor='different-operator', check=False)
            refused(denied, 'ADOPTION_APPROVAL_CONFLICT')
            passed('Lost commit acknowledgement resolves to one exact durable receipt after PostgreSQL restart with MySQL offline')

            postgres('UPDATE planned_sink.rm3_sink_table SET row_count=3')
            refused(adopt(check=False), 'ADOPTION_BOOTSTRAP_CHECKPOINT_CHANGED')
            postgres('UPDATE planned_sink.rm3_sink_table SET row_count=2')
            postgres("UPDATE review.items SET name='different' WHERE id=2")
            refused(adopt(check=False), 'ADOPTION_BOOTSTRAP_ROWS_CHANGED')
            postgres("UPDATE review.items SET name='tail ' WHERE id=2")
            assert json.loads(adopt().stdout)['receipt'] == replayed['receipt']
            passed('Exact replay rejects inconsistent target checkpoints and manually drifted rows while the bootstrap is paused')

            compose('up', '-d', '--wait', '--wait-timeout', '140', 'source')
            assert mysql('SELECT @@global.read_only AND @@global.super_read_only') == '1'
            mysql('SET PERSIST super_read_only=OFF; SET PERSIST read_only=OFF')
            # These changes precede the worker start and therefore require capture from the adopted position.
            mysql("START TRANSACTION; UPDATE sales.items SET name='updated' WHERE id=1; "
                  "INSERT INTO sales.items VALUES (3,'inserted','{\"new\":true}'); DELETE FROM sales.items WHERE id=2; COMMIT")
            cli('resume-pipeline', str(replayed['resumeErrorGeneration']))
            worker = project + '-adopted-worker'
            compose('run', '--rm', '-d', '-T', '--name', worker, 'runner', 'run', '/fixture/adoption-plan.yml')
            try:
                wait_for(lambda: postgres("SELECT string_agg(id||':'||name,',' ORDER BY id) FROM review.items") == '1:updated,3:inserted',
                         'waiting for native CDC from the first adopted position', seconds=110)
                assert postgres('SELECT snapshot_id FROM planned_sink.rm3_sink_table') == replayed['receipt']['snapshot']
                assert postgres('SELECT count(*) FROM planned_journal.rm3_snapshot_part') == '0'
                assert int(postgres('SELECT stream_seq FROM planned_sink.rm3_sink_job')) >= 1
                assert postgres('SELECT count(*) FROM planned_sink.rm3_sink_adoption') == '1'
            except Exception:
                worker_logs = subprocess.run(['docker', 'logs', '--tail', '80', worker], capture_output=True,
                                             text=True, timeout=15, check=False)
                redacted = (worker_logs.stdout + worker_logs.stderr).replace(password, '[REDACTED]')
                (output / 'worker-redacted.log').write_text(redacted)
                raise
            finally:
                subprocess.run(['docker', 'stop', '-t', '5', worker], capture_output=True, timeout=30, check=False)
            refused(adopt(check=False), 'ADOPTION_BOOTSTRAP_MOVED')
            passed('The ordinary worker resumes the adopted position and applies first MySQL insert, update and delete without recopying')

        def verify_writer_readiness(approved, target_contents, original_contents):
            adoption_request = str(uuid.uuid4())
            request = str(uuid.uuid4())
            adoption_actor = 'fixture-adopter'
            acknowledgement = '--acknowledge-exclusive-freeze'
            adopt = compose('run', '--rm', '-T', 'runner', 'adopt-existing', '/fixture/adoption-plan.yml',
                            'pg', 'items', approved, adoption_request, adoption_actor, acknowledgement)
            assert json.loads(adopt.stdout)['acknowledged']
            baseline = postgres('SELECT run_state,epoch,next_seq,normalized_seq,captured FROM planned_journal.rm3_job')

            def prepare(role='readiness_writer', actor='fixture-readiness-operator', confirmation=acknowledgement, check=True):
                return compose('run', '--rm', '-T', 'runner', 'prepare-target-writer', '/fixture/adoption-plan.yml',
                               'pg', 'items', approved, adoption_request, adoption_actor, role, request, actor,
                               confirmation, check=check)

            def refused(code, **options):
                attempt = prepare(check=False, **options)
                assert attempt.returncode != 0 and not attempt.stdout.strip()
                assert code in attempt.stderr, (code, re.findall(r'\b(?:WRITER_READINESS|ADOPTION|SQL)_[A-Z0-9_]+', attempt.stderr))

            def checkpoint_unchanged():
                assert postgres('SELECT run_state,epoch,next_seq,normalized_seq,captured FROM planned_journal.rm3_job') == baseline
                assert postgres(target_contents) == original_contents

            refused('WRITER_READINESS_EXCLUSIVE_FREEZE_ACK_REQUIRED', confirmation='--no-acknowledgement')
            refused('WRITER_READINESS_ROLE_UNSAFE', role='postgres')
            postgres('CREATE ROLE readiness_writer LOGIN NOINHERIT; CREATE ROLE readiness_group NOLOGIN; '
                     'GRANT USAGE ON SCHEMA review TO readiness_writer; GRANT SELECT ON review.items TO readiness_writer')
            postgres('GRANT readiness_group TO readiness_writer')
            refused('WRITER_READINESS_ROLE_UNSAFE')
            postgres('REVOKE readiness_group FROM readiness_writer')
            postgres('GRANT UPDATE(name) ON review.items TO readiness_writer')
            refused('WRITER_READINESS_TARGET_WRITE_PRIVILEGE_PRESENT')
            postgres('REVOKE UPDATE(name) ON review.items FROM readiness_writer; GRANT DELETE ON review.items TO PUBLIC')
            refused('WRITER_READINESS_TARGET_WRITE_PRIVILEGE_PRESENT')
            postgres('REVOKE DELETE ON review.items FROM PUBLIC')
            assert postgres("SELECT to_regclass('planned_sink.rm3_sink_writer_ready') IS NULL") == 't'
            checkpoint_unchanged()
            passed('Readiness refuses owner and member roles, missing acknowledgment, column writers and PUBLIC writes')

            postgres('CREATE FUNCTION review.hidden() RETURNS integer SECURITY DEFINER LANGUAGE sql AS $$ SELECT 1 $$')
            refused('WRITER_READINESS_INDIRECT_WRITE_PATH')
            postgres('DROP FUNCTION review.hidden()')
            postgres('CREATE VIEW review.write_through AS SELECT * FROM review.items; '
                     'GRANT UPDATE ON review.write_through TO readiness_writer')
            refused('WRITER_READINESS_INDIRECT_WRITE_PATH')
            postgres('DROP VIEW review.write_through')
            with sql_session('target', 'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'readiness_writer', '-d', 'preview') as active:
                assert exchange_sql(active, "SELECT 'CONNECTED';") == 'CONNECTED'
                refused('WRITER_READINESS_ROLE_CONNECTED')
            assert postgres("SELECT to_regclass('planned_sink.rm3_sink_writer_ready') IS NULL") == 't'
            passed('Readiness refuses indirect view writes, definer execution and connected dedicated writer sessions')

            original = prepare()
            assert 'sensitive-row-value' not in original.stdout
            confirmed = json.loads(original.stdout)
            assert confirmed['status'] == 'TARGET_WRITER_READINESS_RECORDED' and confirmed['acknowledged'] and not confirmed['replayed']
            assert re.fullmatch('[a-f0-9]{64}', confirmed['receipt']['evidenceSha256'])
            assert re.fullmatch('[a-f0-9]{64}', confirmed['receiptSha256'])
            assert postgres('SELECT count(*) FROM planned_sink.rm3_sink_writer_ready') == '1'
            assert postgres("SELECT has_any_column_privilege('readiness_writer','review.items','INSERT,UPDATE') OR "
                            "has_table_privilege('readiness_writer','review.items','DELETE,TRUNCATE')") == 'f'
            compose('restart', 'target')
            compose('up', '-d', '--wait', '--wait-timeout', '140', 'target')
            replay = json.loads(prepare().stdout)
            assert replay['replayed'] and replay['receipt'] == confirmed['receipt'] and replay['receiptSha256'] == confirmed['receiptSha256']
            refused('WRITER_READINESS_APPROVAL_CONFLICT', actor='different-reviewer')
            checkpoint_unchanged()
            passed('The durable receipt resolves to the exact same approval after PostgreSQL restart with target authority still read only')

            mysql('SET GLOBAL super_read_only=OFF')
            refused('ADOPTION_PLAN_SOURCE_NOT_FROZEN')
            mysql('SET GLOBAL super_read_only=ON')
            postgres('GRANT SELECT ON review.items TO readiness_group')
            refused('WRITER_READINESS_EVIDENCE_CHANGED')
            assert postgres('SELECT count(*) FROM planned_sink.rm3_sink_writer_ready') == '1'
            checkpoint_unchanged()
            passed('Replay checks the live source freeze and refuses changed authority evidence without rewriting the receipt')

        def constraint_review():
            target = dict(case['targets'][0], username='postgres', metadataSchema='unique_meta',
                          mappings={'unique_source': 'unique_copy'})
            managed = dict(case, pipelineId='unique-cut', nodeId='cut-worker', databases=['unique_source'],
                           targets=[target], chunkSize=2, leaseMillis=8000, heartbeatMillis=400,
                           journal={'mode': 'EXTERNAL', 'product': 'POSTGRESQL', 'namespace': 'unique_journal',
                                    'url': target['url'], 'username': 'postgres', 'password': target['password']})
            (directory / 'unique.yml').write_text(json.dumps(managed))
            source_collation = 'utf8mb4_0900_bin' if restoration_profile else 'utf8mb4_bin'
            target_collation = 'pg_catalog."C"' if restoration_profile else 'unique_copy.case_insensitive'
            nickname_source = 'VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin' if args.only_restore_execution else 'VARCHAR(40)'
            nickname_target = 'VARCHAR(40) COLLATE pg_catalog."C"' if args.only_restore_execution else 'VARCHAR(40)'
            mysql(f"""
                CREATE DATABASE unique_source;
                CREATE TABLE unique_source.accounts (
                    id BIGINT PRIMARY KEY, tenant BIGINT,
                    email VARCHAR(80) CHARACTER SET utf8mb4 COLLATE {source_collation},
                    nickname {nickname_source}, UNIQUE KEY uq_email(email), UNIQUE KEY uq_scope(tenant,nickname)
                ) ENGINE=InnoDB;
                INSERT INTO unique_source.accounts VALUES
                    (1,1,'Sensitive-Mail@example.com','one'), (2,1,'sensitive-mail@example.com','two'),
                    (3,1,NULL,NULL), (4,1,NULL,NULL);
            """)
            if restoration_profile:
                mysql("INSERT INTO unique_source.accounts VALUES (5,1,'sensitive-mail@example.com ','three')")
            postgres(f"""
                CREATE SCHEMA unique_copy;
                CREATE COLLATION unique_copy.case_insensitive
                    (provider=icu,locale='und-u-ks-level2',deterministic=false);
                CREATE TABLE unique_copy.accounts (
                    id BIGINT PRIMARY KEY, tenant BIGINT,
                    email VARCHAR(80) COLLATE {target_collation}, nickname {nickname_target}
                );
            """)

            def cli(operation, *parameters, check=True):
                return compose('run', '--rm', '-T', 'runner', operation, '/fixture/unique.yml',
                               *parameters, check=check)

            worker = project + '-cut-worker'
            compose('run', '--rm', '-d', '-T', '--name', worker, 'runner', 'run', '/fixture/unique.yml')
            try:
                expected_rows = '5' if restoration_profile else '4'
                wait_for(lambda: postgres("SELECT count(*) FROM unique_copy.accounts") == expected_rows,
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

                if args.only_restoration_plan:
                    verify_restoration_plan(cli, review_args)
                    return
                if args.only_restore_execution:
                    verify_restore_execution(cli, review_args)
                    return

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

        def verify_restoration_plan(cli, review_args):
            review = json.loads(cli('review-unique', *review_args).stdout)
            assert review['status'] == 'NO_DUPLICATES_OBSERVED'
            table = next(index for index in review['indexes'] if index['sourceIndex'] == 'uq_email')['tableId']
            plan_args = (*review_args, table, 'uq_email')

            def refused(code, *flags):
                attempt = cli('plan-unique-restore', *plan_args, *flags, check=False)
                assert attempt.returncode != 0 and code in attempt.stderr and not attempt.stdout, code

            def snapshot():
                return postgres("SELECT jsonb_agg(to_jsonb(a) ORDER BY id) FROM unique_copy.accounts a; "
                                "SELECT epoch,stream_seq FROM unique_meta.rm3_sink_job; "
                                "SELECT pg_get_indexdef(indexrelid) FROM pg_index "
                                "WHERE indrelid='unique_copy.accounts'::regclass ORDER BY indexrelid")

            refused('UNIQUE_RESTORE_SOURCE_NOT_FROZEN')
            passed('A matching paused pipeline still requires physical MySQL super-read-only isolation')
            mysql('SET GLOBAL super_read_only=ON')
            before = snapshot()
            first = cli('plan-unique-restore', *plan_args).stdout
            assert 'sensitive-mail' not in first.lower()
            body = json.loads(first)
            evidence = body['plan']['targetEvidence']
            assert body['status'] == 'RESTORE_PLAN_OBSERVED' and len(body['planSha256']) == 64
            assert evidence['rows'] == 5 and evidence['columns'] == ['email'] and len(evidence['physicalHash']) == 64
            assert evidence['ordinaryIndex'].startswith('rm_') and evidence['proposedConstraint'].startswith('rm_u_')
            digest_args = ('--expect-sha256', body['planSha256'])
            repeated = json.loads(cli('plan-unique-restore', *plan_args, *digest_args).stdout)
            assert body['planSha256'] == repeated['planSha256'] and repeated['expectedDigestMatched']
            assert before == snapshot() and mysql('SELECT @@global.read_only AND @@global.super_read_only') == '1'
            passed('A frozen native pair yields a stable digest with no row or schema changes and preserves trailing-space semantics')

            ordinary = evidence['ordinaryIndex']
            assert ordinary.isascii() and ordinary.replace('_', '').isalnum()
            postgres('REINDEX INDEX unique_copy."' + ordinary + '"')
            refused('UNIQUE_RESTORE_PLAN_CHANGED', *digest_args)
            assert before == snapshot()
            passed('Rebuilding the same ordinary index invalidates an expected digest without changing rows or checkpoints')

            same_gtids = mysql('SELECT @@global.gtid_executed')
            mysql("SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF; SET SESSION sql_log_bin=0; "
                  "UPDATE unique_source.accounts SET nickname='out-of-band' WHERE id=1; SET GLOBAL super_read_only=ON")
            assert mysql('SELECT @@global.gtid_executed') == same_gtids
            refused('UNIQUE_RESTORE_SOURCE_CONTENT_CHANGED')
            mysql("SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF; SET SESSION sql_log_bin=0; "
                  "UPDATE unique_source.accounts SET nickname='one' WHERE id=1; SET GLOBAL super_read_only=ON")
            assert mysql('SELECT @@global.gtid_executed') == same_gtids
            passed('Fresh source root rejects unlogged row changes even at the exact original MySQL GTID')

            postgres("UPDATE unique_copy.accounts SET nickname='out-of-band' WHERE id=1")
            refused('UNIQUE_RESTORE_TARGET_CONTENT_CHANGED')
            postgres("UPDATE unique_copy.accounts SET nickname='one' WHERE id=1")
            passed('Fresh target root rejects external row changes even when the physical checkpoint is untouched')

            postgres('ALTER TABLE unique_copy.accounts ALTER COLUMN email TYPE VARCHAR(80) COLLATE unique_copy.case_insensitive')
            refused('UNIQUE_RESTORE_COLLATION_UNSUPPORTED')
            passed('Restoration planning refuses an unapproved PostgreSQL key collation')

            mysql("SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF; "
                  "CREATE DATABASE after_cut; SET GLOBAL super_read_only=ON")
            assert mysql('SELECT @@global.gtid_executed') != same_gtids
            refused('UNIQUE_RESTORE_SOURCE_CUT_CHANGED')
            passed('A new source GTID invalidates the restoration plan even when the reviewed table content is unchanged')

        def verify_restore_execution(cli, review_args):
            mysql('SET GLOBAL super_read_only=ON')
            review = json.loads(cli('review-unique', *review_args).stdout)
            table = next(row for row in review['indexes'] if row['sourceIndex'] == 'uq_email')['tableId']
            freeze_ack = '--acknowledge-exclusive-freeze'
            actor = 'fixture-migration-operator'
            bytes_before = postgres("SELECT jsonb_agg(to_jsonb(a) ORDER BY id) FROM unique_copy.accounts a")
            checkpoint_before = postgres("SELECT epoch,stream_seq FROM unique_meta.rm3_sink_job; "
                                         "SELECT run_state,epoch,next_seq FROM unique_journal.rm3_job")

            def planned(index):
                args = (*review_args, table, index)
                return args, json.loads(cli('plan-unique-restore', *args).stdout)

            def restored(args, plan, request, check=True, entered_digest=None, entered_actor=actor, ack=freeze_ack):
                return cli('restore-unique', *args, entered_digest if entered_digest is not None else plan['planSha256'],
                           request, entered_actor, ack, check=check)

            def require_refusal(answer, *codes):
                assert answer.returncode != 0 and not answer.stdout.strip()
                if not any(code in answer.stderr for code in codes):
                    match = re.search(r"Exception: ([A-Z][A-Z0-9_]{2,95})\n", answer.stderr)
                    report['unexpectedRefusalCode'] = match.group(1) if match else 'NO_TYPED_STATUS'
                    raise AssertionError((codes, report['unexpectedRefusalCode']))

            def constraint_count(name):
                assert name.isascii() and name.replace('_', '').isalnum()
                return postgres("SELECT count(*) FROM pg_constraint WHERE conrelid='unique_copy.accounts'::regclass "
                                f"AND conname='{name}'")

            def receipt_count():
                return postgres("SELECT count(*) FROM unique_meta.rm3_sink_unique_restore")

            def unchanged():
                assert bytes_before == postgres("SELECT jsonb_agg(to_jsonb(a) ORDER BY id) FROM unique_copy.accounts a")
                assert checkpoint_before == postgres("SELECT epoch,stream_seq FROM unique_meta.rm3_sink_job; "
                                                     "SELECT run_state,epoch,next_seq FROM unique_journal.rm3_job")

            plan_args, plan = planned('uq_email')
            constraint = plan['plan']['targetEvidence']['proposedConstraint']
            request = str(uuid.uuid4())
            require_refusal(restored(plan_args, plan, request, check=False), 'UNIQUE_RESTORE_SOURCE_FREEZE_NOT_PERSISTED')
            assert constraint_count(constraint) == '0'
            assert postgres("SELECT to_regclass('unique_meta.rm3_sink_unique_restore')::text") == ''
            passed('Runtime-only MySQL freeze cannot admit DDL or create a receipt')

            mysql('SET PERSIST read_only=ON; SET PERSIST super_read_only=ON')
            require_refusal(restored(plan_args, plan, request, check=False, ack='unacknowledged'),
                            'UNIQUE_RESTORE_EXCLUSIVE_FREEZE_ACK_REQUIRED')
            require_refusal(restored(plan_args, plan, request, check=False, entered_digest='0' * 64),
                            'UNIQUE_RESTORE_PLAN_CHANGED')
            assert constraint_count(constraint) == '0'
            unchanged()
            passed('Execution requires explicit exclusive freeze acknowledgement and the exact reviewed digest')

            postgres("""CREATE FUNCTION unique_copy.noop_event() RETURNS event_trigger LANGUAGE plpgsql
                        AS $$ BEGIN RETURN; END $$;
                        CREATE EVENT TRIGGER fixture_event ON ddl_command_start EXECUTE FUNCTION unique_copy.noop_event()""")
            require_refusal(restored(plan_args, plan, request, check=False), 'UNIQUE_RESTORE_TARGET_EVENT_TRIGGER_PRESENT')
            assert constraint_count(constraint) == '0'
            postgres('DROP EVENT TRIGGER fixture_event; DROP FUNCTION unique_copy.noop_event()')
            passed('An enabled PostgreSQL event trigger prevents any managed DDL')

            with ThreadPoolExecutor(max_workers=3) as background:
                guard_name = 'unique_business_reader'
                holder = background.submit(compose, 'exec', '-T', '-e', f'PGAPPNAME={guard_name}', 'target', 'psql',
                                           '-X', '-q', '-U', 'postgres', '-d', 'preview', '-c',
                                           'BEGIN; LOCK TABLE unique_copy.accounts IN ACCESS SHARE MODE; SELECT pg_sleep(30)',
                                           check=False)
                try:
                    wait_for(lambda: postgres("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE "
                                               f"application_name='{guard_name}' AND wait_event='PgSleep')") == 't',
                             'holding a live table read lock')
                    first = background.submit(restored, plan_args, plan, request, check=False)
                    wait_for(lambda: postgres("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE "
                                               "relation='unique_copy.accounts'::regclass AND mode='AccessExclusiveLock' "
                                               "AND NOT granted)") == 't', 'observing the first exclusive write lock')
                    second = background.submit(restored, plan_args, plan, request, check=False)
                finally:
                    postgres(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='{guard_name}'")
                    holder.result(timeout=45)
                responses = [first.result(timeout=80), second.result(timeout=80)]
            successes = [json.loads(response.stdout) for response in responses if response.returncode == 0]
            for response in responses:
                if response.returncode != 0:
                    require_refusal(response, 'VALIDATION_REPORT_BUSY', 'SQL_55P03')
            assert len([result for result in successes if not result['replayed']]) == 1
            confirmed = json.loads(restored(plan_args, plan, request).stdout)
            assert confirmed['status'] == 'UNIQUE_RESTORED' and confirmed['acknowledged'] and confirmed['replayed']
            assert all(result['acknowledged'] and result['receipt'] == confirmed['receipt'] for result in successes)
            assert 'sensitive-mail' not in json.dumps(confirmed).lower()
            assert receipt_count() == '1' and constraint_count(constraint) == '1'
            ordinary = plan['plan']['targetEvidence']['ordinaryIndex']
            assert ordinary.isascii() and ordinary.replace('_', '').isalnum()
            assert postgres("SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
                            "WHERE i.indrelid='unique_copy.accounts'::regclass AND NOT i.indisunique "
                            f"AND c.relname='{ordinary}'") == '1'
            postgres("""DO $$ BEGIN
                          BEGIN
                            INSERT INTO unique_copy.accounts VALUES (99,1,'Sensitive-Mail@example.com','tmp');
                            RAISE EXCEPTION 'UNIQUE was not enforced';
                          EXCEPTION WHEN unique_violation THEN NULL;
                          END;
                        END $$""")
            unchanged()
            passed('Concurrent identical approvals produce one native enforced constraint and one matching receipt under the held locks')

            second_args, second_plan = planned('uq_scope')
            second_constraint = second_plan['plan']['targetEvidence']['proposedConstraint']
            second_request = str(uuid.uuid4())
            with (ThreadPoolExecutor(max_workers=1) as background,
                  sql_session('target', 'psql', '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'preview') as holder,
                  sql_session('source', 'bash', '-c',
                              'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -NB --unbuffered') as killer):
                assert exchange_sql(holder, "BEGIN; LOCK TABLE unique_meta.rm3_sink_unique_restore IN SHARE MODE; SELECT 'HELD';") == 'HELD'
                assert exchange_sql(killer, "SELECT 'READY';") == 'READY'
                interrupted = background.submit(restored, second_args, second_plan, second_request, check=False)
                try:
                    session = wait_for(lambda: int(exchange_sql(killer, """
                        SELECT COALESCE(MAX(t.PROCESSLIST_ID),0) FROM performance_schema.user_variables_by_thread v
                          JOIN performance_schema.threads t ON t.THREAD_ID=v.THREAD_ID
                         WHERE v.VARIABLE_NAME='rivomesh_unique_freeze_session';
                        """)) or None, 'pinning the exact freeze session')
                    wait_for(lambda: exchange_sql(holder, "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE "
                                               "relation='unique_meta.rm3_sink_unique_restore'::regclass "
                                               "AND mode='RowExclusiveLock' AND NOT granted);") == 't',
                             'observing receipt insertion after PostgreSQL DDL')
                    assert exchange_sql(killer, f"KILL CONNECTION {session}; SELECT 'KILLED';") == 'KILLED'
                finally:
                    assert exchange_sql(holder, "ROLLBACK; SELECT 'RELEASED';") == 'RELEASED'
                interrupted_answer = interrupted.result(timeout=70)
            assert receipt_count() == '1' and constraint_count(second_constraint) == '0'
            unchanged()
            report['postDdlSourceLossRollbackObserved'] = True
            require_refusal(interrupted_answer, 'UNIQUE_RESTORE_SOURCE_FENCE_LOST', 'UNIQUE_RESTORE_SOURCE_IDENTITY_CHANGED',
                            'UNIQUE_RESTORE_SOURCE_FREEZE_EXPIRED')
            passed('Losing the exact MySQL session after PostgreSQL DDL rolls the constraint and its receipt back together')

            require_refusal(restored(plan_args, plan, request, check=False, entered_actor='other-operator'),
                            'UNIQUE_RESTORE_APPROVAL_CONFLICT')
            require_refusal(restored(plan_args, plan, str(uuid.uuid4()), check=False), 'UNIQUE_RESTORE_APPROVAL_CONFLICT')
            passed('Receipt replay rejects a changed approval actor and a second request for the same UNIQUE')

            if args.verify_unknown_commit:
                assert postgres("SHOW synchronous_standby_names") == ''
                postgres("ALTER SYSTEM SET synchronous_standby_names='fixture_missing'")
                try:
                    postgres("SELECT pg_reload_conf()")
                    wait_for(lambda: postgres("SHOW synchronous_standby_names") == 'fixture_missing',
                             'enabling the fixture commit stall')
                    with ThreadPoolExecutor(max_workers=1) as background:
                        lost = background.submit(restored, second_args, second_plan, second_request, check=False)
                        pid = int(wait_for(lambda: postgres("""
                            SELECT a.pid FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid
                             WHERE a.wait_event='SyncRep' AND l.relation='unique_copy.accounts'::regclass
                               AND l.mode='AccessExclusiveLock' AND l.granted
                            """), 'waiting for locally flushed restoration COMMIT'))
                        assert postgres(f"SELECT pg_terminate_backend({pid})") == 't'
                        require_refusal(lost.result(timeout=50), 'UNIQUE_RESTORE_COMMIT_UNKNOWN')
                finally:
                    postgres("ALTER SYSTEM RESET synchronous_standby_names")
                    postgres("SELECT pg_reload_conf()")
                wait_for(lambda: postgres("SHOW synchronous_standby_names") == '', 'releasing the fixture commit stall')
                resolved = json.loads(restored(second_args, second_plan, second_request).stdout)
                assert resolved['replayed'] and resolved['acknowledged'] and receipt_count() == '2'
                assert constraint_count(second_constraint) == '1'
                unchanged()
                passed('A lost PostgreSQL commit response reports UNKNOWN and exact request replay resolves the locally durable receipt')

            source_id = mysql('SELECT @@global.server_uuid')
            compose('restart', 'source')
            compose('up', '-d', '--wait', '--wait-timeout', '140', 'source')
            assert mysql('SELECT @@global.server_uuid') == source_id
            assert mysql('SELECT @@global.read_only AND @@global.super_read_only AND @@global.persisted_globals_load') == '1'
            if args.verify_unknown_commit:
                require_refusal(cli('plan-unique-restore', *second_args, check=False), 'UNIQUE_RESTORE_SOURCE_CUT_CHANGED')
            else:
                require_refusal(restored(second_args, second_plan, second_request, check=False), 'UNIQUE_RESTORE_SOURCE_CUT_CHANGED')
                assert constraint_count(second_constraint) == '0'
            passed('MySQL restart retains its persisted write fence and invalidates the previous binlog cut')

            compose('stop', 'source')
            compose('restart', 'target')
            compose('up', '-d', '--wait', '--wait-timeout', '140', 'target')
            replay = json.loads(restored(plan_args, plan, request).stdout)
            assert replay['replayed'] and replay['acknowledged'] and replay['receipt'] == confirmed['receipt']
            assert receipt_count() == ('2' if args.verify_unknown_commit else '1') and constraint_count(constraint) == '1'
            unchanged()
            passed('PostgreSQL restart and an offline source retain exact native receipt and constraint replay')

        try:
            compose('up', '-d', '--wait', '--wait-timeout', '140', 'source', 'target')
            report['mysqlVersion'] = mysql("SELECT VERSION()")
            report['postgresqlVersion'] = postgres("SHOW server_version")
            assert report['mysqlVersion'].startswith('8.4.') and report['postgresqlVersion'].startswith('17.')
            if args.only_adoption_plan or args.only_adoption_execution or args.only_writer_readiness:
                adoption_plan()
                report['status'] = 'PASS'
                return
            if args.only_constraints or restoration_profile:
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
