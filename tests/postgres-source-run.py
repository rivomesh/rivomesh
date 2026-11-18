#!/usr/bin/env python3
"""Native PostgreSQL source snapshot, pgoutput, restart and missing-slot acceptance."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
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
parser.add_argument('--source-image', default='postgres:17')
parser.add_argument('--generated', action='store_true', help='Exercise PostgreSQL 18 stored generated columns')
parser.add_argument('--failover', action='store_true',
                    help='Exercise a synchronized logical slot through physical standby promotion')
parser.add_argument('--reject-old-primary', action='store_true',
                    help='After promotion, route back to the stopped original primary instead of dropping the slot')
parser.add_argument('--jdk', type=int, choices=(21,), default=21)
args = parser.parse_args()
if args.reject_old_primary and not args.failover:
    parser.error('--reject-old-primary requires --failover')
artifact = args.artifact.resolve()
output = args.output.resolve()
output.relative_to(ROOT)
output.mkdir(parents=True, exist_ok=True)
digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
environment = dict(os.environ, RM_PG_PASSWORD=secrets.token_urlsafe(32), RM_PG_TOKEN=secrets.token_urlsafe(32))
project = 'rivomesh-pg-source-' + uuid.uuid4().hex[:10]
image='eclipse-temurin:'+str(args.jdk)+'-jre-jammy'
version_major='18' if args.generated else '17'
report = dict(artifactSha256=digest,status='RUNNING',checks=[],jdk=args.jdk,runtimeImage=image,
              sourceImage=args.source_image,failover=args.failover,rejectOldPrimary=args.reject_old_primary)
local_http=urllib.request.build_opener(urllib.request.ProxyHandler({}))


def require(check,label,seconds=120):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        try:
            observed=check()
            if observed:
                return observed
        except (OSError,urllib.error.URLError,json.JSONDecodeError,subprocess.CalledProcessError):
            pass
        time.sleep(.5)
    raise AssertionError('Timed out: '+label)


def passed(label):
    report['checks'].append(label)
    print('PASS '+label,flush=True)


with tempfile.TemporaryDirectory(prefix='pg-source-',dir=ROOT/'target') as temporary:
    home=Path(temporary)
    tasks=home/'tasks'
    tasks.mkdir()
    pipeline=dict(formatVersion=3,pipelineId='postgres_source',nodeId='pg-worker',databases=['sales','billing'],
                  source=dict(type='POSTGRESQL',clusterId='pg-example',topology='SINGLE',database='sync_test',
                              username='postgres',password='${RM_PG_PASSWORD}',expectedVersion=version_major,
                              postgresql=dict(url='jdbc:postgresql://source:5432/sync_test?connectTimeout=3&socketTimeout=15',
                                              publication='rivomesh_test',slotPrefix='rivomesh_test')),
                  targets=[dict(id='pg',product='POSTGRESQL',url='jdbc:postgresql://target:5432/sync_test?connectTimeout=3&socketTimeout=15',
                                username='postgres',password='${RM_PG_PASSWORD}',mappings=dict(sales='copy_sales',billing='copy_billing'),
                                metadataSchema='copy_meta')],
                  journal=dict(mode='LOCAL'),chunkSize=1,pollIntervalMillis=100,retryDelayMillis=200,leaseMillis=5000,heartbeatMillis=300)
    if args.failover:
        pipeline['source']['postgresql'].update(
            failoverSlot=True,url='jdbc:postgresql://source-rw:5432/sync_test?connectTimeout=3&socketTimeout=15')
    live_source='source'
    platform=dict(bind='0.0.0.0',port=8080,authToken='${RM_PG_TOKEN}',tasksDirectory='/fixture/tasks',
                  dataDirectory='/fixture/platform',autoStart=True,pollMillis=200,workerHeapMiB=256)
    (tasks/'postgres_source.yml').write_text(json.dumps(pipeline))
    (home/'platform.json').write_text(json.dumps(platform))
    database=dict(image='postgres:17',environment=dict(POSTGRES_PASSWORD='${RM_PG_PASSWORD}',POSTGRES_DB='sync_test'))
    services=dict(source=dict(database,image=args.source_image,command=['-c','wal_level=logical','-c','max_replication_slots=8',
                                                '-c','wal_sender_timeout=4000']),target=database,
                  runner=dict(image=image,entrypoint=['java','-Xmx384m','-jar','/app/rivomesh.jar'],
                              command=['serve','/fixture/platform.json'],
                              environment=dict(RM_PG_PASSWORD='${RM_PG_PASSWORD}',RM_PG_TOKEN='${RM_PG_TOKEN}'),
                              volumes=[str(artifact)+':/app/rivomesh.jar:ro',str(home)+':/fixture'],
                              ports=['127.0.0.1::8080'],stop_grace_period='30s'))
    document=dict(services=services)
    if args.failover:
        assert not args.generated, 'Use the first physical promotion profile with PostgreSQL 17'
        services['source']['networks']={'default':{},'access':{'aliases':['source-rw']}}
        services['source']['command']+=['-c','synchronized_standby_slots=rivomesh_wal']
        services['runner']['networks']=['default','access']
        standby_script = '\n'.join([
            'install -d -m 0700 -o postgres -g postgres /standby/data',
            "gosu postgres pg_basebackup -d 'host=source user=postgres dbname=sync_test' "
            '-D /standby/data -X stream -S rivomesh_wal -R',
            'exec gosu postgres postgres -D /standby/data -c wal_level=logical '
            '-c max_replication_slots=8 -c hot_standby_feedback=on -c sync_replication_slots=on',
        ])
        services['standby']=dict(image=args.source_image,entrypoint=['bash','-ceu',standby_script],
                                 environment={'PGPASSWORD':'${RM_PG_PASSWORD}'},volumes=['standby-data:/standby'])
        document.update(networks={'default':{},'access':{}},volumes={'standby-data':{}})
    composition=home/'compose.json'
    composition.write_text(json.dumps(document))

    def compose(*arguments,check=True):
        return subprocess.run(['docker','compose','-p',project,'-f',str(composition),*arguments],
                              cwd=ROOT,env=environment,capture_output=True,text=True,check=check,timeout=180)

    def sql(service,statement):
        if service=='source':
            service=live_source
        return compose('exec','-T',service,'psql','-X','-v','ON_ERROR_STOP=1','-U','postgres','-d','sync_test',
                       '-A','-t','-F','|','-c',statement).stdout.strip()

    def source_timeline():
        identity=compose('exec','-T',live_source,'psql','-X','-v','ON_ERROR_STOP=1','-U','postgres',
                         '-d','dbname=sync_test replication=database','-A','-t','-F','|',
                         '-c','IDENTIFY_SYSTEM').stdout.strip()
        system_id,timeline,_,_=identity.split('|')
        return system_id,int(timeline)

    def root_url():
        port=compose('port','runner','8080').stdout.strip().rsplit(':',1)[1]
        return 'http://127.0.0.1:'+port

    route='/api/tasks/postgres_source'

    def http(path,body=None):
        headers={'Authorization':'Bearer '+environment['RM_PG_TOKEN']}
        if body is not None:
            headers.update({'Content-Type':'application/json','X-Request-ID':'pg_source_'+uuid.uuid4().hex})
        request=urllib.request.Request(root_url()+path,data=json.dumps(body).encode() if body is not None else None,headers=headers)
        with local_http.open(request,timeout=20) as response:
            return json.load(response)

    def task():
        return http(route)

    def equal():
        stored=',derived' if args.generated else ''
        queries=[
            "SELECT id,amount,active,payload::text,encode(data,'hex')"+stored+" FROM {schema}.items ORDER BY id",
            "SELECT value,payload::text"+stored+" FROM {schema}.duplicates ORDER BY value,payload::text",
            "SELECT id,label FROM {schema}.ledger ORDER BY id"]
        for query,schema in zip(queries,('sales','sales','billing')):
            if sql('source',query.format(schema=schema))!=sql('target',query.format(schema='copy_'+schema)):
                return False
        return True

    def stream_ready():
        view=task()
        return view['sourceType']=='POSTGRESQL' and view['status'].get('capture_lsn') and equal()

    try:
        compose('up','-d','source','target')
        for service in ('source','target'):
            require(lambda s=service:sql(s,'SELECT 1')=='1',service+' ready')
        report['sourceVersion']=sql('source',"SHOW server_version")
        sql('source',"""
            CREATE SCHEMA sales; CREATE SCHEMA billing;
            CREATE TABLE sales.items(id BIGINT PRIMARY KEY,amount NUMERIC(20,4),active BOOLEAN,payload JSONB,data BYTEA);
            CREATE TABLE sales.duplicates(value INTEGER,payload JSONB);
            CREATE TABLE billing.ledger(id BIGINT PRIMARY KEY,label VARCHAR(60));
            ALTER TABLE sales.items REPLICA IDENTITY FULL;
            ALTER TABLE sales.duplicates REPLICA IDENTITY FULL;
            ALTER TABLE billing.ledger REPLICA IDENTITY FULL;
            INSERT INTO sales.items VALUES(1,123456789012.1234,true,'{"n":18446744073709551615}',decode('001aff','hex'));
            INSERT INTO sales.duplicates VALUES(7,'{"x":1}'),(7,'{"x":1}');
            INSERT INTO billing.ledger VALUES(1,'before');
            CREATE PUBLICATION rivomesh_test FOR TABLE sales.items,sales.duplicates,billing.ledger
            WITH (publish='insert,update,delete');
        """)
        if args.failover:
            compose('exec','-T','source','sh','-c',
                    'printf "\\nhost replication postgres samenet scram-sha-256\\n" >> "$PGDATA/pg_hba.conf"')
            assert sql('source','SELECT pg_reload_conf()')=='t'
            sql('source',"SELECT pg_create_physical_replication_slot('rivomesh_wal',true)")
            compose('up','-d','standby')
            require(lambda:sql('standby','SELECT pg_is_in_recovery()')=='t','physical standby ready')
            require(lambda:sql('source',"SELECT state FROM pg_stat_replication WHERE state='streaming'")=='streaming',
                    'standby streaming')
        metadata=json.loads(subprocess.check_output(['docker','image','inspect',args.source_image],text=True))[0]
        report['sourceImageId']=metadata['Id']
        if args.generated:
            sql('source',"""
                ALTER TABLE sales.items ADD COLUMN derived BIGINT GENERATED ALWAYS AS
                    (CASE WHEN active THEN id*2 ELSE id*3 END) STORED;
                ALTER TABLE sales.duplicates ADD COLUMN derived BIGINT GENERATED ALWAYS AS (value*4) STORED;
            """)
            check=compose('run','--rm','--no-deps','runner','check','/fixture/tasks/postgres_source.yml',check=False)
            assert check.returncode and 'POSTGRESQL_PUBLICATION_COLUMN_FILTER_UNSUPPORTED' in check.stderr
            sql('source',"ALTER PUBLICATION rivomesh_test SET (publish_generated_columns='stored')")
        preflight=compose('run','--rm','--no-deps','runner','check','/fixture/tasks/postgres_source.yml')
        assert json.loads(preflight.stdout)['sourceTables']==3
        compose('up','-d','runner')
        version=compose('exec','-T','runner','java','-version')
        matched=re.search(r'version "([^"]+)"',version.stderr)
        assert matched and matched.group(1).startswith(str(args.jdk)+'.')
        report['runtimeVersion']=matched.group(1)
        metadata=json.loads(subprocess.check_output(['docker','image','inspect',image],text=True))[0]
        report['runtimeImageId']=metadata['Id']
        report['runtimeArchitecture']=metadata['Architecture']
        require(stream_ready,'native consistent snapshot')
        initial=task()['status']
        snapshot_ids=sql('target','SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')
        assert len(snapshot_ids.splitlines())==3
        try:
            http(route+'/capture-recovery')
        except urllib.error.HTTPError as healthy:
            assert healthy.code==409
        else:
            raise AssertionError('Capture recovery review accepted a running pipeline')
        passed('PostgreSQL '+version_major+' consistent snapshot maps two schemas, JSONB, numeric, bytea and keyless duplicates')

        initial_lsn=initial['capture_lsn']
        sql('source','CREATE TABLE sales.unpublished(id INT); INSERT INTO sales.unpublished VALUES(1)')
        assert sql('source',f"SELECT pg_current_wal_lsn() > '{initial_lsn}'::pg_lsn")=='t'
        time.sleep(5)
        assert sql('source',f"SELECT confirmed_flush_lsn <= '{initial_lsn}'::pg_lsn "
                           "FROM pg_replication_slots WHERE plugin='pgoutput' AND NOT temporary")=='t'
        assert task()['status']['capture_lsn']==initial_lsn
        passed('Idle keepalives retain the original checkpoint as unpublished WAL advances')

        sql('source',"""BEGIN;
            UPDATE sales.items SET active=false,payload=jsonb_set(payload,'{updated}','true') WHERE id=1;
            UPDATE sales.duplicates SET value=9 WHERE ctid=(SELECT ctid FROM sales.duplicates WHERE value=7 LIMIT 1);
            UPDATE billing.ledger SET label='after' WHERE id=1;
            COMMIT""")
        require(lambda:equal() and task()['status'].get('capture_lsn')!=initial['capture_lsn'],'pgoutput transaction delivered')
        passed('A pgoutput transaction spans mapped schemas and updates exactly one keyless occurrence')
        if args.generated:
            assert sql('target','SELECT derived FROM copy_sales.items WHERE id=1')=='3'
            assert sql('target','SELECT derived FROM copy_sales.duplicates ORDER BY derived')=='28\n36'
            assert sql('target',"SELECT count(*) FROM information_schema.columns WHERE table_schema='copy_sales' "
                        "AND column_name='derived' AND is_generated='NEVER'")=='2'
            passed('Native stored generated columns materialize keyed and keyless updates, and missing publication coverage is refused')

        sql('target','ALTER TABLE copy_sales.items ADD CONSTRAINT test_stop CHECK(id<>77)')
        sql('source',"INSERT INTO sales.items VALUES(77,3.1415,true,'{}',decode('1234','hex'))")
        def failed_target():
            status=task()['status']
            sub=next((item for item in status.get('subscriptions',[]) if item['id']=='pg'),{})
            return sub if sub.get('failurePhase')=='TARGET_APPLY' and sub.get('state')=='PAUSED' else None
        failed=require(failed_target,'SQL fault classified')
        assert sql('target','SELECT COUNT(*) FROM copy_sales.items WHERE id=77')=='0'
        sql('target','ALTER TABLE copy_sales.items DROP CONSTRAINT test_stop')
        http(route+'/actions',dict(action='RESUME_TARGET',expectedRevision=task()['revision'],
                                   subscriptionId='pg',errorCount=failed['errorGeneration']))
        require(equal,'same pgoutput transaction replayed')
        passed('Destination SQL failure retains the typed source transaction and replays after explicit resume')

        previous=task()['status']['capture_lsn']
        compose('kill','-s','SIGKILL','runner')
        sql('source',"UPDATE billing.ledger SET label='offline commit' WHERE id=1; DELETE FROM sales.duplicates WHERE value=7")
        compose('up','-d','runner')
        require(lambda:stream_ready() and task()['status']['capture_lsn']!=previous,'exact LSN restart')
        assert sql('target','SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')==snapshot_ids
        durable=task()['status']['capture_lsn']
        slot=sql('source',"SELECT slot_name FROM pg_replication_slots WHERE plugin='pgoutput' AND NOT temporary")
        assert slot.startswith('rivomesh_test_') and len(slot.splitlines())==1
        passed('SIGKILL and restart reuse the original slot and snapshot with continuing LSN')

        if args.failover:
            def synchronized_slot():
                return sql('standby',f"""
                    SELECT synced AND failover AND restart_lsn <= '{durable}'::pg_lsn
                    AND confirmed_flush_lsn <= '{durable}'::pg_lsn
                    AND pg_last_wal_replay_lsn() >= '{durable}'::pg_lsn
                    FROM pg_replication_slots WHERE slot_name='{slot}'
                """)=='t'

            require(synchronized_slot,'logical failover slot synchronized and standby past durable LSN')
            system_id,original_timeline=source_timeline()
            report['checkpointBeforePromotion']=durable
            report['slotBeforePromotion']=slot
            report['timelineBeforePromotion']=original_timeline
            compose('kill','-s','SIGKILL','source')
            assert sql('standby','SELECT pg_promote(true,60)')=='t'
            live_source='standby'
            standby_id=compose('ps','-q','standby').stdout.strip()
            subprocess.run(['docker','network','connect','--alias','source-rw',project+'_access',standby_id],
                           check=True,timeout=30,capture_output=True)
            sql('source',"UPDATE billing.ledger SET label='promoted commit' WHERE id=1; "
                         "INSERT INTO sales.duplicates VALUES(7,'{}')")
            require(lambda:stream_ready() and task()['status']['capture_lsn']!=durable,'automatic promotion continuation')
            assert sql('target','SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')==snapshot_ids
            promoted_system_id,promoted_timeline=source_timeline()
            assert promoted_system_id==system_id and promoted_timeline>original_timeline
            assert sql('source',"SELECT count(*) FROM pg_replication_slots WHERE failover AND slot_name='"+slot+"'")=='1'
            durable=task()['status']['capture_lsn']
            report['checkpointAfterPromotion']=durable
            report['timelineAfterPromotion']=promoted_timeline
            passed('After primary SIGKILL, the promoted standby resumes the synchronized slot and reuses the snapshot')

        compose('kill','-s','SIGKILL','runner')
        if args.reject_old_primary:
            compose('stop','standby')
            compose('up','-d','source')
            live_source='source'
            require(lambda:sql('source','SELECT pg_is_in_recovery()')=='f','original server reopens on its branch')
            assert source_timeline()==(system_id,original_timeline)
            compose('up','-d','runner')

            def old_timeline_fault():
                status=task()['status']
                return status if status.get('error_code')=='POSTGRESQL_POSITION_LINEAGE_CHANGED' else None

            refused=require(old_timeline_fault,'previous timeline fails closed')
            assert refused['run_state']=='ERROR' and refused['capture_lsn']==durable
            assert sql('target',"SELECT label FROM copy_billing.ledger WHERE id=1")=='promoted commit'
            passed('Routing back to the old primary after promotion is refused and preserves the durable new-timeline checkpoint')
        else:
            require(lambda:sql('source',"SELECT active FROM pg_replication_slots WHERE slot_name='"+slot+"'")=='f','old slot disconnected')
            sql('source',"SELECT pg_drop_replication_slot('"+slot+"')")
            compose('up','-d','runner')

            def missing_slot_fault():
                status=task()['status']
                return status if status.get('error_code')=='POSTGRESQL_REQUIRED_WAL_UNAVAILABLE' and status.get('run_state')=='ERROR' else None

            missing=require(missing_slot_fault,'lost slot fails closed')
            assert missing['capture_lsn']==durable
            assert sql('source',"SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name='"+slot+"'")=='0'
            passed('The missing slot is refused and the last durable LSN remains available')
            recovery=http(route+'/capture-recovery')
            assert recovery['sourceAdmission']=='POSTGRESQL_REQUIRED_WAL_UNAVAILABLE'
            assert recovery['phase']=='SOURCE_CONNECT' and recovery['scope']=='UNPROVEN_SOURCE_RANGE'
            assert recovery['affectedSubscriptions']==['pg'] and len(recovery['checkpointHash'])==64
            assert recovery['actions']==['PRESERVE_SOURCE_LOGS','RESTORE_LOGS_OR_SELECT_ELIGIBLE_SOURCE','REVIEW_NEW_BASELINE']
            assert task()['status']['capture_lsn']==durable
            assert sql('source',"SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name='"+slot+"'")=='0'
            passed('Capture recovery explains the missing slot and affected subscriptions while retaining the checkpoint')
        assert hashlib.sha256(artifact.read_bytes()).hexdigest()==digest
        report['status']='PASS'
    except Exception as error:
        report['status']='FAIL'
        log_services=('runner','standby','source') if args.failover else ('runner',)
        log=compose('logs','--no-color','--tail','80',*log_services,check=False).stdout
        worker_log=home/'platform'/'logs'/'postgres_source.log'
        if worker_log.is_file():
            log+='\n'+'\n'.join(worker_log.read_text(errors='replace').splitlines()[-80:])
        if isinstance(error,subprocess.CalledProcessError):
            log+='\n'+(error.stdout or '')+'\n'+(error.stderr or '')
        for key in ('RM_PG_PASSWORD','RM_PG_TOKEN'):
            log=log.replace(environment[key],'[REDACTED]')
        (output/'runner-redacted.log').write_text(log)
        raise
    finally:
        cleanup=compose('down','-v','--remove-orphans',check=False)
        report['cleanupExitCode']=cleanup.returncode
        (output/'manifest.json').write_text(json.dumps(report,indent=2)+'\n')
        if cleanup.returncode:
            raise RuntimeError('Test fixture cleanup failed')
