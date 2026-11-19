#!/usr/bin/env python3
"""Native PostgreSQL source snapshot, pgoutput, restart and missing-slot acceptance."""
import argparse
from datetime import datetime, timedelta
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
parser.add_argument('--uuid', action='store_true', help='Exercise UUID keys and nullable UUID values')
temporal = parser.add_mutually_exclusive_group()
temporal.add_argument('--date-values', action='store_true', help='Exercise civil DATE through PostgreSQL and MySQL targets')
temporal.add_argument('--timestamp-values', action='store_true', help='Exercise civil and UTC timestamps through both targets')
temporal.add_argument('--clock-values', action='store_true', help='Exercise PostgreSQL clock values, including 24:00')
parser.add_argument('--delivery-timing', action='store_true', help='Verify exact native commit time and delivery observation')
parser.add_argument('--ddl-review', action='store_true', help='Review native publication drift without advancing the cut')
parser.add_argument('--tls', action='store_true',
                    help='Require CA and host verification for SQL and pgoutput and exercise rejected certificates')
parser.add_argument('--failover', action='store_true',
                    help='Exercise a synchronized logical slot through physical standby promotion')
parser.add_argument('--reject-old-primary', action='store_true',
                    help='After promotion, route back to the stopped original primary instead of dropping the slot')
parser.add_argument('--jdk', type=int, choices=(21,), default=21)
args = parser.parse_args()
temporal_targets = args.date_values or args.timestamp_values or args.clock_values
if args.reject_old_primary and not args.failover:
    parser.error('--reject-old-primary requires --failover')
if args.tls and args.failover:
    parser.error('--tls verifies the direct source and target endpoints. Use --failover for the promotion profile')
if args.ddl_review and (args.failover or temporal_targets):
    parser.error('--ddl-review is an independent native failure profile')
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
              sourceImage=args.source_image,failover=args.failover,rejectOldPrimary=args.reject_old_primary,
              uuid=args.uuid,dateValues=args.date_values,timestampValues=args.timestamp_values,
              clockValues=args.clock_values,deliveryTiming=args.delivery_timing,tls=args.tls)
if args.ddl_review:
    report.update(ddlReview=True,harnessSha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
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


def certificates(directory):
    trust=directory/'trust'
    trust.mkdir()

    def openssl(*command):
        subprocess.run(['openssl',*command],cwd=directory,check=True,capture_output=True,timeout=30)

    for name in ('accepted','untrusted'):
        openssl('req','-new','-x509','-nodes','-newkey','rsa:2048','-sha256','-days','2',
                '-subj',f'/CN=Rivomesh test {name} authority',
                '-addext','basicConstraints=critical,CA:TRUE',
                '-addext','keyUsage=critical,keyCertSign,cRLSign',
                '-keyout',name+'.key','-out','trust/'+name+'.pem')
    for name in ('source','target'):
        service=directory/name
        service.mkdir()
        openssl('req','-new','-nodes','-newkey','rsa:2048','-sha256','-subj','/CN='+name,
                '-keyout',name+'/server.key','-out',name+'.csr')
        extension=directory/(name+'.cnf')
        extension.write_text(f'basicConstraints=critical,CA:FALSE\nsubjectAltName=DNS:{name}\n'
                             'keyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n')
        openssl('x509','-req','-sha256','-days','2','-in',name+'.csr',
                '-CA','trust/accepted.pem','-CAkey','accepted.key','-CAcreateserial',
                '-extfile',extension.name,'-out',name+'/server.crt')
        (service/'server.key').chmod(0o600)
    return trust


with (tempfile.TemporaryDirectory(prefix='pg-source-',dir=ROOT/'target') as temporary,
      tempfile.TemporaryDirectory(prefix='pg-source-tls-',dir=ROOT/'target') as private_tls):
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
    if temporal_targets:
        pipeline['targets'].append(dict(
            id='mysql',product='MYSQL',
            url='jdbc:mysql://mysql:3306/?connectTimeout=3000&sslMode=DISABLED&allowPublicKeyRetrieval=true',
            username='root',password='${RM_PG_PASSWORD}',mappings=dict(sales='copy_sales',billing='copy_billing'),
            metadataSchema='copy_meta'))
    live_source='source'
    platform=dict(bind='0.0.0.0',port=8080,authToken='${RM_PG_TOKEN}',tasksDirectory='/fixture/tasks',
                  dataDirectory='/fixture/platform',autoStart=True,pollMillis=200,workerHeapMiB=256)
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
    if temporal_targets:
        services['source']['command']+=['-c','datestyle=SQL, DMY']
        services['runner']['environment']['TZ']='Pacific/Apia' if args.date_values else 'America/Los_Angeles'
        services['mysql']=dict(image='mysql:8.4',environment=dict(
            MYSQL_ROOT_PASSWORD='${RM_PG_PASSWORD}',MYSQL_ROOT_HOST='%'))
    if args.timestamp_values or args.clock_values:
        services['source']['command']+=['-c','timezone=America/New_York']
        report['harnessSha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    if args.tls:
        private_certificates=Path(private_tls)
        trust=certificates(private_certificates)
        services['target']['image']='postgres:17.12'
        for service in ('source','target'):
            instance=services[service]
            instance['command']=instance.get('command',[])+[
                '-c','ssl=on','-c','ssl_min_protocol_version=TLSv1.3',
                '-c','ssl_cert_file=/run/rivomesh-tls/server.crt',
                '-c','ssl_key_file=/run/rivomesh-tls/server.key']
            instance['entrypoint']=[
                'bash','-ceu',
                'install -d -o postgres -g postgres -m 0700 /run/rivomesh-tls\n'
                'install -o postgres -g postgres -m 0600 /tls-fixture/server.key /run/rivomesh-tls/server.key\n'
                'install -o postgres -g postgres -m 0644 /tls-fixture/server.crt /run/rivomesh-tls/server.crt\n'
                'exec docker-entrypoint.sh "$@"','rivomesh-fixture']
            instance['volumes']=[str(private_certificates/service)+':/tls-fixture:ro']
        services['source']['networks']={'default':{'aliases':['source-wrong-name']}}
        services['runner']['volumes'].append(str(trust)+':/trust:ro')
        tls_options='&sslmode=verify-full&sslrootcert=/trust/accepted.pem&channelBinding=require'
        pipeline['source']['postgresql']['url']+=tls_options+'&ApplicationName=rivomesh-tls-source'
        pipeline['targets'][0]['url']+=tls_options+'&ApplicationName=rivomesh-tls-target'
        report['tlsProfile']={'driver':'pgJDBC 42.7.14','protocol':'TLSv1.3',
                              'sslmode':'verify-full','channelBinding':'require'}
    (tasks/'postgres_source.yml').write_text(json.dumps(pipeline))
    if args.delivery_timing:
        services['source']['command']+=['-c','track_commit_timestamp=on']
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
            '-c max_replication_slots=8 -c hot_standby_feedback=on -c sync_replication_slots=on'
            + (' -c track_commit_timestamp=on' if args.delivery_timing else ''),
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

    def mysql(statement):
        return compose('exec','-T','mysql','sh','-c',
                       'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -NB -e "$1"',
                       'rivomesh-date',statement).stdout.strip()

    def date_rows():
        key=("SELECT TO_CHAR(event_date,'YYYY-MM-DD'),COALESCE(TO_CHAR(follow_up,'YYYY-MM-DD'),'<NULL>') "
             "FROM {schema}.date_key ORDER BY event_date")
        keyless=("SELECT COALESCE(TO_CHAR(event_date,'YYYY-MM-DD'),'<NULL>') "
                 "FROM {schema}.date_keyless ORDER BY event_date NULLS LAST")
        source=(sql('source',key.format(schema='billing')),sql('source',keyless.format(schema='sales')))
        pg=(sql('target',key.format(schema='copy_billing')),sql('target',keyless.format(schema='copy_sales')))
        my=(mysql("SELECT CONCAT(CAST(event_date AS CHAR),'|',COALESCE(CAST(follow_up AS CHAR),'<NULL>')) "
                  "FROM copy_billing.date_key ORDER BY event_date"),
            mysql("SELECT COALESCE(CAST(event_date AS CHAR),'<NULL>') "
                  "FROM copy_sales.date_keyless ORDER BY event_date IS NULL,event_date"))
        return source,pg,my

    def timestamp_rows():
        pg_format='YYYY-MM-DD HH24:MI:SS.US'
        my_format='%Y-%m-%d %H:%i:%s.%f'

        def postgres(service,billing,sales,instant):
            key=(f"SELECT TO_CHAR(civil,'{pg_format}'),COALESCE(TO_CHAR({instant},'{pg_format}'),'<NULL>') "
                 f"FROM {billing}.time_key ORDER BY civil")
            keyless=(f"SELECT COALESCE(TO_CHAR({instant},'{pg_format}'),'<NULL>') "
                     f"FROM {sales}.time_keyless ORDER BY seen NULLS LAST")
            return sql(service,key),sql(service,keyless)

        source=postgres('source','billing','sales',"seen AT TIME ZONE 'UTC'")
        pg=postgres('target','copy_billing','copy_sales','seen')
        my=(mysql(f"SELECT CONCAT(DATE_FORMAT(civil,'{my_format}'),'|',COALESCE(DATE_FORMAT(seen,'{my_format}'),'<NULL>')) "
                  "FROM copy_billing.time_key ORDER BY civil"),
            mysql(f"SELECT COALESCE(DATE_FORMAT(seen,'{my_format}'),'<NULL>') "
                  "FROM copy_sales.time_keyless ORDER BY seen IS NULL,seen"))
        return source,pg,my

    def clock_rows():
        def microseconds(value):
            if value=='<NULL>':
                return value
            clock,_,fraction=value.partition('.')
            hour,minute,second=map(int,clock.split(':'))
            return str((hour*3600+minute*60+second)*1000000+int(fraction.ljust(6,'0')))

        def read(query, billing, sales, slot, seen, null_order):
            key=query(f"SELECT {slot},COALESCE({seen},'<NULL>') FROM {billing}.clock_key ORDER BY slot")
            keyless=query(f"SELECT COALESCE({seen},'<NULL>') FROM {sales}.clock_keyless ORDER BY {null_order}")
            return ('\n'.join('|'.join(map(microseconds,row.split('|'))) for row in key.splitlines()),
                    '\n'.join(map(microseconds,keyless.splitlines())))

        return (read(lambda query:sql('source',query),'billing','sales','slot::text','seen::text','seen NULLS LAST'),
                read(lambda query:sql('target',query),'copy_billing','copy_sales','slot::text','seen::text','seen NULLS LAST'),
                read(lambda query:mysql(query).replace('\t','|'),'copy_billing','copy_sales',
                     'CAST(slot AS CHAR)','CAST(seen AS CHAR)','seen IS NULL,seen'))

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
        tables=[
            ("SELECT id,amount,active,payload::text,encode(data,'hex')"+stored+" FROM {schema}.items ORDER BY id","sales"),
            ("SELECT value,payload::text"+stored+" FROM {schema}.duplicates ORDER BY value,payload::text","sales"),
            ("SELECT id,label FROM {schema}.ledger ORDER BY id","billing")]
        if args.uuid:
            tables.append(("SELECT id,ref FROM {schema}.identities ORDER BY id","billing"))
        for query,schema in tables:
            if sql('source',query.format(schema=schema))!=sql('target',query.format(schema='copy_'+schema)):
                return False
        if temporal_targets:
            temporal_rows=clock_rows if args.clock_values else timestamp_rows if args.timestamp_values else date_rows
            source,pg,my=temporal_rows()
            return source==pg==my
        return True

    def stream_ready():
        view=task()
        return view['sourceType']=='POSTGRESQL' and view['status'].get('capture_lsn') and equal()

    def active_tls(service,application):
        return sql(service,"SELECT count(*) FROM pg_stat_activity a JOIN pg_stat_ssl s USING (pid) "
                   f"WHERE a.application_name='{application}' AND s.ssl AND s.version='TLSv1.3'")

    def untrusted_endpoint(name, edit, message):
        case=json.loads(json.dumps(pipeline))
        edit(case)
        (home/(name+'.yml')).write_text(json.dumps(case))
        result=compose('run','--rm','--no-deps','runner','check','/fixture/'+name+'.yml',check=False)
        error=result.stderr.lower()
        assert result.returncode!=0 and any(fragment in error for fragment in message), f'{name} was not rejected as expected'

    try:
        endpoints=['source','target']+(['mysql'] if temporal_targets else [])
        compose('up','-d',*endpoints)
        for service in ('source','target'):
            require(lambda s=service:sql(s,'SELECT 1')=='1',service+' ready')
        report['sourceVersion']=sql('source',"SHOW server_version")
        if temporal_targets:
            report['mysqlVersion']=require(lambda:mysql('SELECT VERSION()'),'MySQL target ready')
            report['postgresTargetVersion']=sql('target','SHOW server_version')
            assert sql('source','SHOW DateStyle')=='SQL, DMY'
        if args.timestamp_values:
            assert sql('source','SHOW TimeZone')=='America/New_York'
        if args.tls:
            report['targetVersion']=sql('target',"SHOW server_version")
            report['harnessSha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
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
        if args.uuid:
            sql('source',"""
                CREATE TABLE billing.identities(id UUID PRIMARY KEY,ref UUID);
                ALTER TABLE billing.identities REPLICA IDENTITY FULL;
                INSERT INTO billing.identities VALUES
                    ('A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11',NULL),
                    ('ffffffff-ffff-ffff-ffff-ffffffffffff','00000000-0000-0000-0000-000000000000');
                ALTER PUBLICATION rivomesh_test ADD TABLE billing.identities;
            """)
        if args.date_values:
            sql('source',"""
                CREATE TABLE billing.date_key(event_date DATE PRIMARY KEY,follow_up DATE);
                CREATE TABLE sales.date_keyless(event_date DATE);
                ALTER TABLE billing.date_key REPLICA IDENTITY FULL;
                ALTER TABLE sales.date_keyless REPLICA IDENTITY FULL;
                INSERT INTO billing.date_key VALUES (DATE '1000-01-01',NULL),
                    (DATE '1582-10-10',DATE '2024-02-29'),(DATE '2011-12-30',DATE '9999-12-31');
                INSERT INTO sales.date_keyless VALUES
                    (DATE '2011-12-30'),(DATE '2011-12-30'),(DATE '2011-12-31'),(NULL);
                ALTER PUBLICATION rivomesh_test ADD TABLE billing.date_key,sales.date_keyless;
            """)
        if args.timestamp_values:
            sql('source',"""
                CREATE TABLE billing.time_key(civil TIMESTAMP(6) PRIMARY KEY,seen TIMESTAMPTZ(6));
                CREATE TABLE sales.time_keyless(seen TIMESTAMPTZ(6));
                ALTER TABLE billing.time_key REPLICA IDENTITY FULL;
                ALTER TABLE sales.time_keyless REPLICA IDENTITY FULL;
                INSERT INTO billing.time_key VALUES
                    ('1000-01-01 00:00:00',NULL),
                    ('2024-03-10 02:30:00.123456','2024-11-03 01:30:00.123456-04'),
                    ('2024-11-03 01:30:00.654321','2024-11-03 01:30:00.654321-05'),
                    ('9999-12-31 23:59:59.499999','1999-12-31 23:59:59.999999+00');
                INSERT INTO sales.time_keyless VALUES
                    ('2024-11-03 01:30:00.123456-04'),('2024-11-03 05:30:00.123456+00'),
                    ('2024-11-03 01:30:00.654321-05'),(NULL);
                ALTER PUBLICATION rivomesh_test ADD TABLE billing.time_key,sales.time_keyless;
            """)
        if args.clock_values:
            sql('source',"""
                CREATE TABLE billing.clock_key(slot TIME(6) PRIMARY KEY,seen TIME(6));
                CREATE TABLE sales.clock_keyless(seen TIME(6));
                ALTER TABLE billing.clock_key REPLICA IDENTITY FULL;
                ALTER TABLE sales.clock_keyless REPLICA IDENTITY FULL;
                INSERT INTO billing.clock_key VALUES
                    ('00:00:00',NULL),('12:34:56.123456','23:59:59.999999'),('24:00:00','00:00:00');
                INSERT INTO sales.clock_keyless VALUES ('12:34:56.123456'),('12:34:56.123456'),('24:00:00'),(NULL);
                ALTER PUBLICATION rivomesh_test ADD TABLE billing.clock_key,sales.clock_keyless;
                CREATE TABLE sales.zoned_clock(seen TIMETZ);
                ALTER TABLE sales.zoned_clock REPLICA IDENTITY FULL;
                ALTER PUBLICATION rivomesh_test ADD TABLE sales.zoned_clock;
            """)
            denied=compose('run','--rm','--no-deps','runner','check','/fixture/tasks/postgres_source.yml',check=False)
            assert denied.returncode!=0 and 'POSTGRESQL_TYPE_UNSUPPORTED' in denied.stderr
            assert sql('source',"SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput' AND NOT temporary")=='0'
            sql('source','DROP TABLE sales.zoned_clock')
            passed('TIMETZ is refused by native discovery before creating a logical slot')
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
        if args.tls:
            def wrong_source_name(case):
                options=case['source']['postgresql']
                options['url']=options['url'].replace('://source:', '://source-wrong-name:')

            def wrong_target_authority(case):
                target=case['targets'][0]
                target['url']=target['url'].replace('/trust/accepted.pem','/trust/untrusted.pem')

            untrusted_endpoint('wrong-source-name',wrong_source_name,('hostname', 'host name'))
            untrusted_endpoint('wrong-target-authority',wrong_target_authority,('certification path', 'pkix', 'trust'))
            passed('The normal preflight refuses a mismatched source hostname and an untrusted target certificate')
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
        table_count=3+int(args.uuid)+2*int(temporal_targets)
        assert json.loads(preflight.stdout)['sourceTables']==table_count
        compose('up','-d','runner')
        version=compose('exec','-T','runner','java','-version')
        matched=re.search(r'version "([^"]+)"',version.stderr)
        assert matched and matched.group(1).startswith(str(args.jdk)+'.')
        report['runtimeVersion']=matched.group(1)
        metadata=json.loads(subprocess.check_output(['docker','image','inspect',image],text=True))[0]
        report['runtimeImageId']=metadata['Id']
        report['runtimeArchitecture']=metadata['Architecture']
        require(stream_ready,'native consistent snapshot')
        if args.tls:
            require(lambda: int(active_tls('source','rivomesh-tls-source'))>0
                    and int(active_tls('target','rivomesh-tls-target'))>0,
                    'native TLS session observation on both endpoints')
            assert sql('source',"SELECT count(*) FROM pg_stat_replication r JOIN pg_stat_ssl s USING (pid) "
                       "WHERE r.application_name='rivomesh-tls-source' AND s.ssl AND s.version='TLSv1.3'")=='1'
            passed('The native views confirm TLS 1.3 for the packaged logical replication and destination sessions')
        initial=task()['status']
        snapshot_ids=sql('target','SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')
        assert len(snapshot_ids.splitlines())==table_count
        try:
            http(route+'/capture-recovery')
        except urllib.error.HTTPError as healthy:
            assert healthy.code==409
        else:
            raise AssertionError('Capture recovery review accepted a running pipeline')
        passed('PostgreSQL '+version_major+' consistent snapshot maps two schemas, JSONB, numeric, bytea and keyless duplicates')
        if temporal_targets:
            mysql_snapshots=mysql('SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')
            assert len(mysql_snapshots.splitlines())==table_count
            temporal_slot=sql('source',"SELECT slot_name FROM pg_replication_slots WHERE plugin='pgoutput' AND NOT temporary")
        if args.date_values:
            baseline_dates=('1000-01-01|<NULL>\n1582-10-10|2024-02-29\n2011-12-30|9999-12-31',
                            '2011-12-30\n2011-12-30\n2011-12-31\n<NULL>')
            assert date_rows()==(baseline_dates,baseline_dates,baseline_dates)
            passed('DATE snapshots agree in PostgreSQL and MySQL under SQL DateStyle and a skipped JVM local day')
        if args.timestamp_values:
            baseline_times=(
                '\n'.join(('1000-01-01 00:00:00.000000|<NULL>',
                           '2024-03-10 02:30:00.123456|2024-11-03 05:30:00.123456',
                           '2024-11-03 01:30:00.654321|2024-11-03 06:30:00.654321',
                           '9999-12-31 23:59:59.499999|1999-12-31 23:59:59.999999')),
                '\n'.join(('2024-11-03 05:30:00.123456','2024-11-03 05:30:00.123456',
                           '2024-11-03 06:30:00.654321','<NULL>')))
            assert timestamp_rows()==(baseline_times,baseline_times,baseline_times)
            assert sql('target',"SELECT COUNT(*) FROM information_schema.columns "
                                "WHERE table_name IN ('time_key','time_keyless') "
                                "AND data_type='timestamp without time zone' AND datetime_precision=6")=='3'
            assert mysql("SELECT COUNT(*) FROM information_schema.columns "
                         "WHERE table_name IN ('time_key','time_keyless') "
                         "AND data_type='datetime' AND datetime_precision=6")=='3'
            passed('Both snapshots preserve civil microseconds, distinct DST instants as UTC and SQL NULL')
        if args.clock_values:
            original=('0|<NULL>\n45296123456|86399999999\n86400000000|0',
                      '45296123456\n45296123456\n86400000000\n<NULL>')
            assert clock_rows()==(original,original,original)
            assert sql('target',"SELECT COUNT(*) FROM information_schema.columns "
                                "WHERE table_name IN ('clock_key','clock_keyless') "
                                "AND data_type='time without time zone' AND datetime_precision=6")=='3'
            assert mysql("SELECT COUNT(*) FROM information_schema.columns "
                         "WHERE table_name IN ('clock_key','clock_keyless') "
                         "AND data_type='time' AND datetime_precision=6")=='3'
            passed('Both TIME(6) snapshots distinguish 00:00 and 24:00 and retain microseconds and NULL')

        if args.delivery_timing:
            assert initial['subscriptions'][0]['lastDeliveryTiming']=={'state':'UNKNOWN'}
            xid=sql('source',"INSERT INTO billing.ledger VALUES(15,'delivery observation') RETURNING xmin").splitlines()[0]
            assert xid.isdecimal()
            utc_commit=f"SELECT pg_xact_commit_timestamp('{xid}'::xid) AT TIME ZONE 'UTC'"
            source_time=datetime.fromisoformat(sql('source',utc_commit)+"+00:00")

            def observed_timing():
                subscription=task()['status']['subscriptions'][0]
                sample=subscription.get('lastDeliveryTiming',{})
                return sample if sample.get('state')=='OBSERVED' and subscription['pendingTransactions']=='0' else None

            timing=require(observed_timing,'target SQL commit observation reaches the journal')
            assert sql('target','SELECT label FROM copy_billing.ledger WHERE id=15')=='delivery observation'
            assert sql('target','SELECT stream_seq FROM copy_meta.rm3_sink_job')==timing['sequence']
            assert datetime.fromisoformat(timing['sourceCommittedAt'])==source_time
            confirmed=datetime.fromisoformat(timing['targetCommitObservedAt'])
            assert int(timing['millis'])==(confirmed-source_time)//timedelta(milliseconds=1)>=0
            report['deliveryTimingSample']=timing
            compose('kill','-s','SIGKILL','runner')
            compose('up','-d','runner')
            require(stream_ready,'Worker and controller serve original checkpoints after restart')
            assert observed_timing()==timing
            passed('Pgoutput timestamp matches native commit metadata and direct SQL confirmation persists across a process restart')
            initial=task()['status']

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
        if args.uuid:
            sql('source',"""
                BEGIN;
                UPDATE billing.identities SET id='A0EEBC999C0B4EF8BB6D6BB9BD380A12',
                    ref='ffffffff-ffff-ffff-ffff-ffffffffffff'
                    WHERE id='a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11';
                DELETE FROM billing.identities WHERE id='ffffffff-ffff-ffff-ffff-ffffffffffff';
                INSERT INTO billing.identities VALUES ('00000000-0000-0000-0000-000000000000',NULL);
                COMMIT;
            """)
            require(equal,'UUID keys and nullable values delivered')
            assert sql('target',"""
                SELECT count(*) FROM information_schema.columns
                 WHERE table_schema='copy_billing' AND table_name='identities'
                   AND data_type='character varying' AND character_maximum_length=36
            """)=='2'
            passed('UUID values preserve canonical text, NULL, primary-key changes, and deletes in VARCHAR(36)')
        if args.generated:
            assert sql('target','SELECT derived FROM copy_sales.items WHERE id=1')=='3'
            assert sql('target','SELECT derived FROM copy_sales.duplicates ORDER BY derived')=='28\n36'
            assert sql('target',"SELECT count(*) FROM information_schema.columns WHERE table_schema='copy_sales' "
                        "AND column_name='derived' AND is_generated='NEVER'")=='2'
            passed('Native stored generated columns materialize keyed and keyless updates, and missing publication coverage is refused')
        if args.date_values:
            sql('source',"""
                BEGIN;
                UPDATE billing.date_key SET event_date=DATE '9999-12-31',follow_up=DATE '2024-02-29'
                    WHERE event_date=DATE '1000-01-01';
                DELETE FROM sales.date_keyless WHERE ctid=(
                    SELECT ctid FROM sales.date_keyless WHERE event_date=DATE '2011-12-30' LIMIT 1);
                COMMIT;
            """)
            require(equal,'DATE key change and exact keyless occurrence delivered to both targets')
            assert date_rows()[0]==('1582-10-10|2024-02-29\n2011-12-30|9999-12-31\n9999-12-31|2024-02-29',
                                    '2011-12-30\n2011-12-31\n<NULL>')
            passed('DATE redo changes a primary key and deletes one exact duplicate without changing the adjacent local date')
        if args.timestamp_values:
            sql('source',"""
                BEGIN;
                UPDATE billing.time_key SET civil='2024-03-10 02:30:00.123457',seen='2024-03-10 01:59:59.999999-05'
                    WHERE civil='2024-03-10 02:30:00.123456';
                DELETE FROM sales.time_keyless WHERE ctid=(
                    SELECT ctid FROM sales.time_keyless WHERE seen='2024-11-03 05:30:00.123456+00' LIMIT 1);
                COMMIT;
            """)
            require(equal,'timestamp key change and exact keyless occurrence delivered to both targets')
            keyed,keyless=timestamp_rows()[0]
            assert '2024-03-10 02:30:00.123457|2024-03-10 06:59:59.999999' in keyed
            assert keyless.splitlines()==['2024-11-03 05:30:00.123456','2024-11-03 06:30:00.654321','<NULL>']
            passed('Native redo changes a civil microsecond key and removes one exact UTC keyless occurrence')
        if args.clock_values:
            sql('source',"""
                BEGIN;
                UPDATE billing.clock_key SET slot='12:34:56.123457',seen='00:00:00.000001' WHERE slot='12:34:56.123456';
                DELETE FROM sales.clock_keyless WHERE ctid=(
                    SELECT ctid FROM sales.clock_keyless WHERE seen='12:34:56.123456' LIMIT 1);
                COMMIT;
            """)
            require(equal,'clock key change and exact keyless occurrence delivered to both targets')
            assert clock_rows()[0]==('0|<NULL>\n45296123457|1\n86400000000|0', '45296123456\n86400000000\n<NULL>')
            passed('Native clock redo changes a one-microsecond key and removes one exact keyless occurrence')

        if args.delivery_timing:
            last_success=require(observed_timing,'last SQL commit observed before target fault')
        sql('target','ALTER TABLE copy_sales.items ADD CONSTRAINT test_stop CHECK(id<>77)')
        sql('source',"INSERT INTO sales.items VALUES(77,3.1415,true,'{}',decode('1234','hex'))")
        def failed_target():
            status=task()['status']
            sub=next((item for item in status.get('subscriptions',[]) if item['id']=='pg'),{})
            return sub if sub.get('failurePhase')=='TARGET_APPLY' and sub.get('state')=='PAUSED' else None
        failed=require(failed_target,'SQL fault classified')
        assert sql('target','SELECT COUNT(*) FROM copy_sales.items WHERE id=77')=='0'
        if args.delivery_timing:
            assert int(failed['pendingTransactions'])>0 and failed['lastDeliveryTiming']==last_success
        sql('target','ALTER TABLE copy_sales.items DROP CONSTRAINT test_stop')
        http(route+'/actions',dict(action='RESUME_TARGET',expectedRevision=task()['revision'],
                                   subscriptionId='pg',errorCount=failed['errorGeneration']))
        require(equal,'same pgoutput transaction replayed')
        passed('Destination SQL failure retains the typed source transaction and replays after explicit resume')
        if args.delivery_timing:
            restored=require(observed_timing,'resumed target records a new direct apply observation')
            assert int(restored['sequence'])>int(last_success['sequence'])
            passed('A paused destination keeps its previous sample until the failed transaction directly commits')

        previous=task()['status']['capture_lsn']
        compose('kill','-s','SIGKILL','runner')
        sql('source',"UPDATE billing.ledger SET label='offline commit' WHERE id=1; DELETE FROM sales.duplicates WHERE value=7")
        if args.date_values:
            sql('source',"""
                BEGIN;
                INSERT INTO billing.date_key VALUES(DATE '1000-01-01',NULL);
                UPDATE sales.date_keyless SET event_date=DATE '1582-10-10' WHERE event_date=DATE '2011-12-30';
                COMMIT;
            """)
        if args.timestamp_values:
            sql('source',"INSERT INTO billing.time_key VALUES('1582-10-10 05:06:07.000008','2024-03-10 03:00:00.000001-04'); "
                         "UPDATE sales.time_keyless SET seen='2000-02-29 00:00:00.000001+00' WHERE seen='2024-11-03 05:30:00.123456+00'")
        if args.clock_values:
            sql('source',"UPDATE billing.clock_key SET slot='23:59:59.999999' WHERE slot='24:00:00'; "
                         "INSERT INTO sales.clock_keyless VALUES('00:00:00.000001')")
        compose('up','-d','runner')
        require(lambda:stream_ready() and task()['status']['capture_lsn']!=previous,'exact LSN restart')
        assert sql('target','SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')==snapshot_ids
        durable=task()['status']['capture_lsn']
        slot=sql('source',"SELECT slot_name FROM pg_replication_slots WHERE plugin='pgoutput' AND NOT temporary")
        assert slot.startswith('rivomesh_test_') and len(slot.splitlines())==1
        passed('SIGKILL and restart reuse the original slot and snapshot with continuing LSN')
        if temporal_targets:
            assert slot==temporal_slot
            assert mysql_snapshots==mysql('SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')
        if args.date_values:
            assert date_rows()[0]==(
                '1000-01-01|<NULL>\n1582-10-10|2024-02-29\n2011-12-30|9999-12-31\n9999-12-31|2024-02-29',
                '1582-10-10\n2011-12-31\n<NULL>')
            passed('DATE catch-up after SIGKILL reuses the source slot and both sets of destination snapshot IDs')
        if args.timestamp_values:
            keyed,keyless=timestamp_rows()[0]
            assert '1582-10-10 05:06:07.000008|2024-03-10 07:00:00.000001' in keyed
            assert '2000-02-29 00:00:00.000001' in keyless
            passed('Timestamp catch-up after SIGKILL reuses the slot and both original destination snapshot sets')
        if args.clock_values:
            assert clock_rows()[0]==('0|<NULL>\n45296123457|1\n86399999999|0',
                                     '1\n45296123456\n86400000000\n<NULL>')
            passed('Clock catch-up after SIGKILL reuses the original slot and both target snapshot sets')
        if args.tls:
            require(lambda: int(active_tls('source','rivomesh-tls-source'))>0
                    and int(active_tls('target','rivomesh-tls-target'))>0,
                    'TLS sessions restored with original slot and destination data')
            passed('Process restart re-establishes verified TLS and retains the original slot, snapshots and exact rows')

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

        if args.ddl_review:
            def target_cut():
                return (sql('target','SELECT stream_seq FROM copy_meta.rm3_sink_job'),
                        sql('target','SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id'),
                        sql('target',"SELECT id,label FROM copy_billing.ledger ORDER BY id"),
                        sql('target',"SELECT column_name FROM information_schema.columns WHERE table_schema='copy_billing' "
                                     "AND table_name='ledger' ORDER BY ordinal_position"))

            original=target_cut()
            sql('source',"ALTER TABLE billing.ledger ADD COLUMN first_note TEXT; "
                         "INSERT INTO billing.ledger VALUES(2,'unapplied','pending')")

            def ddl_fault():
                status=task()['status']
                return status if status.get('error_stage')=='DDL' and status.get('run_state')=='ERROR' else None

            refused=require(ddl_fault,'changed Relation stops at the intact logical cut')
            assert refused['error_code']=='POSTGRESQL_RELATION_CHANGED_REQUIRES_PLAN'
            assert refused['capture_lsn']==durable and target_cut()==original
            first=http(route+'/capture-recovery')
            assert first['sourceAdmission']=='SOURCE_CHECKPOINT_AVAILABLE' and first['phase']=='DDL'
            assert first['affectedSubscriptions']==['pg'] and first['scope']=='UNPROVEN_SOURCE_RANGE'
            diff=first['schemaDrift']
            assert diff['scope']=='CURRENT_POSTGRESQL_PUBLICATION_VS_JOURNAL' and diff['changedTables']==1
            table,=diff['tables']
            assert (table['database'],table['table'],table['change'])==('billing','ledger','TABLE_CHANGED')
            assert table['columns']['added']==['first_note']
            passed('Changed native Relation fails closed and review identifies its current publication shape')

            sql('source','ALTER TABLE billing.ledger ADD COLUMN second_note VARCHAR(30)')
            later=http(route+'/capture-recovery')
            current,=later['schemaDrift']['tables']
            assert current['columns']['added']==['first_note','second_note']
            assert current['recordedSha256']==table['recordedSha256'] and current['liveSha256']!=table['liveSha256']
            assert later['checkpointHash']==first['checkpointHash'] and later['errorGeneration']==first['errorGeneration']
            passed('A later DDL changes the advisory live digest while preserving the recorded schema and failure cut')

            sql('source','ALTER TABLE billing.ledger ADD COLUMN out_of_profile TIMETZ')
            unavailable=http(route+'/capture-recovery')
            assert unavailable['schemaDrift']=={'scope':'CURRENT_POSTGRESQL_PUBLICATION_VS_JOURNAL',
                                               'state':'UNAVAILABLE','code':'POSTGRESQL_TYPE_UNSUPPORTED'}
            assert unavailable['checkpointHash']==first['checkpointHash'] and task()['status']['capture_lsn']==durable
            assert target_cut()==original
            passed('An unsupported publication shape has an explicit unavailable diagnostic without changing either cut')
        elif args.date_values:
            before_date_fault=task()['status']
            finished_dates=date_rows()
            sql('source',"""
                BEGIN;
                INSERT INTO billing.date_key VALUES(DATE '2025-01-01',NULL);
                INSERT INTO sales.date_keyless VALUES('infinity'::date);
                COMMIT;
            """)

            def date_fault():
                value=task()['status']
                return value if value.get('error_code')=='POSTGRESQL_DATE_OUT_OF_RANGE' else None

            refused=require(date_fault,'infinite DATE fails closed before changing target state')
            assert refused['error_stage']=='SOURCE_DECODE'
            assert refused['capture_lsn']==before_date_fault['capture_lsn']
            assert date_rows()[1:]==finished_dates[1:]
            passed('An infinite DATE stops native decoding at the original LSN without applying its transaction to either target')
        elif args.timestamp_values:
            before_timestamp_fault=task()['status']['capture_lsn']
            finished=timestamp_rows()[1:]
            sql('source',"BEGIN; INSERT INTO billing.time_key VALUES('2025-01-01 12:15:00',NULL); "
                         "INSERT INTO sales.time_keyless VALUES('infinity'::timestamptz); COMMIT;")

            def timestamp_fault():
                value=task()['status']
                return value if value.get('error_code')=='POSTGRESQL_TIMESTAMP_OUT_OF_RANGE' else None

            refused=require(timestamp_fault,'infinite timestamp fails closed before changing target state')
            assert refused['error_stage']=='SOURCE_DECODE' and refused['capture_lsn']==before_timestamp_fault
            assert timestamp_rows()[1:]==finished
            passed('An infinite timestamp refuses the complete transaction at the original LSN for both targets')
        else:
            compose('kill','-s','SIGKILL','runner')
        if args.reject_old_primary and not temporal_targets:
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
        elif not temporal_targets and not args.ddl_review:
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
