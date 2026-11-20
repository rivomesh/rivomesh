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
import shlex
import struct
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
type_profile = parser.add_mutually_exclusive_group()
type_profile.add_argument('--date-values', action='store_true', help='Exercise civil DATE through PostgreSQL and MySQL targets')
type_profile.add_argument('--timestamp-values', action='store_true', help='Exercise civil and UTC timestamps through both targets')
type_profile.add_argument('--clock-values', action='store_true', help='Exercise PostgreSQL clock values, including 24:00')
type_profile.add_argument('--floating-values', action='store_true',
                          help='Verify finite PostgreSQL REAL and DOUBLE PRECISION in two SQL destinations')
type_profile.add_argument('--numeric-values', action='store_true',
                          help='Exercise exact bounded PostgreSQL NUMERIC with negative and fractional-only scales')
type_profile.add_argument('--bytea-values', action='store_true',
                          help='Verify PostgreSQL BYTEA and out-of-line TOAST through PostgreSQL and MySQL')
type_profile.add_argument('--dm', action='store_true', help='Verify PostgreSQL CDC to native Dameng')
type_profile.add_argument('--kingbase', action='store_true', help='Verify native PostgreSQL WAL to Kingbase PG mode')
type_profile.add_argument('--gauss-indexes', action='store_true', help='Verify PostgreSQL secondary indexes on openGauss')
type_profile.add_argument('--doris', action='store_true', help='Verify WAL materialization to Doris')
type_profile.add_argument('--doris-hive', action='store_true', help='Verify WAL fan-out to Doris and Hive')
type_profile.add_argument('--redis-clickhouse', action='store_true', help='Verify PostgreSQL WAL fan-out to Redis Streams and ClickHouse')
type_profile.add_argument('--common-cut', action='store_true', help='Verify one PostgreSQL validation cut in both native SQL targets')
type_profile.add_argument('--truncate-review', action='store_true', help='Verify retained DDL boundary for a PostgreSQL TRUNCATE transaction')
parser.add_argument('--bytea-escape-output', action='store_true', help='Use escape as the source server BYTEA output default')
parser.add_argument('--previous-numeric-artifact', type=Path,
                    help='Check a prior release against the same native numeric declarations before the candidate')
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
dual_targets = temporal_targets or args.numeric_values or args.floating_values or args.bytea_values or args.common_cut or args.truncate_review
if args.bytea_escape_output and not args.bytea_values:
    parser.error('--bytea-escape-output requires --bytea-values')
if args.bytea_values and (args.generated or args.failover):
    parser.error('--bytea-values exercises the direct PostgreSQL 17.12 profile')
if args.previous_numeric_artifact and not args.numeric_values:
    parser.error('--previous-numeric-artifact requires --numeric-values')
if args.reject_old_primary and not args.failover:
    parser.error('--reject-old-primary requires --failover')
if args.tls and args.failover:
    parser.error('--tls verifies the direct source and target endpoints. Use --failover for the promotion profile')
if (args.common_cut or args.truncate_review) and (args.failover or args.generated or args.uuid):
    parser.error('--common-cut and --truncate-review use the direct three-table publication profile')
direct_fanout = any((args.redis_clickhouse, args.gauss_indexes, args.kingbase, args.dm, args.doris, args.doris_hive))
isolated_resources = args.bytea_values or args.ddl_review or direct_fanout
if direct_fanout and (args.failover or args.generated or args.uuid or args.tls or args.ddl_review or args.delivery_timing):
    parser.error('Select the direct three-table PostgreSQL 17 profile for --redis-clickhouse, --gauss-indexes, --kingbase, --dm or --doris-hive')
if args.ddl_review and (args.failover or dual_targets):
    parser.error('--ddl-review is an independent native failure profile')
artifact = args.artifact.resolve()
output = args.output.resolve()
output.relative_to(ROOT)
output.mkdir(parents=True, exist_ok=True)
digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
environment = dict(os.environ, RM_PG_PASSWORD=secrets.token_urlsafe(32), RM_PG_TOKEN=secrets.token_urlsafe(32), RM_PG_GAUSS_PASSWORD='Ri8!'+secrets.token_hex(12), RM_PG_KINGBASE_PASSWORD='Kb!'+secrets.token_hex(12), RM_PG_DM_PASSWORD='Dm8!'+secrets.token_hex(10))
project = 'rivomesh-pg-source-' + uuid.uuid4().hex[:10]
image='eclipse-temurin:'+str(args.jdk)+'-jre-jammy'
version_major='18' if args.generated else '17'
report = dict(artifactSha256=digest,status='RUNNING',checks=[],jdk=args.jdk,runtimeImage=image,
              sourceImage=args.source_image,failover=args.failover,rejectOldPrimary=args.reject_old_primary,
              uuid=args.uuid,dateValues=args.date_values,timestampValues=args.timestamp_values,
              clockValues=args.clock_values,deliveryTiming=args.delivery_timing,tls=args.tls)
if args.numeric_values:
    report.update(numericValues=True,harnessSha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
if args.floating_values:
    report.update(floatingValues=True,harnessSha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
if args.bytea_values:
    report.update(byteaValues=True,project=project,harnessSha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
if args.ddl_review:
    report.update(ddlReview=True,project=project,harnessSha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
if args.gauss_indexes:
    report.update(gaussIndexes=True, project=project, harnessSha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
if args.doris or args.doris_hive:
    report.update(doris=True,dorisHive=args.doris_hive, project=project, harnessSha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
if args.dm:
    report.update(dm=True,project=project,harnessSha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
if args.kingbase:
    report.update(kingbase=True,project=project,harnessSha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
if args.redis_clickhouse:
    report.update(redisClickHouse=True, project=project, harnessSha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
if args.truncate_review:
    report.update(truncateReview=True,harnessSha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
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
    if args.common_cut:
        pipeline['source']['username']='capture_fixture'
        pipeline['source']['postgresql']['commonCutValidation']=True
        report.update(commonCut=True,harnessSha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    if args.failover:
        pipeline['source']['postgresql'].update(
            failoverSlot=True,url='jdbc:postgresql://source-rw:5432/sync_test?connectTimeout=3&socketTimeout=15')
    if dual_targets:
        pipeline['targets'].append(dict(
            id='mysql',product='MYSQL',
            url='jdbc:mysql://mysql:3306/?connectTimeout=3000&sslMode=DISABLED&allowPublicKeyRetrieval=true',
            username='root',password='${RM_PG_PASSWORD}',mappings=dict(sales='copy_sales',billing='copy_billing'),
            metadataSchema='copy_meta'))
    if args.doris or args.doris_hive:
        pipeline['targets'].extend([
            dict(id='doris', product='DORIS', url='jdbc:mysql://doris:9030/?useSSL=false&connectTimeout=3000&socketTimeout=10000',
                 username='root', password='', mappings=dict(sales='doris_sales',billing='doris_billing'),
                 metadataSchema='doris_meta', doris=dict(loadUrl='http://doris:8040',replicas=1,buckets=1)),
            dict(id='hive', product='HIVE', url='jdbc:hive2://hive:10000/default;auth=noSasl',
                 username='hive', password='', mappings=dict(sales='hive_sales',billing='hive_billing'),
                 metadataSchema='hive_meta', hive=dict(acknowledgeIndexOmission=True,queryTimeoutSeconds=300)),
        ])
        if not args.doris_hive:
            pipeline['targets'].pop()
        else:
            pipeline['chunkSize']=1000
    if args.redis_clickhouse:
        pipeline['targets'].extend([
            dict(id='redis', product='REDIS_STREAMS', url='redis://redis:6379/2',
                 mappings=dict(sales='sales_events', billing='billing_events'), messaging=dict(stream='RM_PG_PARITY')),
            dict(id='clickhouse', product='CLICKHOUSE', url='http://clickhouse:8123',
                 username='sync_test', password='${RM_PG_PASSWORD}', metadataSchema='ch_meta',
                 mappings=dict(sales='ch_sales', billing='ch_billing')),
        ])
    if args.dm:
        pipeline['targets'].append(dict(
            id='dm',product='DM',url='jdbc:dm://dm:5236',username='SYSDBA',
            password='${RM_PG_DM_PASSWORD}',metadataSchema='dm_meta',mappings=dict(sales='dm_sales',billing='dm_billing'),
            dm=dict(acknowledgeTextConversions=True)))
    if args.kingbase:
        pipeline['targets'].append(dict(
            id='kingbase',product='KINGBASE',url='jdbc:kingbase8://kingbase:54321/test?connectTimeout=4&socketTimeout=20',
            username='system',password='${RM_PG_KINGBASE_PASSWORD}',metadataSchema='kb_meta',
            mappings=dict(sales='kb_sales',billing='kb_billing')))
    if args.gauss_indexes:
        pipeline['source']['postgresql']['captureSecondaryIndexes']=True
        pipeline['targets'].append(dict(id='gauss',product='GAUSS',
            url='jdbc:postgresql://gauss:5432/sync_test?prepareThreshold=0&connectTimeout=5&socketTimeout=15',
            username='sync_test',password='${RM_PG_GAUSS_PASSWORD}',metadataSchema='ga_meta',
            uniquePolicy='DOWNGRADE_BUSINESS',mappings=dict(sales='ga_sales',billing='ga_billing')))
    live_source='source'
    platform=dict(bind='0.0.0.0',port=8080,authToken='${RM_PG_TOKEN}',tasksDirectory='/fixture/tasks',
                  dataDirectory='/fixture/platform',autoStart=True,pollMillis=200,workerHeapMiB=256)
    (home/'platform.json').write_text(json.dumps(platform))
    database=dict(image='postgres:17',environment=dict(POSTGRES_PASSWORD='${RM_PG_PASSWORD}',POSTGRES_DB='sync_test'))
    services=dict(source=dict(database,image=args.source_image,command=['-c','wal_level=logical','-c','max_replication_slots=8',
                                                '-c','wal_sender_timeout=4000']),target=database,
                  runner=dict(image=image,entrypoint=['java','-Xmx384m','-jar','/app/rivomesh.jar'],
                              command=['serve','/fixture/platform.json'],
                              environment=dict(RM_PG_PASSWORD='${RM_PG_PASSWORD}',RM_PG_TOKEN='${RM_PG_TOKEN}', RM_PG_GAUSS_PASSWORD='${RM_PG_GAUSS_PASSWORD}'),
                              volumes=[str(artifact)+':/app/rivomesh.jar:ro',str(home)+':/fixture'],
                              ports=['127.0.0.1::8080'],stop_grace_period='30s'))
    previous_numeric_jar=None
    if args.previous_numeric_artifact:
        previous_numeric_jar=args.previous_numeric_artifact.resolve()
        report['previousNumericArtifactSha256']=hashlib.sha256(previous_numeric_jar.read_bytes()).hexdigest()
        services['runner']['volumes'].append(str(previous_numeric_jar)+':/app/previous-numeric.jar:ro')
    document=dict(services=services)
    if args.dm:
        for service in ('source','target'):
            services[service]['tmpfs']=['/var/lib/postgresql/data:rw,size=256m','/var/run/postgresql:rw,size=8m,mode=1777']
        fixtures=Path(__file__).resolve().parent/'dm'
        services['runner']['environment']['RM_PG_DM_PASSWORD']='${RM_PG_DM_PASSWORD}'
        services['dm']=dict(image='dm8:dm8_20250924_rev288894_HWarm_kylin10_64',mem_limit='2g',cpus=2,
                            entrypoint=['/bin/bash','/fixture/start.sh'],
                            environment=dict(SYSDBA_PWD='${RM_PG_DM_PASSWORD}',SYSAUDITOR_PWD='${RM_PG_DM_PASSWORD}',
                                             SYSSSO_PWD='${RM_PG_DM_PASSWORD}',UNICODE_FLAG='1',CASE_SENSITIVE='1',
                                             BLANK_PAD_MODE='1',PAGE_SIZE='32',BUFFER='128',TIME_ZONE='+00:00'),
                            volumes=[str(fixtures/'start.sh')+':/fixture/start.sh:ro',
                                     str(fixtures/'health.sh')+':/fixture/health.sh:ro','dm_data:/opt/dmdbms/data'])
        services['dm_probe']=dict(image='maven:3.9-eclipse-temurin-21',
                                 environment=dict(RM_PG_DM_PASSWORD='${RM_PG_DM_PASSWORD}'),
                                 volumes=[str(fixtures/'DmSourceTargetProbe.java')+':/probe/DmSourceTargetProbe.java:ro',
                                          str(artifact)+':/app/rivomesh.jar:ro'],
                                 entrypoint=['sh','-ec','javac -cp /app/rivomesh.jar -d /tmp /probe/DmSourceTargetProbe.java '
                                                         '&& exec sleep infinity'])
        document['volumes']={'dm_data':{}}
    if args.kingbase:
        for service in ('source','target'):
            services[service]['tmpfs']=['/var/lib/postgresql/data:rw,size=256m','/var/run/postgresql:rw,size=8m,mode=1777']
        services['runner']['environment']['RM_PG_KINGBASE_PASSWORD']='${RM_PG_KINGBASE_PASSWORD}'
        services['kingbase']=dict(image='kingbase_v009r001c001b0025_single_arm:v1',mem_limit='1500m',cpus=2,
                                  stop_signal='SIGINT',stop_grace_period='30s',
                                  environment=dict(DB_PASSWORD='${RM_PG_KINGBASE_PASSWORD}'),
                                  entrypoint=['/bin/bash','/fixture/start.sh'],
                                  volumes=[str(Path(__file__).resolve().parent/'kingbase/start.sh')+':/fixture/start.sh:ro',
                                           'kingbase_data:/home/kingbase/userdata'])
        document['volumes']={'kingbase_data':{}}
    if args.gauss_indexes:
        for service in ('source','target'):
            services[service]['tmpfs']=['/var/lib/postgresql/data:rw,size=256m','/var/run/postgresql:rw,size=8m,mode=1777']
        services['gauss']=dict(image='opengauss/opengauss-server:latest',mem_limit='2600m',shm_size='512m',
                               tmpfs=['/var/lib/opengauss:rw,size=1536m'],
                               environment=dict(GS_PASSWORD='${RM_PG_GAUSS_PASSWORD}',GS_DB='sync_test',
                                                GS_USERNAME='sync_test',GS_HOST_AUTH_METHOD='sha256',
                                                OTHER_PG_CONF='password_encryption_type = 2'))
    if args.doris or args.doris_hive:
        for service in ('source','target'):
            services[service]['tmpfs']=['/var/lib/postgresql/data:rw,size=256m','/var/run/postgresql:rw,size=8m,mode=1777']
        services['doris']=dict(image='apache/doris:all-in-one-4.1.3',mem_limit='6g',cpus=3,
                              environment=dict(FE_HEAP='768m',BE_HEAP='256m',
                                               BE_CONFIG_EXTRA='mem_limit = 2g'),
                              volumes=['doris_meta:/opt/apache-doris/fe/doris-meta',
                                       'doris_storage:/opt/apache-doris/be/storage'])
        hive_paths=['hive_meta:/opt/hive/rivomesh-meta','hive_data:/opt/hive/data/warehouse',
                    str(Path(__file__).resolve().parent/'hive/hive-site.xml')+':/opt/hive/conf/hive-site.xml:ro']
        services['hive_init']=dict(image='apache/hive:4.0.1',user='root',volumes=hive_paths,
                                  entrypoint=['bash','-euc'], command=[
                                      'chown -R hive:hive /opt/hive/rivomesh-meta /opt/hive/data/warehouse; '
                                      "su hive -s /bin/bash -c 'schematool -dbType derby -info || schematool -dbType derby -initSchema'"])
        services['hive']=dict(image='apache/hive:4.0.1',mem_limit='5g',cpus=3,volumes=hive_paths,
                             depends_on=dict(hive_init=dict(condition='service_completed_successfully')),
                             tmpfs=['/run/rivomesh-hive:rw,nosuid,nodev,noexec,size=64k,mode=1777'],
                             environment=dict(SERVICE_NAME='hiveserver2',IS_RESUME='true',HADOOP_HEAPSIZE='1536',
                                              HADOOP_CLIENT_OPTS='-Xmx1536m',HIVESERVER2_PID_DIR='/run/rivomesh-hive'))
        document['volumes']={name:{} for name in ('doris_meta','doris_storage','hive_meta','hive_data')}
        if not args.doris_hive:
            services.pop('hive')
            services.pop('hive_init')
    if args.redis_clickhouse:
        services['redis'] = dict(image='redis:7.4.2-alpine',
                                 command=['redis-server','--appendonly','yes','--appendfsync','always'])
        services['clickhouse'] = dict(image='clickhouse/clickhouse-server:25.8.33.6',
                                      environment=dict(CLICKHOUSE_USER='sync_test', CLICKHOUSE_PASSWORD='${RM_PG_PASSWORD}',
                                                       CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT='1'))
    if temporal_targets:
        services['source']['command']+=['-c','datestyle=SQL, DMY']
        services['runner']['environment']['TZ']='Pacific/Apia' if args.date_values else 'America/Los_Angeles'
    if dual_targets:
        services['mysql']=dict(image='mysql:8.4',environment=dict(
            MYSQL_ROOT_PASSWORD='${RM_PG_PASSWORD}',MYSQL_ROOT_HOST='%'))
    if args.floating_values:
        services['source']['command']+=['-c','extra_float_digits=0']
    if args.bytea_values:
        services['target']['image']='postgres:17.12'
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
    if args.bytea_escape_output:
        services['source']['command']+=['-c','bytea_output=escape']
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
        env_file=['--env-file',os.devnull] if isolated_resources else []
        return subprocess.run(['docker','compose',*env_file,'-p',project,'-f',str(composition),*arguments],
                              cwd=ROOT,env=environment,capture_output=True,text=True,check=check,timeout=180)

    def sql(service,statement):
        if service=='source':
            service=live_source
        return compose('exec','-T',service,'psql','-X','-v','ON_ERROR_STOP=1','-U','postgres','-d','sync_test',
                       '-A','-t','-F','|','-c',statement).stdout.strip()

    def mysql(statement):
        return compose('exec','-T','mysql','sh','-c',
                       'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql --protocol=TCP --host=127.0.0.1 -uroot -NB -e "$1"',
                       'rivomesh-date',statement).stdout.strip()

    def redis_events():
        raw = compose('exec','-T','redis','redis-cli','--json','-2','-n','2','XRANGE','RM_PG_PARITY','-','+').stdout
        records = []
        for position, fields in json.loads(raw):
            properties = dict(zip(fields[::2], fields[1::2]))
            payload = json.loads(properties['body'])
            assert properties['rivomesh_event_id'] == payload['id']
            records.append((position, payload))
        return records

    def clickhouse(statement):
        return compose('exec','-T','clickhouse','sh','-ec',
                       'clickhouse-client --user sync_test --password "$CLICKHOUSE_PASSWORD" --query "$1"',
                       'clickhouse-client',statement).stdout.strip()

    def doris(statement):
        return compose('exec','-T','doris','mysql','--protocol=TCP','-h127.0.0.1','-P9030','-uroot',
                       '-NB','-e',statement).stdout.strip()

    def hive(statement):
        return compose('exec','-T','hive','beeline','--silent=true','--showHeader=false','--outputformat=tsv2',
                       '-u','jdbc:hive2://127.0.0.1:10000/default;auth=noSasl',
                       '-e',statement).stdout.strip()

    def latest_fanout_cut(expected=None):
        status = task()['status']
        sequence = int(status.get('lastSequence', 0))
        subscriptions = {item['id']: item for item in status.get('subscriptions', [])}
        if set(subscriptions) != (expected or {'pg','redis','clickhouse'}):
            return None
        if any(item['state'] != 'RUNNING' or int(item['acknowledged']) != sequence for item in subscriptions.values()):
            return None
        return status

    def dm(statement):
        return compose('exec','-T','dm_probe','java','-cp','/app/rivomesh.jar:/tmp',
                       'DmSourceTargetProbe',statement).stdout.strip()

    def kingbase(statement):
        return compose('exec','-T','kingbase','/home/kingbase/install/kingbase/bin/ksql',
                       '-U','system','-d','test','-p','54321','-At','-F','|','-c',statement).stdout.strip()

    def gauss(statement):
        return compose('exec','-T','gauss','su','-','omm','-c',
                       'gsql -X -v ON_ERROR_STOP=1 -d sync_test -At -F "|" -c ' + shlex.quote(statement)).stdout.strip()

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

    def numeric_rows():
        def read(query, text_type, billing, sales):
            keyed=(f"SELECT CAST(rounded AS {text_type}),COALESCE(CAST(fractional AS {text_type}),'<NULL>'),"
                   f"COALESCE(CAST(wide AS {text_type}),'<NULL>') FROM {billing}.numeric_key ORDER BY rounded")
            keyless=(f"SELECT COALESCE(CAST(fractional AS {text_type}),'<NULL>'),"
                     f"COALESCE(CAST(wide AS {text_type}),'<NULL>') FROM {sales}.numeric_keyless "
                      "ORDER BY fractional IS NULL,fractional,wide IS NULL,wide")
            return query(keyed),query(keyless)

        return (read(lambda query:sql('source',query),'TEXT','billing','sales'),
                read(lambda query:sql('target',query),'TEXT','copy_billing','copy_sales'),
                read(lambda query:mysql(query).replace('\t','|'),'CHAR','copy_billing','copy_sales'))

    def floating_rows():
        def zero_bits(entry):
            return {'80000000':'00000000','8000000000000000':'0000000000000000'}.get(entry,entry)

        def postgres(service,billing,sales):
            values="COALESCE(encode(float4send(narrow),'hex'),'<NULL>'),COALESCE(encode(float8send(wide),'hex'),'<NULL>')"
            keyed=sql(service,f"SELECT id,{values} FROM {billing}.floating_key ORDER BY id")
            keyless=sql(service,f"SELECT {values} FROM {sales}.floating_keyless")
            return (
                ['|'.join((key,*map(zero_bits,fields))) for key,*fields in (row.split('|') for row in keyed.splitlines())],
                sorted('|'.join(map(zero_bits,row.split('|'))) for row in keyless.splitlines()),
            )

        def packed(value,kind):
            if value=='<NULL>':
                return value
            numeric=float(value)
            return struct.pack(kind,0 if numeric==0 else numeric).hex()

        expressions=("COALESCE(CAST(CAST(narrow AS DOUBLE) AS CHAR),'<NULL>'),"
                     "COALESCE(CAST(wide AS CHAR),'<NULL>')")
        keyed=mysql(f"SELECT id,{expressions} FROM copy_billing.floating_key ORDER BY id")
        keyless=mysql(f"SELECT {expressions} FROM copy_sales.floating_keyless")
        mysql_rows=(
            ['|'.join((key,packed(narrow,'>f'),packed(wide,'>d')))
             for key,narrow,wide in (row.split('\t') for row in keyed.splitlines())],
            sorted('|'.join((packed(narrow,'>f'),packed(wide,'>d')))
                   for narrow,wide in (row.split('\t') for row in keyless.splitlines())),
        )
        return postgres('source','billing','sales'),postgres('target','copy_billing','copy_sales'),mysql_rows

    def floating_target_cut():
        receipt='SELECT epoch,stream_seq,transaction_hash FROM copy_meta.rm3_sink_job'
        return floating_rows()[1:],sql('target',receipt),mysql(receipt)

    def numeric_storage_shape(read):
        return read("SELECT column_name,numeric_precision,numeric_scale,is_nullable FROM information_schema.columns "
                    "WHERE table_schema='copy_billing' AND table_name='numeric_key' ORDER BY ordinal_position")

    def numeric_target_cut():
        receipt='SELECT epoch,stream_seq,transaction_hash FROM copy_meta.rm3_sink_job'
        return numeric_rows()[1:],sql('target',receipt),mysql(receipt)

    def bytea_rows():
        def normalized(read,billing,sales,columns,separator):
            def rows(prefix,schema,table):
                values=read(f"SELECT {prefix},{columns} FROM {schema}.{table}")
                return sorted(tuple(row.split(separator)) for row in values.splitlines())
            return rows('id,label',billing,'bytea_key'),rows('label',sales,'bytea_keyless')

        postgres=("CASE WHEN data IS NULL THEN '<NULL>' "
                  "WHEN octet_length(data)<=32 THEN 'hex:'||encode(data,'hex') "
                  "ELSE octet_length(data)||':sha256:'||encode(sha256(data),'hex') END")
        mysql_binary=("CASE WHEN data IS NULL THEN '<NULL>' "
                      "WHEN octet_length(data)<=32 THEN CONCAT('hex:',LOWER(HEX(data))) "
                      "ELSE CONCAT(octet_length(data),':sha256:',SHA2(data,256)) END")
        return (normalized(lambda query:sql('source',query),'billing','sales',postgres,'|'),
                normalized(lambda query:sql('target',query),'copy_billing','copy_sales',postgres,'|'),
                normalized(mysql,'copy_billing','copy_sales',mysql_binary,'\t'))

    def bytea_value(value):
        if value is None:
            return '<NULL>'
        if len(value)<=32:
            return 'hex:'+value.hex()
        return f'{len(value)}:sha256:{hashlib.sha256(value).hexdigest()}'

    def bytea_fixture_rows(keyed,keyless):
        return (sorted((str(key),label,bytea_value(data)) for key,label,data in keyed),
                sorted((label,bytea_value(data)) for label,data in keyless))

    def bytea_receipt():
        current=task()['status']
        cut=current['capture_lsn']
        query=('SELECT generation,stream_seq,transaction_id,transaction_hash FROM copy_meta.rm3_sink_job '
               "WHERE pipeline_id='postgres_source'")
        postgres=sql('target',query).split('|')
        mysql_receipt=mysql(query).split('\t')
        subscriptions={row['id']:int(row['pendingTransactions']) for row in current['subscriptions']}
        if (postgres!=mysql_receipt or len(postgres)!=4 or subscriptions!={'pg':0,'mysql':0}
                or postgres[2]!='pg:'+cut or not re.fullmatch('[0-9a-f]{64}',postgres[3])):
            return None
        return dict(lsn=cut,generation=postgres[0],sequence=int(postgres[1]),transactionId=postgres[2],
                    transactionHash=postgres[3])

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
        if args.numeric_values:
            source,pg,my=numeric_rows()
            return source==pg==my
        if args.floating_values:
            source,pg,my=floating_rows()
            return source==pg==my
        if args.bytea_values:
            source,pg,my=bytea_rows()
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
        endpoints=['source','target']+(['mysql'] if dual_targets else [])
        if args.doris or args.doris_hive:
            endpoints.extend(('hive','doris') if args.doris_hive else ('doris',))
        if args.redis_clickhouse:
            endpoints.extend(('redis','clickhouse'))
        if args.dm:
            endpoints.extend(('dm','dm_probe'))
        if args.kingbase:
            endpoints.append('kingbase')
        if args.gauss_indexes:
            endpoints.append('gauss')
        compose('up','-d',*endpoints)
        if args.doris or args.doris_hive:
            require(lambda:doris('SELECT 1')=='1','Doris SQL available',180)
            report['dorisWireVersion']=doris('SELECT VERSION()')
            report['dorisImage']='apache/doris:all-in-one-4.1.3'
            require(lambda:doris('SHOW BACKENDS') and '\ttrue\t' in doris('SHOW BACKENDS'),
                    'Doris data backend available',180)
            if args.doris_hive:
                require(lambda:hive('SELECT 1')=='1','HiveServer2 available',240)
                report['hiveVersion']='4.0.1'
        if args.redis_clickhouse:
            require(lambda: compose('exec','-T','redis','redis-cli','PING').stdout.strip() == 'PONG',
                    'Redis target ready')
            report['clickHouseVersion'] = require(lambda: clickhouse('SELECT version()'), 'ClickHouse target ready')
        if args.bytea_values:
            for service in ('source','target'):
                require(lambda s=service:compose('exec','-T',s,'pg_isready','-h','127.0.0.1',
                                                 '-U','postgres','-d','sync_test',check=False).returncode==0,
                        service+' accepts TCP after initialization')
        for service in ('source','target'):
            require(lambda s=service:sql(s,'SELECT 1')=='1',service+' ready')
        if args.dm:
            require(lambda:compose('exec','-T','dm','/bin/bash','/fixture/health.sh',check=False).returncode==0,
                    'Dameng server ready',240)
            report['dmVersion']=require(lambda:dm('--version'),'Dameng JDBC ready',180)
        if args.kingbase:
            report['kingbaseVersion']=require(lambda:kingbase('SELECT version()'),'Kingbase target ready',240)
            assert 'V009R001C001B0025' in report['kingbaseVersion']
        if args.gauss_indexes:
            report['gaussVersion']=require(lambda:gauss('SELECT version()'),'openGauss target ready')
            assert '7.0.0-RC3' in report['gaussVersion']
        report['sourceVersion']=sql('source',"SHOW server_version")
        if dual_targets:
            report['mysqlVersion']=require(lambda:mysql('SELECT VERSION()'),'MySQL target ready')
            report['postgresTargetVersion']=sql('target','SHOW server_version')
        if args.bytea_values:
            assert report['sourceVersion'].split()[0]==report['postgresTargetVersion'].split()[0]=='17.12'
            assert report['mysqlVersion'].split('.')[0:2]==['8','4']
        if temporal_targets:
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
            WITH (publish='insert,update,delete,truncate');
        """)
        if args.common_cut:
            sql('source',"CREATE ROLE capture_fixture LOGIN REPLICATION PASSWORD '"+environment['RM_PG_PASSWORD']+"'; "
                         "GRANT USAGE ON SCHEMA sales,billing TO capture_fixture; "
                         "GRANT SELECT ON ALL TABLES IN SCHEMA sales,billing TO capture_fixture")
        if args.ddl_review:
            sql('source','ALTER TABLE billing.ledger ADD COLUMN observed TIMESTAMP; '
                         'CREATE TABLE billing.retained(id BIGINT PRIMARY KEY); '
                         'ALTER TABLE billing.retained REPLICA IDENTITY FULL; '
                         'ALTER PUBLICATION rivomesh_test ADD TABLE billing.retained')
        if args.uuid:
            sql('source',"""
                CREATE TABLE billing.identities(id UUID PRIMARY KEY,ref UUID);
                ALTER TABLE billing.identities REPLICA IDENTITY FULL;
                INSERT INTO billing.identities VALUES
                    ('A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11',NULL),
                    ('ffffffff-ffff-ffff-ffff-ffffffffffff','00000000-0000-0000-0000-000000000000');
                ALTER PUBLICATION rivomesh_test ADD TABLE billing.identities;
            """)
        if args.bytea_values:
            raw_bytes=bytes.fromhex('00ff5c8000ff')
            updated_bytes=bytes.fromhex('ff00017f')
            original_toast=b''.join(hashlib.md5(f'snapshot-{n}'.encode()).digest() for n in range(1,1025))
            next_toast=b''.join(hashlib.md5(f'after-restart-{n}'.encode()).digest() for n in range(1,2049))
            sql('source',"""
                CREATE TABLE billing.bytea_key(id BIGINT PRIMARY KEY,label VARCHAR(24) NOT NULL,data BYTEA);
                CREATE TABLE sales.bytea_keyless(label VARCHAR(24) NOT NULL,data BYTEA);
                ALTER TABLE billing.bytea_key ALTER COLUMN data SET STORAGE EXTERNAL;
                ALTER TABLE sales.bytea_keyless ALTER COLUMN data SET STORAGE EXTERNAL;
                ALTER TABLE billing.bytea_key REPLICA IDENTITY FULL;
                ALTER TABLE sales.bytea_keyless REPLICA IDENTITY FULL;
                WITH payload(data) AS (
                    SELECT decode(string_agg(md5('snapshot-'||i),'' ORDER BY i),'hex')
                    FROM generate_series(1,1024) i
                )
                INSERT INTO billing.bytea_key
                    SELECT * FROM (VALUES (1,'null',NULL::bytea),(2,'empty',decode('','hex')),
                                          (3,'raw',decode('00ff5c8000ff','hex'))) small
                    UNION ALL SELECT 4,'toast',data FROM payload;
                INSERT INTO sales.bytea_keyless
                    SELECT label,data FROM billing.bytea_key CROSS JOIN generate_series(1,2);
                ALTER PUBLICATION rivomesh_test ADD TABLE billing.bytea_key,sales.bytea_keyless;
            """)
            assert sql('source',"SELECT count(*) FROM pg_class WHERE oid IN "
                               "('billing.bytea_key'::regclass,'sales.bytea_keyless'::regclass) "
                               "AND pg_relation_size(reltoastrelid)>8192")=='2'
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
        if args.floating_values:
            assert sql('source','SHOW extra_float_digits')=='0'
            sql('source',"""
                CREATE TABLE billing.floating_key(id BIGINT PRIMARY KEY, narrow REAL, wide DOUBLE PRECISION);
                CREATE TABLE sales.floating_keyless(narrow REAL, wide DOUBLE PRECISION);
                ALTER TABLE billing.floating_key REPLICA IDENTITY FULL;
                ALTER TABLE sales.floating_keyless REPLICA IDENTITY FULL;
                INSERT INTO billing.floating_key VALUES
                    (1,1.0000001,1.0000000000000002),(2,'-0','-0'),
                    (3,'1.40129846e-45','4.9406564584124654e-324'),(4,NULL,NULL);
                INSERT INTO sales.floating_keyless VALUES
                    (0.1,-0.10000000000000002),(0.1,-0.10000000000000002),
                    (-1.0000001,NULL),(NULL,1.0000000000000002);
                ALTER PUBLICATION rivomesh_test ADD TABLE billing.floating_key,sales.floating_keyless;
            """)
        if args.numeric_values:
            sql('source',"""
                CREATE TABLE sales.unportable_numeric(amount NUMERIC(65,-1));
                ALTER TABLE sales.unportable_numeric REPLICA IDENTITY FULL;
                ALTER PUBLICATION rivomesh_test ADD TABLE sales.unportable_numeric;
            """)
            denied=compose('run','--rm','--no-deps','runner','check','/fixture/tasks/postgres_source.yml',check=False)
            assert denied.returncode!=0 and 'POSTGRESQL_NUMERIC_PROFILE_UNSUPPORTED' in denied.stderr
            assert sql('source',"SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput' AND NOT temporary")=='0'
            sql('source','DROP TABLE sales.unportable_numeric')
            passed('Preflight refuses a PostgreSQL numeric declaration requiring 66 target digits before creating a slot')
            sql('source',"""
                CREATE TABLE billing.numeric_key(rounded NUMERIC(4,-2) PRIMARY KEY,
                    fractional NUMERIC(2,4), wide NUMERIC(65,30));
                CREATE TABLE sales.numeric_keyless(fractional NUMERIC(2,4), wide NUMERIC(65,30));
                ALTER TABLE billing.numeric_key REPLICA IDENTITY FULL;
                ALTER TABLE sales.numeric_keyless REPLICA IDENTITY FULL;
                INSERT INTO billing.numeric_key VALUES
                    (-1249,-0.00994,NULL),
                    (1250,0.00124,-12345678901234567890123456789012345.123456789012345678901234567890);
                INSERT INTO sales.numeric_keyless VALUES
                    (0.00124,0.000000000000000000000000000001),
                    (0.00124,0.000000000000000000000000000001),(0.0099,NULL),(NULL,NULL);
                ALTER PUBLICATION rivomesh_test ADD TABLE billing.numeric_key,sales.numeric_keyless;
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
        if previous_numeric_jar:
            previous=compose('run','--rm','--no-deps','--entrypoint','java','runner','-Xmx384m','-jar',
                             '/app/previous-numeric.jar','check','/fixture/tasks/postgres_source.yml',check=False)
            assert previous.returncode!=0 and 'POSTGRESQL_NUMERIC_PROFILE_UNSUPPORTED' in previous.stderr
            assert sql('source',"SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput' AND NOT temporary")=='0'
            passed('The prior runtime refuses the valid publication containing both extended numeric scales')
        if args.truncate_review:
            sql('source',"ALTER PUBLICATION rivomesh_test SET (publish='insert,update,delete')")
            refused=compose('run','--rm','--no-deps','runner','check','/fixture/tasks/postgres_source.yml',check=False)
            assert refused.returncode and 'POSTGRESQL_PUBLICATION_TRUNCATE_REQUIRED' in refused.stderr
            assert sql('source',"SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput'")=='0'
            sql('source',"ALTER PUBLICATION rivomesh_test SET (publish='insert,update,delete,truncate')")
            passed('Preflight refuses to start a publication that silently omits TRUNCATE')
        if args.gauss_indexes:
            sql('source',"CREATE INDEX idx_amount ON sales.items(amount); "
                         "CREATE UNIQUE INDEX idx_unique_label ON billing.ledger(label); "
                         "CREATE INDEX idx_partial ON sales.items(id) WHERE active")
            partial=json.loads(compose('run','--rm','--no-deps','runner','check','/fixture/tasks/postgres_source.yml').stdout)['gaussIndexReview']
            assert partial['state']=='REQUIRES_PLAN'
            assert any(entry['sourceIndex']=='idx_partial' and entry['action']=='REQUIRES_PLAN' for entry in partial['entries'])
            assert sql('source',"SELECT count(*) FROM pg_replication_slots WHERE plugin='pgoutput'")=='0'
            sql('source','DROP INDEX sales.idx_partial')
            passed('PostgreSQL reports a partial index for review without mutating the published schema')
        preflight=compose('run','--rm','--no-deps','runner','check','/fixture/tasks/postgres_source.yml')
        table_count=3+int(args.uuid)+int(args.ddl_review)+2*int(dual_targets and not (args.common_cut or args.truncate_review))
        assert json.loads(preflight.stdout)['sourceTables']==table_count
        compose('up','-d','runner')
        version=compose('exec','-T','runner','java','-version')
        matched=re.search(r'version "([^"]+)"',version.stderr)
        assert matched and matched.group(1).startswith(str(args.jdk)+'.')
        report['runtimeVersion']=matched.group(1)
        metadata=json.loads(subprocess.check_output(['docker','image','inspect',image],text=True))[0]
        report['runtimeImageId']=metadata['Id']
        report['runtimeArchitecture']=metadata['Architecture']
        if args.bytea_escape_output:
            assert sql('source','SHOW bytea_output')=='escape'
            report['sourceByteaOutput']='escape'
        require(stream_ready,'native consistent snapshot',240 if args.doris or args.doris_hive else 120)
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
        if args.gauss_indexes:
            review=json.loads(preflight.stdout)['gaussIndexReview']
            assert review['state']=='REVIEWED' and review['indexCounts']['supported']==1 and review['indexCounts']['downgradedUnique']==1
            report['sourceIndexReview']=review
            require(lambda:gauss('SELECT count(*) FROM ga_sales.duplicates')=='2','openGauss keyless snapshot')
            catalog_sql="""SELECT n.nspname, c.relname, x.relname, i.indisprimary, i.indisunique
                FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid JOIN pg_namespace n ON n.oid=c.relnamespace
                JOIN pg_class x ON x.oid=i.indexrelid
                WHERE (n.nspname='ga_sales' AND c.relname='items') OR
                      (n.nspname='ga_billing' AND c.relname='ledger') ORDER BY 1,2,3"""
            catalog=gauss(catalog_sql)
            for entry in review['entries']:
                assert any(line.split('|')[2:]==[entry['targetIndex'],'f','f'] for line in catalog.splitlines()), catalog
            assert len([line for line in catalog.splitlines() if line.endswith('|t|t')])==2, catalog
            passed('openGauss creates the reviewed normal indexes and retains both primary keys')
            sql('source',"UPDATE sales.items SET id=42 WHERE id=1; "
                         "DELETE FROM sales.duplicates WHERE ctid=(SELECT ctid FROM sales.duplicates ORDER BY ctid LIMIT 1)")
            require(lambda:gauss('SELECT id FROM ga_sales.items')=='42' and gauss('SELECT count(*) FROM ga_sales.duplicates')=='1',
                    'openGauss receives the key update and one duplicate deletion')
            snapshots=gauss('SELECT snapshot_id FROM ga_meta.rm3_sink_table ORDER BY table_id')
            slot=sql('source',"SELECT slot_name FROM pg_replication_slots WHERE plugin='pgoutput'")
            compose('kill','-s','SIGKILL','runner')
            sql('source',"UPDATE billing.ledger SET label='after restart'")
            compose('up','-d','runner')
            require(lambda:gauss('SELECT label FROM ga_billing.ledger')=='after restart' and equal(),
                    'openGauss receives offline WAL work')
            assert gauss(catalog_sql)==catalog and gauss('SELECT snapshot_id FROM ga_meta.rm3_sink_table ORDER BY table_id')==snapshots
            assert sql('source',"SELECT slot_name FROM pg_replication_slots WHERE plugin='pgoutput'")==slot
            passed('PostgreSQL and openGauss retain the original index definitions and snapshots through Worker restart')
            report['status']='PASS'
            raise SystemExit(0)
        if args.dm:
            expected={'pg','dm'}

            def native_rows(key, hexadecimal, duplicates, label):
                row=dm('SELECT "id","amount","payload","data" FROM "dm_sales"."items"').split('|')
                return (len(row)==4 and row[:2]==[str(key),'123456789012.1234']
                        and json.loads(row[2])=={'n':18446744073709551615} and row[3]==hexadecimal
                        and dm('SELECT count(*) FROM "dm_sales"."duplicates"')==str(duplicates)
                        and dm('SELECT "label" FROM "dm_billing"."ledger"')==label)

            require(lambda:native_rows(1,'001aff',2,'before'),'Dameng exact snapshot',140)
            snapshot_query='SELECT snapshot_id FROM "dm_meta"."rm3_sink_table" WHERE snapshot_state=\'COMPLETE\' ORDER BY table_id'
            original=require(lambda:cut if len((cut:=dm(snapshot_query)).splitlines())==3 else None,
                             'all Dameng snapshots complete')
            passed('Dameng preserves PostgreSQL JSON text, BLOB bytes, exact decimals and keyless multiplicity')
            sql('source',"BEGIN; UPDATE sales.items SET id=9,data=decode('baff','hex') WHERE id=1; "
                         "DELETE FROM sales.duplicates WHERE ctid=(SELECT ctid FROM sales.duplicates ORDER BY ctid LIMIT 1); "
                         "UPDATE billing.ledger SET label='online' WHERE id=1; COMMIT")
            require(lambda:native_rows(9,'baff',1,'online') and equal(),'Dameng atomic mixed-schema update')
            require(lambda:(status:=latest_fanout_cut(expected)) and int(status['lastSequence'])>=1,'both SQL receipts')
            passed('Dameng applies the changed key and removes exactly one keyless row')
            slot=sql('source',"SELECT slot_name FROM pg_replication_slots WHERE plugin='pgoutput'")
            compose('kill','-s','SIGKILL','runner')
            sql('source',"UPDATE billing.ledger SET label='offline' WHERE id=1")
            compose('up','-d','runner')
            require(lambda:native_rows(9,'baff',1,'offline') and equal(),'Dameng offline WAL restart')
            require(lambda:(status:=latest_fanout_cut(expected)) and int(status['lastSequence'])>=2,'both restarted SQL receipts')
            assert original==dm(snapshot_query)
            assert snapshot_ids==sql('target','SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')
            assert slot==sql('source',"SELECT slot_name FROM pg_replication_slots WHERE plugin='pgoutput'")
            passed('Dameng and PostgreSQL keep original snapshots and the WAL slot after SIGKILL')
            report['status']='PASS'
            raise SystemExit(0)

        if args.kingbase:
            expected={'pg','kingbase'}

            def native_rows(key, hexadecimal, duplicates, label):
                return (kingbase("SELECT id,amount::text,payload->>'n',encode(data,'hex') FROM kb_sales.items")
                        ==f'{key}|123456789012.1234|18446744073709551615|{hexadecimal}'
                        and kingbase('SELECT count(*) FROM kb_sales.duplicates')==str(duplicates)
                        and kingbase('SELECT label FROM kb_billing.ledger')==label)

            require(lambda:native_rows(1,'001aff',2,'before'),'Kingbase exact snapshot',120)
            original=kingbase("SELECT snapshot_id FROM kb_meta.rm3_sink_table WHERE snapshot_state='COMPLETE' ORDER BY table_id")
            assert len(original.splitlines())==3
            passed('Kingbase preserves PostgreSQL JSON numbers, bytea, exact decimals and keyless multiplicity')
            sql('source',"BEGIN; UPDATE sales.items SET id=9,data=decode('baff','hex') WHERE id=1; "
                         "DELETE FROM sales.duplicates WHERE ctid=(SELECT ctid FROM sales.duplicates ORDER BY ctid LIMIT 1); "
                         "UPDATE billing.ledger SET label='online' WHERE id=1; COMMIT")
            require(lambda:native_rows(9,'baff',1,'online') and equal(),'Kingbase atomic mixed-schema update')
            require(lambda:(status:=latest_fanout_cut(expected)) and int(status['lastSequence'])>=1,'both SQL receipts')
            passed('Kingbase applies the changed key and removes exactly one keyless row')
            slot=sql('source',"SELECT slot_name FROM pg_replication_slots WHERE plugin='pgoutput'")
            compose('kill','-s','SIGKILL','runner')
            sql('source',"UPDATE billing.ledger SET label='offline' WHERE id=1")
            compose('up','-d','runner')
            require(lambda:native_rows(9,'baff',1,'offline') and equal(),'Kingbase offline WAL restart')
            require(lambda:(status:=latest_fanout_cut(expected)) and int(status['lastSequence'])>=2,'both restarted SQL receipts')
            assert original==kingbase("SELECT snapshot_id FROM kb_meta.rm3_sink_table WHERE snapshot_state='COMPLETE' ORDER BY table_id")
            assert snapshot_ids==sql('target','SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')
            assert slot==sql('source',"SELECT slot_name FROM pg_replication_slots WHERE plugin='pgoutput'")
            passed('Kingbase and PostgreSQL keep both original snapshots and the original WAL slot after SIGKILL')
            report['status']='PASS'
            raise SystemExit(0)

        if args.doris or args.doris_hive:
            expected={'pg','doris','hive'} if args.doris_hive else {'pg','doris'}
            targets=[(doris,'doris'),(hive,'hive')] if args.doris_hive else [(doris,'doris')]
            profile='Doris and Hive' if args.doris_hive else 'Doris'
            timeout=600 if args.doris_hive else 120

            def warehouse_rows(rows, duplicate_count, note):
                for query, prefix in targets:
                    items=prefix+'_sales.items'
                    if int(query('SELECT count(*) FROM '+prefix+'_sales.duplicates')) != duplicate_count:
                        return False
                    if query('SELECT label FROM '+prefix+'_billing.ledger ORDER BY id')!=note:
                        return False
                    if [line.split('\t') for line in query(f'SELECT id,data FROM {items} ORDER BY id').splitlines()]!=rows:
                        return False
                return True

            def warehouse_snapshot_ready():
                status=task()['status']
                for item in status.get('subscriptions',[]):
                    if item['state']=='PAUSED':
                        raise AssertionError(('initial target pause',item['id'],item.get('errorCode')))
                return status.get('phase')=='STREAMING'

            require(warehouse_snapshot_ready,'durable snapshot acknowledgment for every warehouse',timeout)
            require(lambda:warehouse_rows([['1','ABr/']],2,'before') or None,
                    profile+' materialized snapshot values',timeout)
            for query,prefix in targets:
                projected=query('SELECT CAST(amount AS STRING),payload FROM '+prefix+'_sales.items').split('\t',1)
                assert projected[0]=='123456789012.1234' and json.loads(projected[1])=={'n':18446744073709551615}
            passed(profile+' targets preserve snapshot JSON precision, binary Base64 and exact DECIMAL')

            sql('source',"BEGIN; UPDATE sales.items SET id=9, data=decode('baff','hex') WHERE id=1; "
                         "DELETE FROM sales.duplicates WHERE ctid=(SELECT ctid FROM sales.duplicates ORDER BY ctid LIMIT 1); "
                         "UPDATE billing.ledger SET label='online' WHERE id=1; COMMIT")
            require(lambda:warehouse_rows([['9','uv8=']],1,'online') and equal(),
                    profile+' applies the changed key and exactly one keyless delete',timeout)
            require(lambda:(status:=latest_fanout_cut(expected)) and int(status['lastSequence'])>=1,
                    'all target ACKs',timeout)
            passed(profile+' targets apply the same logical transaction with a key change and a single duplicate deletion')

            slot=sql('source',"SELECT slot_name FROM pg_replication_slots WHERE plugin='pgoutput'")
            compose('kill','-s','SIGKILL','runner')
            sql('source',"UPDATE billing.ledger SET label='offline' WHERE id=1")
            compose('up','-d','runner')
            require(lambda:warehouse_rows([['9','uv8=']],1,'offline') and equal(),
                    profile+' catches up the offline WAL after Worker SIGKILL',timeout)
            require(lambda:(status:=latest_fanout_cut(expected)) and int(status['lastSequence'])>=2,
                    'the original fan-out resumes all ACKs',timeout)
            assert snapshot_ids==sql('target','SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')
            assert slot==sql('source',"SELECT slot_name FROM pg_replication_slots WHERE plugin='pgoutput'")
            passed('PostgreSQL to '+profile+' resumes the original slot and snapshots after forced restart')
            report['status']='PASS'
            raise SystemExit(0)

        if args.redis_clickhouse:
            require(latest_fanout_cut, 'PostgreSQL, Redis and ClickHouse snapshots drain')
            require(lambda: clickhouse('SELECT count() FROM ch_sales.duplicates') == '2',
                    'ClickHouse keeps both identical keyless occurrences')
            assert clickhouse('SELECT id,toString(amount),hex(data) FROM ch_sales.items') == '1\t123456789012.1234\t001AFF'
            assert json.loads(clickhouse('SELECT payload FROM ch_sales.items')) == {'n': 18446744073709551615}
            before = redis_events()
            baseline_ids = {event['snapshotId'] for _, event in before if event['kind'] == 'SNAPSHOT_END'}
            assert len(baseline_ids) == 3
            schemas = [event['schema'] for _,event in before if event['kind'] == 'SCHEMA']
            assert len(schemas) == 3 and all(schema['sourceEngine'] == 'POSTGRESQL' for schema in schemas)
            passed('PostgreSQL snapshot preserves JSON, bytes, decimal, and duplicate multiplicity through three target families')

            sql('source', "BEGIN; UPDATE sales.items SET id=9, data=decode('baff','hex') WHERE id=1; "
                          "DELETE FROM sales.duplicates WHERE ctid=(SELECT ctid FROM sales.duplicates ORDER BY ctid LIMIT 1); "
                          "UPDATE billing.ledger SET label='online' WHERE id=1; COMMIT")
            require(lambda: clickhouse("SELECT id,hex(data) FROM ch_sales.items") == '9\tBAFF'
                    and clickhouse('SELECT count() FROM ch_sales.duplicates') == '1'
                    and clickhouse('SELECT label FROM ch_billing.ledger') == 'online'
                    and equal(), 'native online primary-key update and single keyless delete in both databases')
            require(lambda: (status := latest_fanout_cut()) and int(status['lastSequence']) >= 1, 'three online receipts')
            online = redis_events()
            assert online[:len(before)] == before
            commits = [event for _,event in online if event['kind'] == 'COMMIT']
            assert {event['namespace'] for event in commits} == {'sales_events','billing_events'}
            assert len(commits) == 2 and len({event['transactionId'] for event in commits}) == 1
            assert all(event['totalChanges'] == 3 and event['position']['sourceType'] == 'POSTGRESQL'
                       and event['position']['payload']['lsn'] for event in commits)
            passed('Redis retains a typed WAL transaction while ClickHouse applies a changed key and one keyless deletion')

            slot = sql('source', "SELECT slot_name FROM pg_replication_slots WHERE plugin='pgoutput'")
            compose('kill','-s','SIGKILL','runner')
            sql('source', "UPDATE billing.ledger SET label='offline' WHERE id=1")
            compose('up','-d','runner')
            require(lambda: clickhouse("SELECT label FROM ch_billing.ledger") == 'offline' and equal(),
                    'restarted Worker delivers the offline PostgreSQL transaction')
            require(lambda: (status := latest_fanout_cut()) and int(status['lastSequence']) >= 2, 'three recovered receipts')
            recovered = redis_events()
            assert recovered[:len(online)] == online
            assert {event['snapshotId'] for _,event in recovered if event['kind'] == 'SNAPSHOT_END'} == baseline_ids
            assert len([event for _,event in recovered if event['kind']=='COMMIT']) == 4
            assert sql('source', "SELECT slot_name FROM pg_replication_slots WHERE plugin='pgoutput'") == slot
            assert sql('target','SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id') == snapshot_ids
            assert hashlib.sha256(artifact.read_bytes()).hexdigest() == digest
            passed('Original WAL slot, Redis stream offsets and SQL snapshots survive an abrupt Worker restart')
            report['status'] = 'PASS'
            raise SystemExit(0)
        if dual_targets:
            mysql_snapshots=mysql('SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')
            assert len(mysql_snapshots.splitlines())==table_count
            dual_target_slot=sql('source',"SELECT slot_name FROM pg_replication_slots WHERE plugin='pgoutput' AND NOT temporary")
        if args.truncate_review:
            require(lambda:mysql('SELECT COUNT(*) FROM copy_sales.duplicates')=='2','MySQL keyless snapshot')
            sql('source',"CREATE TABLE sales.outside_publication(id bigint); INSERT INTO sales.outside_publication VALUES(1); "
                         "TRUNCATE sales.outside_publication; UPDATE billing.ledger SET label='predecessor' WHERE id=1")
            require(lambda:sql('target','SELECT label FROM copy_billing.ledger')==mysql('SELECT label FROM copy_billing.ledger')
                    =='predecessor','unpublished truncate leaves the replication stream healthy')
            predecessor=require(bytea_receipt,'exact preceding source and target receipts')
            passed('TRUNCATE on an unpublished table leaves both destinations receiving published commits')
            sql('source',"BEGIN; INSERT INTO billing.ledger VALUES(2,'before truncate'); "
                         "TRUNCATE sales.items,sales.duplicates; INSERT INTO billing.ledger VALUES(3,'after truncate'); COMMIT")

            def stopped_at_truncate():
                status=task()['status']
                return status if status.get('error_stage')=='DDL' and status.get('run_state')=='ERROR' else None

            error=require(stopped_at_truncate,'published TRUNCATE stops the complete native transaction')
            assert error['error_code']=='POSTGRESQL_TRUNCATE_REQUIRES_PLAN' and error['capture_lsn']==predecessor['lsn']
            assert sql('source','SELECT COUNT(*) FROM sales.items')=='0'
            assert sql('source','SELECT COUNT(*) FROM billing.ledger')=='3'
            for query in ('SELECT COUNT(*) FROM copy_sales.items','SELECT COUNT(*) FROM copy_billing.ledger'):
                assert sql('target',query)==mysql(query)=='1'
            assert sql('target','SELECT COUNT(*) FROM copy_sales.duplicates')==mysql('SELECT COUNT(*) FROM copy_sales.duplicates')=='2'
            assert bytea_receipt()==predecessor
            review=http(route+'/capture-recovery')
            assert review['phase']=='DDL' and set(review['affectedSubscriptions'])=={'pg','mysql'}
            assert review['actions']==['PRESERVE_SOURCE_LOGS','REVIEW_NEW_BASELINE']
            assert review['schemaDrift']['state']=='NO_VISIBLE_CHANGE'
            passed('A published multi-table TRUNCATE retains the original LSN and both original receipts with no partial inserts')
            compose('kill','-s','SIGKILL','runner')
            compose('up','-d','runner')
            require(stopped_at_truncate,'restart retains the original error and source position')
            assert http(route+'/capture-recovery')['checkpointHash']==review['checkpointHash']
            assert bytea_receipt()==predecessor
            assert sql('target','SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')==snapshot_ids
            assert mysql('SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')==mysql_snapshots
            passed('Worker restart retains the paused DDL evidence and the same snapshot generations')
            assert hashlib.sha256(artifact.read_bytes()).hexdigest()==digest
            report['status']='PASS'
            raise SystemExit(0)
        if args.common_cut:
            def validate(expected):
                result=http(route+'/actions',dict(action='VALIDATE',expectedRevision=task()['revision'],
                    validation={'targets':['pg','mysql'],'gateMillis':10000,'sourceMillis':20000,'targetMillis':20000}))
                operation_id=result['operations'][-1]['id']
                def completed():
                    row=next((row for row in task()['operations'] if row['id']==operation_id),{})
                    return row if row.get('status') in ('SUCCEEDED','FAILED') else None
                row=require(completed,'common cut operation finishes')
                assert row['status']==expected,row
                return row

            require(lambda: mysql('SELECT COUNT(*) FROM copy_sales.duplicates')=='2','native MySQL snapshot')
            initial_cut=task()['status']['capture_lsn']
            refused=validate('FAILED')
            assert refused['code']=='POSTGRESQL_COMMON_CUT_PRIVILEGE_REQUIRED',refused
            assert task()['status']['capture_lsn']==initial_cut
            sql('source','GRANT MAINTAIN ON ALL TABLES IN SCHEMA sales,billing TO capture_fixture')
            http(route+'/actions',dict(action='START',expectedRevision=task()['revision']))
            require(lambda:sql('source',"SELECT active FROM pg_replication_slots WHERE plugin='pgoutput' AND NOT temporary")=='t',
                    'source Worker resumes after the refused validation')
            passed('The read-only capture role requires an explicit source table lock grant for shared-cut validation')
            sql('source',"SELECT pg_logical_emit_message(false,'another.application','unpublished',true)")
            sql('source',"SELECT pg_logical_emit_message(true,'another.application','transactional')")
            sql('source',"UPDATE billing.ledger SET label='ready for common cut' WHERE id=1")
            require(lambda:sql('target','SELECT label FROM copy_billing.ledger')==mysql('SELECT label FROM copy_billing.ledger')
                    =='ready for common cut','published change reaches both targets')
            previous=task()['status']['capture_lsn']
            validation=validate('SUCCEEDED')
            verified=http(route+'/validations/'+validation['validation']['reportId'])
            assert verified['code']=='VALIDATION_MATCH' and len(verified['results'])==6,verified.get('code')
            parked=task()
            assert parked['desired']=='PAUSED' and parked['status']['run_state']=='PAUSED'
            captured=parked['status']['capture_lsn']
            assert captured!=previous
            statement="SELECT transaction_id FROM copy_meta.rm3_sink_job WHERE pipeline_id='postgres_source'"
            assert sql('target',statement)==mysql(statement)=='pg:'+captured
            passed('The flushed publication marker pins an exact LSN for all source tables and both native target receipts')
            compose('kill','-s','SIGKILL','runner')
            sql('source',"INSERT INTO billing.ledger VALUES (2,'after cut')")
            compose('up','-d','runner')
            prior_report=verified['id']
            require(lambda:task().get('status',{}).get('capture_lsn')==captured and task().get('desired')=='PAUSED',
                    'runner recovers the validated position after abrupt exit')
            assert http(route+'/validations/'+prior_report)['code']=='VALIDATION_MATCH'
            parked=task()
            assert sql('target','SELECT COUNT(*) FROM copy_billing.ledger')==mysql('SELECT COUNT(*) FROM copy_billing.ledger')=='1'
            http(route+'/actions',dict(action='RESUME',expectedRevision=parked['revision'],errorCount=parked['status']['error_count']))
            require(lambda:sql('target','SELECT COUNT(*) FROM copy_billing.ledger')==mysql('SELECT COUNT(*) FROM copy_billing.ledger')=='2',
                    'both SQL targets resume the WAL after the original anchored snapshot')
            assert sql('target','SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')==snapshot_ids
            assert mysql('SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')==mysql_snapshots
            assert sql('source',"SELECT slot_name FROM pg_replication_slots WHERE plugin='pgoutput' AND NOT temporary")==dual_target_slot
            passed('Abrupt restart keeps the validation receipt and both targets resume from the same slot and original snapshots')
            assert hashlib.sha256(artifact.read_bytes()).hexdigest()==digest
            report['status']='PASS'
            raise SystemExit(0)
        if args.bytea_values:
            types=("SELECT data_type FROM information_schema.columns WHERE table_schema='copy_billing' "
                   "AND table_name='bytea_key' AND column_name='data'")
            assert sql('target',types)=='bytea' and mysql(types)=='longblob'
            expected=bytea_fixture_rows(
                [(1,'null',None),(2,'empty',b''),(3,'raw',raw_bytes),(4,'toast',original_toast)],
                [(label,data) for label,data in (('null',None),('empty',b''),('raw',raw_bytes),
                                                ('toast',original_toast)) for _ in range(2)])
            assert bytea_rows()==(expected,expected,expected)
            report['byteaToastBytes']=[len(original_toast),len(next_toast)]
            passed('BYTEA snapshot preserves NULL, empty, binary bytes, out-of-line values and keyless multiplicity')
        if args.floating_values:
            types=("SELECT column_name,data_type FROM information_schema.columns "
                   "WHERE table_schema='copy_billing' AND table_name='floating_key' "
                   "AND column_name IN ('narrow','wide') ORDER BY ordinal_position")
            assert sql('target',types)=='narrow|real\nwide|double precision'
            assert mysql(types)=='narrow\tfloat\nwide\tdouble'
            first,second,third=floating_rows()
            assert first==second==third
            assert first[0]==['1|3f800001|3ff0000000000001','2|00000000|0000000000000000',
                              '3|00000001|0000000000000001','4|<NULL>|<NULL>']
            assert first[1].count('3dcccccd|bfb999999999999b')==2
            passed('Binary32 and binary64 snapshots agree at adjacent values, subnormals, SQL NULL and duplicates')
        if args.numeric_values:
            expected_shape='rounded|6|0|NO\nfractional|4|4|YES\nwide|65|30|YES'
            assert numeric_storage_shape(lambda query:sql('target',query))==expected_shape
            assert numeric_storage_shape(lambda query:mysql(query).replace('\t','|'))==expected_shape
            wide='-12345678901234567890123456789012345.123456789012345678901234567890'
            zero_tail='0.000000000000000000000000000001'
            expected=(f'-1200|-0.0099|<NULL>\n1300|0.0012|{wide}',
                      f'0.0012|{zero_tail}\n0.0012|{zero_tail}\n0.0099|<NULL>\n<NULL>|<NULL>')
            assert numeric_rows()==(expected,expected,expected)
            passed('Native snapshots preserve rounded keys, fractional-only values and 65-digit decimals in both targets')
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
            xid=sql('source',"INSERT INTO billing.ledger(id,label) VALUES(15,'delivery observation') RETURNING xmin").splitlines()[0]
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
        if args.bytea_values:
            sql('source',"""
                BEGIN;
                UPDATE billing.bytea_key SET label='was-null',data=decode('','hex') WHERE id=1;
                UPDATE billing.bytea_key SET label='was-empty',data=decode('00ff5c8000ff','hex') WHERE id=2;
                UPDATE billing.bytea_key SET label='was-raw',data=NULL WHERE id=3;
                UPDATE billing.bytea_key SET label='toast-retained' WHERE id=4;
                UPDATE sales.bytea_keyless SET label='raw-updated',data=decode('ff00017f','hex')
                    WHERE ctid=(SELECT ctid FROM sales.bytea_keyless WHERE label='raw' LIMIT 1);
                DELETE FROM sales.bytea_keyless WHERE ctid=(
                    SELECT ctid FROM sales.bytea_keyless WHERE label='null' AND data IS NULL LIMIT 1);
                UPDATE sales.bytea_keyless SET label='toast-retained' WHERE ctid=(
                    SELECT ctid FROM sales.bytea_keyless WHERE label='toast' LIMIT 1);
                COMMIT;
            """)
            require(equal,'BYTEA pgoutput transaction delivered to both destinations')
            expected=bytea_fixture_rows(
                [(1,'was-null',b''),(2,'was-empty',raw_bytes),(3,'was-raw',None),
                 (4,'toast-retained',original_toast)],
                [('null',None),('empty',b''),('empty',b''),('raw',raw_bytes),('raw-updated',updated_bytes),
                 ('toast',original_toast),('toast-retained',original_toast)])
            assert bytea_rows()==(expected,expected,expected)
            passed('Pgoutput retains unchanged out-of-line bytes and changes exactly one matching keyless occurrence')
        if args.floating_values:
            sql('source',"""
                BEGIN;
                UPDATE billing.floating_key SET id=11,narrow=1.0000002,wide=1.0000000000000004 WHERE id=1;
                UPDATE sales.floating_keyless SET wide=0.10000000000000003 WHERE ctid=(
                    SELECT ctid FROM sales.floating_keyless WHERE narrow=0.1::real LIMIT 1);
                COMMIT;
            """)
            require(equal,'bit-exact pgoutput floating-point transaction delivered to both targets')
            keyed,keyless=floating_rows()[0]
            assert keyed[-1]=='11|3f800002|3ff0000000000002'
            assert keyless.count('3dcccccd|bfb999999999999b')==1
            assert keyless.count('3dcccccd|3fb999999999999c')==1
            passed('Pgoutput preserves binary precision despite rounded server defaults, including one keyless update')
        if args.numeric_values:
            sql('source',"""
                BEGIN;
                UPDATE billing.numeric_key SET rounded=1450,fractional=0.00496 WHERE rounded=1300;
                UPDATE sales.numeric_keyless SET fractional=-0.00344 WHERE ctid=(
                    SELECT ctid FROM sales.numeric_keyless WHERE fractional=0.0012 LIMIT 1);
                COMMIT;
            """)
            require(equal,'numeric key move and one duplicate occurrence delivered')
            keyed,keyless=numeric_rows()[0]
            assert keyed.splitlines()==['-1200|-0.0099|<NULL>',f'1500|0.0050|{wide}']
            assert keyless.splitlines()==[f'-0.0034|{zero_tail}',f'0.0012|{zero_tail}','0.0099|<NULL>','<NULL>|<NULL>']
            passed('Pgoutput moves the source-rounded primary key and updates exactly one numeric duplicate')
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

        if args.bytea_values:
            before_bytea_restart=require(bytea_receipt,'dual BYTEA receipts agree at the drained journal LSN')
            report['byteaBeforeRestart']=before_bytea_restart
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
        if args.bytea_values:
            sql('source',"""
                BEGIN;
                UPDATE billing.bytea_key SET label='back-online',data=decode('ff00017f','hex') WHERE id=3;
                UPDATE billing.bytea_key SET label='toast-replaced',data=(
                    SELECT decode(string_agg(md5('after-restart-'||i),'' ORDER BY i),'hex')
                    FROM generate_series(1,2048) i
                ) WHERE id=4;
                INSERT INTO billing.bytea_key VALUES(5,'new-null',NULL);
                DELETE FROM sales.bytea_keyless WHERE ctid=(
                    SELECT ctid FROM sales.bytea_keyless WHERE label='empty' AND octet_length(data)=0 LIMIT 1);
                DELETE FROM sales.bytea_keyless WHERE ctid=(
                    SELECT ctid FROM sales.bytea_keyless WHERE label='toast' LIMIT 1);
                INSERT INTO sales.bytea_keyless
                    SELECT 'toast-new',data FROM billing.bytea_key WHERE id=4;
                COMMIT;
            """)
        if args.floating_values:
            sql('source',"""
                BEGIN;
                INSERT INTO billing.floating_key VALUES(9,-1.0000001,1.0000000000000004);
                DELETE FROM sales.floating_keyless WHERE wide=-0.10000000000000002;
                COMMIT;
            """)
        if args.numeric_values:
            sql('source',"""
                BEGIN;
                UPDATE billing.numeric_key SET rounded=-1450,fractional=NULL WHERE rounded=-1200;
                DELETE FROM sales.numeric_keyless WHERE fractional=0.0012;
                COMMIT;
            """)
        compose('up','-d','runner')
        require(lambda:stream_ready() and task()['status']['capture_lsn']!=previous,'exact LSN restart')
        assert sql('target','SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')==snapshot_ids
        durable=task()['status']['capture_lsn']
        slot=sql('source',"SELECT slot_name FROM pg_replication_slots WHERE plugin='pgoutput' AND NOT temporary")
        assert slot.startswith('rivomesh_test_') and len(slot.splitlines())==1
        passed('SIGKILL and restart reuse the original slot and snapshot with continuing LSN')
        if dual_targets:
            assert slot==dual_target_slot
            assert mysql_snapshots==mysql('SELECT snapshot_id FROM copy_meta.rm3_sink_table ORDER BY table_id')
        if args.bytea_values:
            expected=bytea_fixture_rows(
                [(1,'was-null',b''),(2,'was-empty',raw_bytes),(3,'back-online',updated_bytes),
                 (4,'toast-replaced',next_toast),(5,'new-null',None)],
                [('null',None),('empty',b''),('raw',raw_bytes),('raw-updated',updated_bytes),
                 ('toast-retained',original_toast),('toast-new',next_toast)])
            assert bytea_rows()==(expected,expected,expected)
            after_bytea_restart=require(bytea_receipt,'original-generation receipts cover the offline BYTEA writes')
            assert after_bytea_restart['lsn']==durable
            assert after_bytea_restart['generation']==before_bytea_restart['generation']
            assert after_bytea_restart['sequence']>before_bytea_restart['sequence']
            assert after_bytea_restart['transactionHash']!=before_bytea_restart['transactionHash']
            assert snapshot_ids==mysql_snapshots
            require(lambda:sql('source',"SELECT confirmed_flush_lsn='"+durable+"'::pg_lsn "
                                        "FROM pg_replication_slots WHERE slot_name='"+slot+"'")=='t',
                    'original PostgreSQL slot feedback reaches durable BYTEA LSN')
            report['byteaAfterRestart']=after_bytea_restart
            report['byteaSourceSlot']=slot
            report['byteaSnapshotIds']=snapshot_ids.splitlines()
            passed('Offline BYTEA changes retain both original snapshots and advance the original slot and both target receipts')
        if args.floating_values:
            keyed,keyless=floating_rows()[0]
            assert '9|bf800001|3ff0000000000002' in keyed
            assert '3dcccccd|bfb999999999999b' not in keyless
            passed('Binary32 and binary64 changes made offline reuse the logical slot and both target snapshots')
        if args.numeric_values:
            assert numeric_rows()[0]==(
                f'-1500|<NULL>|<NULL>\n1500|0.0050|{wide}',
                f'-0.0034|{zero_tail}\n0.0099|<NULL>\n<NULL>|<NULL>')
            passed('Numeric CDC catch-up after SIGKILL keeps the original slot and both snapshot sets')
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
                         "ALTER TABLE billing.ledger ALTER COLUMN observed TYPE TIMESTAMP(6); "
                         "INSERT INTO billing.ledger(id,label,first_note) VALUES(2,'unapplied','pending')")

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
            assert table['columns']['modified']==['observed'] and not table['columnOrderChanged']
            blocked=first['blockedRelationEvent']
            assert blocked['state']=='OBSERVED' and blocked['protocol']=='PGOUTPUT_V1_RELATION',blocked
            assert first['blockedRelationCatalog']=={'state':'MATCH'}
            event=blocked['event']
            lsn=lambda value:int(value.split('/')[0],16)<<32|int(value.split('/')[1],16)
            assert event['checkpointSha256']==first['checkpointHash']
            assert (event['namespace'],event['relation'],event['replicaIdentity'])==('billing','ledger','f')
            assert lsn(event['transactionFinalLsn'])>lsn(durable)
            assert [column['name'] for column in event['columns']]==['id','label','observed','first_note']
            assert [(column['typeOid'],column['typeModifier']) for column in event['columns'][2:]]==[(1114,6),(25,-1)]
            assert event['recordedSchemaVersion']==1 and len(event['recordedSchemaSha256'])==64
            report['blockedRelationEvent']=blocked
            passed('The stopped transaction keeps the original native Relation and exact TIMESTAMP type modifier')

            sql('source','ALTER TABLE billing.ledger ADD COLUMN second_note VARCHAR(30)')
            later=http(route+'/capture-recovery')
            current,=later['schemaDrift']['tables']
            assert current['columns']['added']==['first_note','second_note']
            assert current['recordedSha256']==table['recordedSha256'] and current['liveSha256']!=table['liveSha256']
            assert later['checkpointHash']==first['checkpointHash'] and later['errorGeneration']==first['errorGeneration']
            assert later['blockedRelationEvent']==blocked
            assert later['blockedRelationCatalog']=={'state':'COLUMN_LAYOUT_CHANGED'}
            passed('A later DDL changes the advisory live digest while preserving the recorded schema and failure cut')

            sql('source','ALTER TABLE billing.ledger ADD COLUMN out_of_profile TIMETZ')
            unavailable=http(route+'/capture-recovery')
            assert unavailable['schemaDrift']=={'scope':'CURRENT_POSTGRESQL_PUBLICATION_VS_JOURNAL',
                                               'state':'UNAVAILABLE','code':'POSTGRESQL_TYPE_UNSUPPORTED'}
            assert unavailable['checkpointHash']==first['checkpointHash'] and task()['status']['capture_lsn']==durable
            assert unavailable['blockedRelationEvent']==blocked
            assert unavailable['blockedRelationCatalog']=={'state':'UNAVAILABLE','code':'POSTGRESQL_TYPE_UNSUPPORTED'}
            assert target_cut()==original
            passed('An unsupported publication shape has an explicit unavailable diagnostic without changing either cut')

            sql('source','ALTER TABLE billing.ledger DROP COLUMN out_of_profile; '
                         'ALTER TABLE billing.ledger RENAME TO ledger_archive')
            renamed=http(route+'/capture-recovery')
            assert renamed['blockedRelationCatalog']=={'state':'RELATION_RENAMED'}
            sql('source','ALTER PUBLICATION rivomesh_test DROP TABLE billing.ledger_archive')
            removed=http(route+'/capture-recovery')
            assert removed['blockedRelationCatalog']=={'state':'RELATION_NOT_PUBLISHED'}
            sql('source','CREATE TABLE billing.ledger(id BIGINT PRIMARY KEY,label VARCHAR(60),'
                         'observed TIMESTAMP(6),first_note TEXT); '
                         'ALTER TABLE billing.ledger REPLICA IDENTITY FULL; '
                         'ALTER PUBLICATION rivomesh_test ADD TABLE billing.ledger')
            current_oid=int(sql('source',"SELECT 'billing.ledger'::regclass::oid"))
            assert current_oid!=event['relationOid']
            replacement={'state':'RELATION_REPLACED','currentRelationOid':current_oid}
            replaced=http(route+'/capture-recovery')
            assert replaced['blockedRelationCatalog']==replacement
            for review in (renamed,removed,replaced):
                assert review['blockedRelationEvent']==blocked and review['checkpointHash']==first['checkpointHash']
                assert review['errorGeneration']==first['errorGeneration']
            assert target_cut()==original
            passed('The catalog distinguishes a renamed relation, removal from publication and a same-name replacement')

            compose('kill','-s','SIGKILL','runner')
            compose('up','-d','runner')
            restored=require(lambda:http(route+'/capture-recovery'),'the original Relation after controller restart')
            assert restored['blockedRelationEvent']==blocked and restored['checkpointHash']==first['checkpointHash']
            assert restored['blockedRelationCatalog']==replacement and restored['errorGeneration']==first['errorGeneration']
            assert target_cut()==original
            passed('The Controller restart reads the same stored Relation record and leaves the target cut unchanged')
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
        elif args.floating_values:
            prior_lsn=task()['status']['capture_lsn']
            prior_cut=floating_target_cut()
            sql('source',"""
                BEGIN;
                UPDATE billing.floating_key SET id=20 WHERE id=4;
                INSERT INTO sales.floating_keyless VALUES('NaN','Infinity');
                COMMIT;
            """)
            def floating_fault():
                status=task()['status']
                return status if status.get('error_code')=='POSTGRESQL_FLOAT_NONFINITE' else None

            refused=require(floating_fault,'non-finite float aborts the complete logical transaction')
            assert refused['error_stage']=='SOURCE_DECODE' and refused['capture_lsn']==prior_lsn
            assert floating_target_cut()==prior_cut
            passed('A non-finite floating-point value refuses the entire transaction and retains both native receipts')
        elif args.numeric_values:
            prior_lsn=task()['status']['capture_lsn']
            prior_cut=numeric_target_cut()
            sql('source',"""
                BEGIN;
                INSERT INTO billing.numeric_key VALUES(2000,0.0012,NULL);
                INSERT INTO sales.numeric_keyless VALUES('NaN',NULL);
                COMMIT;
            """)

            def numeric_fault():
                value=task()['status']
                return value if value.get('error_code')=='POSTGRESQL_NUMERIC_NONFINITE' else None

            refused=require(numeric_fault,'non-finite numeric aborts logical decoding without changing either target')
            assert refused['error_stage']=='SOURCE_DECODE' and refused['capture_lsn']==prior_lsn
            assert numeric_target_cut()==prior_cut
            passed('A native numeric NaN refuses the complete transaction without moving the journal LSN or either target receipt')
        else:
            compose('kill','-s','SIGKILL','runner')
        if args.reject_old_primary and not dual_targets:
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
        elif not dual_targets and not args.ddl_review:
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
        if previous_numeric_jar:
            assert hashlib.sha256(previous_numeric_jar.read_bytes()).hexdigest()==report['previousNumericArtifactSha256']
        report['status']='PASS'
    except Exception as error:
        report['status']='FAIL'
        if args.doris_hive:
            try:
                cut=task()['status']
                report['targetStateAtFailure']={
                    'phase':cut.get('phase'),
                    'sequence':cut.get('lastSequence'),
                    'subscriptions':[{key:item.get(key) for key in ('id','state','errorCode','errorGeneration')}
                                     for item in cut.get('subscriptions',[])]
                }
            except (OSError,KeyError,NameError):
                pass
        log_services=('runner','standby','source') if args.failover else ('runner',)
        log=compose('logs','--no-color','--tail','80',*log_services,check=False).stdout
        worker_log=home/'platform'/'logs'/'postgres_source.log'
        if worker_log.is_file():
            log+='\n'+'\n'.join(worker_log.read_text(errors='replace').splitlines()[-80:])
        if isinstance(error,subprocess.CalledProcessError):
            log+='\n'+(error.stdout or '')+'\n'+(error.stderr or '')
        for key in ('RM_PG_PASSWORD','RM_PG_TOKEN','RM_PG_GAUSS_PASSWORD','RM_PG_KINGBASE_PASSWORD','RM_PG_DM_PASSWORD'):
            log=log.replace(environment[key],'[REDACTED]')
        (output/'runner-redacted.log').write_text(log)
        raise
    finally:
        cleanup=compose('down','-v','--remove-orphans',check=False)
        report['cleanupExitCode']=cleanup.returncode
        if isolated_resources:
            label='label=com.docker.compose.project='+project
            resources={'containers':['ps','-aq'],'volumes':['volume','ls','-q'],'networks':['network','ls','-q']}
            report['remaining']={kind:len(subprocess.check_output(
                ['docker',*command,'--filter',label],text=True,timeout=30).splitlines())
                for kind,command in resources.items()}
            if any(report['remaining'].values()):
                report['status']='FAIL'
        (output/'manifest.json').write_text(json.dumps(report,indent=2)+'\n')
        if cleanup.returncode or (isolated_resources and any(report['remaining'].values())):
            raise RuntimeError('Test fixture cleanup failed')
