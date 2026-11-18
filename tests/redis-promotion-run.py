#!/usr/bin/env python3
"""Verify Redis streams, original AOF promotion and consumer group recovery."""
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

ROOT = Path(__file__).resolve().parents[1]
BROKER = 'redis:7.4.2-alpine'
RUNTIME = 'eclipse-temurin:21-jre-jammy'
NODES = ('redis-a','redis-b','redis-c')
SOURCE = ROOT/'tests/redis/ReplicatedRedisAcceptance.java'
CLASS = 'io.github.rivomesh.acceptance.ReplicatedRedisAcceptance'
STREAM = 'rivomesh-events'
GROUP = 'retained-consumers'
OWNERS = '_rivomesh_publication_owners_v1'
BINDING = 'rivomesh:'+hashlib.sha256(b'redis-cluster:redis:fixture-generation').hexdigest()


def sha256(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream,'sha256').hexdigest()


def require(check,label,seconds=50):
    end = time.monotonic()+seconds
    reason = None
    while time.monotonic()<end:
        try:
            if result := check():
                return result
        except (OSError,subprocess.SubprocessError,json.JSONDecodeError) as failure:
            reason = failure
        time.sleep(.35)
    raise AssertionError('Timed out: '+label) from reason


def compose_config(home,artifact):
    services = {}
    for node in NODES:
        services[node] = dict(image=BROKER,hostname=node,volumes=[node+':/data'],
                              command=['redis-server','--appendonly','yes','--appendfsync','always',
                                       '--maxmemory-policy','noeviction','--protected-mode','no',
                                       '--repl-diskless-sync-delay','0','--save',''])
    services['redis-a']['networks'] = {'default':{},'access':{'aliases':['redis-rw']}}
    services['runner'] = dict(image=RUNTIME,user=f'{os.getuid()}:{os.getgid()}',
                              networks=['default','access'],
                              volumes=[str(home)+':/fixture',str(artifact)+':/app/rivomesh.jar:ro'],
                              entrypoint=['java','-Xmx208m','-cp','/app/rivomesh.jar:/fixture/classes',CLASS])
    return dict(services=services,volumes={node:{} for node in NODES},
                networks={'default':{},'access':{}})


def pairs(values):
    assert len(values)%2==0
    mapped = dict(zip(values[::2],values[1::2],strict=True))
    assert len(mapped)*2==len(values)
    return mapped


def observed_events(rows, snapshot, transactions):
    unique = {}
    physical = []
    for stream_id, fields in rows:
        assert re.fullmatch(r'[0-9]+-[0-9]+',stream_id)
        fields = pairs(fields)
        raw_bytes = fields['body'].encode('utf-8')
        event = json.loads(raw_bytes)
        assert event['pipelineId']=='redis-cluster' and event['subscriptionId']=='redis'
        assert event['generation']=='fixture-generation' and event['namespace']=='orders'
        assert event['id']==fields['rivomesh_event_id'] and event['kind']==fields['rivomesh_kind']
        route = {'SCHEMA':'orders.__schema','SNAPSHOT_ROW':'orders.items','ROW':'orders.items'}.get(
            event['kind'],'orders.__transactions')
        assert fields['rivomesh_channel']==route
        content = hashlib.sha256(raw_bytes).hexdigest()
        assert unique.setdefault(event['id'],content)==content, 'Repeated event body changed'
        physical.append((stream_id,event))

    assert [event['kind'] for _,event in physical[:4]]==['SCHEMA','SNAPSHOT_BEGIN','SNAPSHOT_ROW','SNAPSHOT_END']
    for _,event in physical[1:4]:
        assert event['snapshotId']==snapshot
    assert [cell['value'] for cell in physical[2][1]['change']['after']]==['1','snapshot']
    for sequence,tx in enumerate(transactions,1):
        events = [event for _,event in physical if event['transactionId']==tx['sourceTransactionId']]
        by_kind = {event['kind']:event for event in events}
        assert set(by_kind)=={'BEGIN','ROW','COMMIT'}
        assert all(event['sequence']==sequence and event['position']['sourceType']=='MYSQL' for event in events)
        expected_order = ['BEGIN']*(2 if sequence==2 else 1)+['ROW','COMMIT']
        assert [event['kind'] for event in events]==expected_order
        assert len({event['id'] for event in events})==3
        assert by_kind['COMMIT']['contentHash']==tx['sourceTransactionHash']
        assert by_kind['COMMIT']['totalChanges']==1 and by_kind['COMMIT']['expectedChanges']=={'orders':1}
        assert by_kind['ROW']['change']['operation']=='INSERT' and by_kind['ROW']['ordinal']==0
        assert [cell['value'] for cell in by_kind['ROW']['change']['after']]==[str(sequence+1),f'native-{sequence}']
    assert len(unique)==4+3*len(transactions)
    return unique


def stage(path,records):
    with path.open('x',encoding='utf-8') as receipt:
        receipt.write(json.dumps(records,ensure_ascii=False)+'\n')
        receipt.flush()
        os.fsync(receipt.fileno())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--java-home',type=Path,default=os.environ.get('JAVA_HOME'))
    args = parser.parse_args()
    artifact,output = args.artifact.resolve(),args.output.resolve()
    for path in (artifact,output):
        path.relative_to(ROOT)
    output.mkdir(mode=0o700,parents=True,exist_ok=False)
    project = 'rivomesh-redis-promotion-'+secrets.token_hex(5)
    javac = str(args.java_home/'bin/javac') if args.java_home else 'javac'
    report = dict(status='FAIL',artifactSha256=sha256(artifact),harnessSha256=sha256(Path(__file__)),
                  acceptanceSourceSha256=sha256(SOURCE),checks=[],phases={})

    def run(*command,check=True,timeout=150,stdin=None):
        return subprocess.run(command,cwd=ROOT,text=True,capture_output=True,input=stdin,
                              check=check,timeout=timeout)

    def passed(label):
        report['checks'].append(label)
        print('PASS '+label,flush=True)

    with tempfile.TemporaryDirectory(prefix='fixture-',dir=output) as temporary:
        home = Path(temporary)
        composition = home/'compose.json'
        composition.write_text(json.dumps(compose_config(home,artifact)))
        command = ('docker','compose','--env-file','/dev/null','-p',project,'-f',str(composition))

        def compose(*docker_args,**options):
            return run(*command,*docker_args,**options)

        def cli(node,*args,db=2,stdin=None):
            result = compose('exec','-T',node,'redis-cli','--json','-2','-n',str(db),
                             *args,stdin=stdin,timeout=15)
            if stdin is not None:
                return [json.loads(line) for line in result.stdout.splitlines()]
            return json.loads(result.stdout)

        def info(node,section):
            raw = compose('exec','-T',node,'redis-cli','--raw','INFO',section,timeout=15).stdout
            return dict(line.split(':',1) for line in raw.splitlines()
                        if ':' in line and not line.startswith('#'))

        def ready(node):
            return cli(node,'PING')=='PONG'

        def replica(node,primary):
            status = info(node,'replication')
            data = info(node,'persistence')
            return (status['role']=='slave' and status['master_host']==primary and status['master_link_status']=='up'
                    and status['master_sync_in_progress']=='0' and data['aof_enabled']=='1')

        def stream(node):
            return cli(node,'XRANGE',STREAM,'-','+')

        def owner(node):
            assert cli(node,'EXISTS',STREAM,OWNERS,db=0)==0
            assert cli(node,'HGET',OWNERS,STREAM)==BINDING
            assert cli(node,'PTTL',STREAM)==cli(node,'PTTL',OWNERS)==-1

        def pending(node):
            return [(row[0],row[1]) for row in cli(node,'XPENDING',STREAM,GROUP,'-','+','40')]

        def group(node):
            state, = [pairs(item) for item in cli(node,'XINFO','GROUPS',STREAM) if pairs(item)['name']==GROUP]
            return state

        def read_group(node,consumer):
            response = cli(node,'XREADGROUP','GROUP',GROUP,consumer,'COUNT','40','STREAMS',STREAM,'>')
            if response is None or response==[]:
                return []
            (key,records), = response
            assert key==STREAM
            return records

        def confirm_consumer(node,entry_ids):
            assert all(re.fullmatch(r'[0-9]+-[0-9]+',entry_id) for entry_id in entry_ids)
            # WAITAOF proves the preceding XACK on this same socket, including earlier PEL updates.
            replies = cli(node,stdin='XACK '+STREAM+' '+GROUP+' '+' '.join(entry_ids)+'\nWAITAOF 1 1 6000\n')
            assert replies==[len(entry_ids),[1,1]]

        def phase(name):
            result = compose('run','--rm','-T','--no-deps','runner',name,'/fixture')
            observed = json.loads(result.stdout)
            report['phases'][name] = observed
            return observed

        started = False
        try:
            run(javac,'--release','21','-cp',str(artifact),'-d',str(home/'classes'),str(SOURCE))
            started = True
            compose('up','-d','redis-a','redis-b')
            for node in NODES[:2]:
                require(lambda:ready(node),'Redis startup '+node)
            report['serverVersion'] = info('redis-a','server')['redis_version']
            report['images'] = {tag:run('docker','image','inspect','--format','{{.Id}}',tag).stdout.strip()
                                for tag in (BROKER,RUNTIME)}
            version = compose('run','--rm','-T','--no-deps','--entrypoint','java','runner','-version')
            report['javaVersion'] = next(line.strip() for line in version.stderr.splitlines()
                                         if line.strip().startswith('openjdk version '))
            assert report['javaVersion'].startswith('openjdk version "21.')
            assert cli('redis-b','REPLICAOF','redis-a','6379')=='OK'
            require(lambda:replica('redis-b','redis-a'),'designated AOF replica')
            initial = phase('initial')
            assert initial['publicationReceipt']['sequence']==initial['journalAcknowledged']==1
            originals = stream('redis-a')
            initial_ids = observed_events(originals,initial['snapshotId'],[initial])
            assert len(originals)==7 and stream('redis-b')==originals
            assert cli('redis-a','XGROUP','CREATE',STREAM,GROUP,'0-0')=='OK'
            delivered = read_group('redis-a','original-consumer')
            assert delivered==originals
            stage(output/'initial-consumer.json',delivered[:4])
            confirmed = [item[0] for item in delivered[:4]]
            confirm_consumer('redis-a',confirmed)
            expected_pending = [(item[0],'original-consumer') for item in delivered[4:]]
            assert pending('redis-a')==pending('redis-b')==expected_pending
            assert group('redis-a')['last-delivered-id']==group('redis-b')['last-delivered-id']==originals[-1][0]
            owner('redis-a')
            owner('redis-b')
            report['initialRunId'] = info('redis-a','server')['run_id']
            report['promotionRunId'] = info('redis-b','server')['run_id']
            report['initialConsumer'] = dict(acknowledged=confirmed,pending=expected_pending,
                                             lastDelivered=originals[-1][0])
            passed('Native WAITAOF preserves the exact original messages, consumer acknowledgements and pending IDs on the designated replica')

            compose('kill','-s','SIGKILL','redis-a')
            replacement = compose('ps','-q','redis-b').stdout.strip()
            run('docker','network','connect','--alias','redis-rw',project+'_access',replacement)
            phase('replica-refused')
            assert pending('redis-b')==expected_pending and stream('redis-b')==originals
            assert info('redis-b','replication')['role']=='slave'
            assert cli('redis-b','REPLICAOF','NO','ONE')=='OK'
            require(lambda:info('redis-b','replication').get('role')=='master','replica promoted')
            assert info('redis-b','server')['run_id']==report['promotionRunId']
            assert report['initialRunId']!=report['promotionRunId']
            assert pending('redis-b')==expected_pending and stream('redis-b')==originals
            owner('redis-b')
            passed('The stable endpoint refuses its read-only replacement, then admits the original fenced replica with unchanged bytes and group state')

            blocked = phase('no-replica')
            assert blocked['journalAcknowledged']==blocked['publicationReceipt']['sequence']==1
            for key in ('transactionId','transactionHash'):
                assert blocked['publicationReceipt'][key]==initial['publicationReceipt'][key]
            ambiguous = stream('redis-b')
            assert len(ambiguous)==8 and ambiguous[:7]==originals
            unknown = json.loads(pairs(ambiguous[-1][1])['body'])
            assert unknown['kind']=='BEGIN' and unknown['transactionId']==blocked['sourceTransactionId']
            assert pending('redis-b')==expected_pending
            passed('One local AOF fsync without its required replica leaves the publisher at the old receipt and a replayable BEGIN')

            compose('up','-d','redis-c')
            require(lambda:ready('redis-c'),'replacement replica starting')
            assert cli('redis-c','REPLICAOF','redis-b','6379')=='OK'
            require(lambda:replica('redis-c','redis-b') and stream('redis-c')==ambiguous,
                    'replacement AOF replica contains the original stream')
            restored = phase('restored')
            assert restored['journalAcknowledged']==restored['publicationReceipt']['sequence']==2
            assert restored['publicationReceipt']['epoch']>blocked['publicationReceipt']['epoch']
            assert restored['sourceTransactionId']==blocked['sourceTransactionId']
            assert restored['sourceTransactionHash']==blocked['sourceTransactionHash']
            assert restored['snapshotId']==blocked['snapshotId']==initial['snapshotId']
            final_records = stream('redis-b')
            assert final_records[:8]==ambiguous and stream('redis-c')==final_records
            final_ids = observed_events(final_records,initial['snapshotId'],[initial,restored])
            assert len(final_records)==11 and initial_ids.items()<=final_ids.items()
            assert pairs(final_records[7][1])['body']==pairs(final_records[8][1])['body']
            passed('Restored replica fsync delivers the retained transaction on the same endpoint and proves byte-identical uncertain replay')

            cursor,claimed,deleted = cli('redis-b','XAUTOCLAIM',STREAM,GROUP,'recovered-consumer','0','0-0','COUNT','40')
            assert cursor=='0-0' and deleted==[] and claimed==originals[4:]
            fresh = read_group('redis-b','recovered-consumer')
            assert fresh==final_records[7:]
            stage(output/'recovered-consumer.json',claimed+fresh)
            acknowledged = [item[0] for item in claimed+fresh]
            confirm_consumer('redis-b',acknowledged)
            assert pending('redis-b')==pending('redis-c')==[]
            last = final_records[-1][0]
            assert group('redis-b')['last-delivered-id']==group('redis-c')['last-delivered-id']==last
            assert read_group('redis-b','recovered-consumer')==[]
            report['consumerGroup'] = dict(reclaimed=[item[0] for item in claimed],acknowledged=acknowledged,
                                           lastDelivered=last)
            passed('Consumer recovery claims the original pending IDs and persists acknowledgement after staging bytes and checking event identity')

            compose('kill','-s','SIGKILL','redis-b')
            compose('start','redis-b')
            require(lambda:ready('redis-b'),'promoted primary cold restart')
            require(lambda:replica('redis-c','redis-b'),'replica reconnected after restart')
            restarted = phase('restarted')
            assert restarted['publicationReceipt']==restored['publicationReceipt']
            assert restarted['snapshotId']==initial['snapshotId']
            report['afterRestartRunId'] = info('redis-b','server')['run_id']
            assert report['afterRestartRunId']!=report['promotionRunId']
            assert final_records==stream('redis-b')==stream('redis-c')
            assert pending('redis-b')==pending('redis-c')==[]
            assert group('redis-b')['last-delivered-id']==last and read_group('redis-b','after-restart')==[]
            owner('redis-b')
            owner('redis-c')
            stage(output/'retained-stream.json',final_records)
            report['producerUniqueEventIds'] = final_ids
            report['physicalRedisEntries'] = len(final_records)
            assert sha256(artifact)==report['artifactSha256']
            passed('Cold restart of the promoted primary loads the same stream IDs, original bytes, group position and empty PEL from AOF')
            report['status'] = 'PASS'
        except subprocess.CalledProcessError as failure:
            (output/'failure.log').write_text((failure.stdout or '')+'\n'+(failure.stderr or ''))
            raise
        finally:
            try:
                if started:
                    try:
                        logs = compose('logs','--no-color','--tail','55',check=False,timeout=45)
                        (output/'redis.log').write_text(logs.stdout+'\n'+logs.stderr)
                    finally:
                        cleanup = compose('down','-v','--remove-orphans',check=False,timeout=90)
                        report['cleanupExitCode'] = cleanup.returncode
                        (output/'cleanup.log').write_text(cleanup.stdout+'\n'+cleanup.stderr)
            finally:
                if started and report.get('cleanupExitCode')!=0:
                    report['status'] = 'FAIL'
                (output/'manifest.json').write_text(json.dumps(report,indent=2)+'\n')
            if report.get('cleanupExitCode'):
                raise RuntimeError('Redis promotion fixture cleanup failed')


if __name__=='__main__':
    main()
