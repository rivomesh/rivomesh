#!/usr/bin/env python3
"""Verify packaged Kafka publication and consumer offset recovery on KRaft 3.9.2."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
BROKER = 'apache/kafka:3.9.2'
RUNTIME = 'eclipse-temurin:21-jre-jammy'
BROKERS = {1: 'kafka-a', 2: 'kafka-b', 3: 'kafka-c', 4: 'kafka-d'}
JAVA_SOURCE = ROOT/'tests/kafka/ReplicatedKafkaAcceptance.java'
JAVA_CLASS = 'io.github.rivomesh.acceptance.ReplicatedKafkaAcceptance'
NOTICE_SOURCE = ROOT/'tests/kafka/KafkaRecoveryNoticeAcceptance.java'
NOTICE_CLASS = 'io.github.rivomesh.cdc.KafkaRecoveryNoticeAcceptance'


def sha256(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def compose_config(home, artifact, java_class):
    cluster = base64.urlsafe_b64encode(secrets.token_bytes(16)).decode().rstrip('=')
    voters = ','.join(f'{node}@{BROKERS[node]}:9093' for node in (1,2,3))
    services = {}
    for node, name in BROKERS.items():
        controller = node <= 3
        services[name] = dict(image=BROKER, hostname=name, volumes=[name+':/var/lib/kafka/data'],
                              environment={
                                  'CLUSTER_ID': cluster,
                                  'KAFKA_NODE_ID': str(node),
                                  'KAFKA_PROCESS_ROLES': 'broker,controller' if controller else 'broker',
                                  'KAFKA_CONTROLLER_QUORUM_VOTERS': voters,
                                  'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
                                  'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT',
                                  'KAFKA_INTER_BROKER_LISTENER_NAME': 'PLAINTEXT',
                                  'KAFKA_LISTENERS': 'PLAINTEXT://:9092'+(',CONTROLLER://:9093' if controller else ''),
                                  'KAFKA_ADVERTISED_LISTENERS': f'PLAINTEXT://{name}:9092',
                                  'KAFKA_LOG_DIRS': '/var/lib/kafka/data',
                                  'KAFKA_HEAP_OPTS': '-Xms128m -Xmx320m -XX:ActiveProcessorCount=2',
                                  'KAFKA_NUM_NETWORK_THREADS': '2',
                                  'KAFKA_NUM_IO_THREADS': '3',
                                  'KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS': '3',
                                  'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '3',
                                  'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': '3',
                                  'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '2',
                                  'KAFKA_MIN_INSYNC_REPLICAS': '2',
                                  'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS': '0',
                                  'KAFKA_AUTO_CREATE_TOPICS_ENABLE': 'false',
                                  'KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE': 'false',
                              })
    services['runner'] = dict(image=RUNTIME, user=f'{os.getuid()}:{os.getgid()}',
                              volumes=[str(home)+':/fixture',str(artifact)+':/app/rivomesh.jar:ro'],
                              entrypoint=['java','-Xmx256m','-XX:ActiveProcessorCount=2',
                                          '-Dorg.slf4j.simpleLogger.defaultLogLevel=warn',
                                          '-cp','/app/rivomesh.jar:/fixture/classes',java_class])
    return dict(services=services,volumes={name:{} for name in BROKERS.values()})


def verify_records(phase, publications):
    records = phase['records']
    unique = {}
    for offset, record in enumerate(records):
        assert record['offset']==offset, 'Unexpected Kafka offset'
        raw = base64.b64decode(record['bodyBase64'], validate=True)
        event = json.loads(raw)
        assert event['id']==record['id'] and event['subscriptionId']=='kafka'
        assert event['pipelineId']=='kafka-cluster' and event['generation']=='fixture-generation'
        assert event['namespace']=='orders' and event['version']==1
        channel = {'SCHEMA':'orders.__schema','SNAPSHOT_ROW':'orders.items','ROW':'orders.items'}.get(
            event['kind'],'orders.__transactions')
        assert record['channel']==channel
        original = unique.setdefault(record['id'], (event, offset, raw))
        assert original[2]==raw, 'Repeated event ID changed bytes'

    values = [(event, offset) for event, offset, _ in unique.values()]
    observed_snapshot = {}
    (schema, _), = [(event, offset) for event, offset in values if event['kind']=='SCHEMA']
    assert [column['name'] for column in schema['schema']['columns']]==['id','label']
    table = schema['tableId']
    for kind in ('SCHEMA','SNAPSHOT_BEGIN','SNAPSHOT_ROW','SNAPSHOT_END'):
        (event, offset), = [(e,o) for e,o in values if e['kind']==kind]
        observed_snapshot[kind] = offset
        assert event['tableId']==table
        if kind != 'SCHEMA':
            assert event['snapshotId']==phase['snapshotId']
        if kind == 'SNAPSHOT_ROW':
            assert [cell['value'] for cell in event['change']['after']] == ['1','snapshot']

    last = observed_snapshot['SNAPSHOT_END']
    assert list(observed_snapshot.values()) == sorted(observed_snapshot.values())
    for sequence, publication in enumerate(publications,1):
        gtid = publication['sourceTransactionId']
        for kind in ('BEGIN','ROW','COMMIT'):
            (event, offset), = [(e,o) for e,o in values if e['transactionId']==gtid and e['kind']==kind]
            assert offset>last and event['sequence']==sequence
            assert event['position']['sourceType']=='MYSQL'
            assert event['position']['payload']['gtidSet'].endswith(f':1-{100+sequence}')
            if kind=='ROW':
                assert event['ordinal']==0 and event['change']['operation']=='INSERT'
                assert event['tableId']==event['change']['tableId']==table
                assert [cell['value'] for cell in event['change']['after']] == [str(sequence+1),f'native-{sequence}']
            else:
                assert event['expectedChanges']=={'orders':1} and event['totalChanges']==1
            if kind=='COMMIT':
                assert event['contentHash']==publication['sourceTransactionHash']
            last = offset
    assert len(unique)==4+3*len(publications)
    return {id_value:hashlib.sha256(raw).hexdigest() for id_value, (_, _, raw) in unique.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--java-home',type=Path,default=os.environ.get('JAVA_HOME'))
    recovery = parser.add_mutually_exclusive_group()
    recovery.add_argument('--only-recovery-review',action='store_true')
    recovery.add_argument('--only-managed-recovery-review',action='store_true')
    recovery.add_argument('--only-managed-notice-plan',action='store_true')
    recovery.add_argument('--only-notice-recovery',action='store_true')
    args = parser.parse_args()
    artifact,output = args.artifact.resolve(),args.output.resolve()
    for path in (artifact,output):
        path.relative_to(ROOT)
    output.mkdir(mode=0o700,parents=True,exist_ok=False)
    project = 'rivomesh-kafka-'+secrets.token_hex(5)
    javac = str(args.java_home/'bin/javac') if args.java_home else 'javac'
    java_source,java_class = (NOTICE_SOURCE,NOTICE_CLASS) if args.only_notice_recovery else (JAVA_SOURCE,JAVA_CLASS)
    report = dict(status='FAIL',artifactSha256=sha256(artifact),harnessSha256=sha256(Path(__file__)),
                  acceptanceSourceSha256=sha256(java_source),checks=[],phases={})

    def run(*command,check=True,timeout=210):
        return subprocess.run(command,cwd=ROOT,text=True,capture_output=True,check=check,timeout=timeout)

    def passed(label):
        report['checks'].append(label)
        print('PASS '+label,flush=True)

    with tempfile.TemporaryDirectory(prefix='fixture-',dir=output) as temporary:
        home = Path(temporary)
        composition = home/'compose.json'
        composition.write_text(json.dumps(compose_config(home,artifact,java_class)))
        command = ('docker','compose','--env-file','/dev/null','-p',project,'-f',str(composition))

        def compose(*docker_args,**options):
            return run(*command,*docker_args,**options)

        def phase(name,active):
            result = compose('run','--rm','-T','--no-deps','runner',name,','.join(map(str,sorted(active))),'/fixture')
            observed = json.loads(result.stdout)
            (output/(name+'-phase.json')).write_text(json.dumps(observed,indent=2)+'\n')
            consumer = home/(name+'-consumer.json')
            if consumer.is_file():
                (output/consumer.name).write_bytes(consumer.read_bytes())
                assert [record['offset'] for record in json.loads(consumer.read_text())]==observed['stagedOffsets']
            report['phases'][name] = {key:value for key,value in observed.items() if key!='records'}
            return observed

        started = False
        try:
            run(javac,'--release','21','-cp',str(artifact),'-d',str(home/'classes'),str(java_source))
            started = True
            compose('up','-d',*BROKERS.values())
            report['images'] = {tag:run('docker','image','inspect','--format','{{.Id}}',tag).stdout.strip()
                                for tag in (BROKER,RUNTIME)}
            version = compose('run','--rm','-T','--no-deps','--entrypoint','java','runner','-version')
            report['javaVersion'] = next(line.strip() for line in version.stderr.splitlines()
                                         if line.strip().startswith('openjdk version '))
            assert report['javaVersion'].startswith('openjdk version "21.')
            if args.only_notice_recovery:
                notice = phase('notice-recovery',set(BROKERS))
                assert notice['oldProducerFenced'] and notice['oldUncommittedOffset'] < notice['recoveredOffset']
                assert notice['visibleControlOffsets'] == [notice['recoveredOffset']]
                passed('A new transactional writer fences the old Kafka producer and exact offset replay proves one committed control notice')
                assert notice['businessBefore'] == notice['businessAfter']
                assert notice['failedTargetAcknowledgment'] == 1 and notice['healthyPeerAcknowledgment'] == 2
                assert notice['sourcePayloadUnchanged']
                passed('Commit response loss and journal reopen preserve both business topics, consumer offsets, original payload and target acknowledgments')
                assert sha256(artifact)==report['artifactSha256']
                report['status'] = 'PASS'
                return
            if args.only_recovery_review or args.only_managed_recovery_review or args.only_managed_notice_plan:
                mode = ('managed-notice-plan' if args.only_managed_notice_plan else
                        'managed-recovery-review' if args.only_managed_recovery_review else 'recovery-review')
                review = phase(mode,set(BROKERS))
                assert review['review']['status']=='PRECEDING_KAFKA_RECEIPTS_VERIFIED'
                assert review['review']['acknowledgedSequence']=='1' and review['review']['failedSequence']=='2'
                assert len(review['review']['nativeReceipts'])==2
                assert review['brokerEndBefore']==review['brokerEndAfterReview']
                assert review['consumerOffsetBefore']==review['consumerOffsetAfterReview']
                if args.only_managed_recovery_review or args.only_managed_notice_plan:
                    assert review['managedOwner']=='owner' and review['managedActor']=='legacy-admin'
                    assert review['managedReplayedAcrossEntries'] and review['managedHistoricalOwnerOffline']
                    passed('Two packaged management entries verify both preceding native COMMIT receipts with stable replay and owner-offline lookup')
                else:
                    passed('The read-only command verifies both preceding native COMMIT receipts with no offset or checkpoint mutation')
                assert review['partialFailureCode']=='KAFKA_RECOVERY_PUBLICATION_UNCERTAIN'
                assert review['journalAcknowledged']==review['publicationAcknowledged']==1
                assert review['peerAcknowledged']==2 and review['peerEndBefore']==review['peerEndAfter']
                passed('A native partial BEGIN refuses recovery proof and preserves the original failed payload and checkpoints')
                if args.only_managed_notice_plan:
                    assert not review['noticePlan']['executionAuthorized']
                    assert review['noticePlan']['publicationBlockCode']=='KAFKA_NOTICE_GLOBAL_OWNERSHIP_UNVERIFIED'
                    assert review['noticePlanPartialRefusal']=='KAFKA_RECOVERY_PUBLICATION_UNCERTAIN'
                    assert review['noticeControlEnd']==0
                    passed('Both management entries return one identical read-only notice plan, with publication blocked and the dedicated topic untouched')
                assert sha256(artifact)==report['artifactSha256']
                report['status'] = 'PASS'
                return
            initial = phase('initial',set(BROKERS))
            assert initial['publicationReceipt']['sequence']==initial['journalAcknowledged']==1
            assert initial['storedOffsetBefore']==0 and initial['storedOffsetAfter']==len(initial['records'])==7
            originals = verify_records(initial,[initial])
            passed('Three in-sync topic replicas accept exact source bytes and persist the consumer offset')

            first_leader = initial['cluster']['leader']
            assert first_leader in (1,2)
            compose('kill','-s','SIGKILL',BROKERS[first_leader])
            survivors = set(BROKERS)-{first_leader}
            elected = phase('elected',survivors)
            assert elected['cluster']['leader']!=first_leader and len(elected['cluster']['isr'])==2
            assert elected['cluster']['clusterId']==initial['cluster']['clusterId']
            assert elected['cluster']['topicId']==initial['cluster']['topicId']
            assert elected['records'][:7]==initial['records']
            assert elected['storedOffsetBefore']==elected['storedOffsetAfter']==7
            assert elected['stagedOffsets']==list(range(7,len(elected['records'])))
            preserved = verify_records(elected,[initial,elected])
            assert originals.items()<=preserved.items()
            passed('Partition leader SIGKILL elects a surviving replica and keeps original offsets and event bodies')

            compose('kill','-s','SIGKILL',BROKERS[4])
            blocked = phase('below-isr',survivors-{4})
            assert len(blocked['cluster']['isr'])==1 and blocked['retryCode']=='KAFKA_RETRY'
            assert blocked['metadataProofHighWatermark']>blocked['cluster']['metadataHighWatermark']
            assert blocked['publicationReceipt']['sequence']==blocked['journalAcknowledged']==2
            for key in ('transactionId','transactionHash'):
                assert blocked['publicationReceipt'][key]==elected['publicationReceipt'][key]
            passed('One data replica refuses confirmation while two controllers still commit new cluster metadata')

            compose('start',BROKERS[4])
            restored = phase('restored',survivors)
            assert restored['sourceTransactionId']==blocked['sourceTransactionId']
            assert restored['sourceTransactionHash']==blocked['sourceTransactionHash']
            assert restored['publicationReceipt']['sequence']==restored['journalAcknowledged']==3
            assert restored['publicationReceipt']['epoch']>blocked['publicationReceipt']['epoch']
            assert initial['snapshotId']==elected['snapshotId']==blocked['snapshotId']==restored['snapshotId']
            assert restored['records'][:len(elected['records'])]==elected['records']
            assert restored['storedOffsetBefore']==7 and restored['storedOffsetAfter']==len(restored['records'])
            assert restored['stagedOffsets']==list(range(7,len(restored['records'])))
            recovered = verify_records(restored,[initial,elected,restored])
            assert preserved.items()<=recovered.items()
            passed('Restarted publisher reopens the original journal and delivers the retained transaction when ISR recovers')

            compose('start',BROKERS[first_leader])
            final = phase('offsets',set(BROKERS))
            assert final['cluster']['clusterId']==initial['cluster']['clusterId']
            assert final['cluster']['topicId']==initial['cluster']['topicId']
            assert final['storedOffsetBefore']==final['storedOffsetAfter']==len(restored['records'])
            assert final['stagedOffsets']==[] and final['records']==restored['records']
            report['consumerEventIds'] = recovered
            report['brokerRecordCount'] = len(final['records'])
            assert sha256(artifact)==report['artifactSha256']
            passed('Reopened consumer uses the committed offset after broker restart and retains original topic history')
            report['status'] = 'PASS'
        except subprocess.CalledProcessError as failure:
            (output/'failure.log').write_text((failure.stdout or '')+'\n'+(failure.stderr or ''))
            raise
        finally:
            try:
                if started:
                    try:
                        logs = compose('logs','--no-color','--tail','55',check=False,timeout=45)
                        (output/'broker.log').write_text(logs.stdout+'\n'+logs.stderr)
                    finally:
                        cleanup = compose('down','-v','--remove-orphans',check=False,timeout=90)
                        report['cleanupExitCode'] = cleanup.returncode
                        (output/'cleanup.log').write_text(cleanup.stdout+'\n'+cleanup.stderr)
            finally:
                if started and report.get('cleanupExitCode')!=0:
                    report['status'] = 'FAIL'
                (output/'manifest.json').write_text(json.dumps(report,indent=2)+'\n')
            if report.get('cleanupExitCode'):
                raise RuntimeError('Kafka fixture cleanup failed')


if __name__=='__main__':
    main()
