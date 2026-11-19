#!/usr/bin/env python3
"""Exercise broker ownership and BookKeeper quorum recovery in one disposable Pulsar cluster."""
import argparse
import hashlib
import json
import secrets
import subprocess
import tempfile
import time
from pathlib import Path

IMAGE = 'apachepulsar/pulsar:4.0.14'
RUNTIME = 'eclipse-temurin:21-jre'
BOOKIES = ('bookie-a', 'bookie-b', 'bookie-c')
CLUSTER = 'local-quorum'
METADATA = 'zk:zookeeper:2181'


def docker(*command, check=True, timeout=35):
    return subprocess.run(('docker', *command), check=check, timeout=timeout, capture_output=True, text=True)


def sha256(file):
    with file.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def wait_for(check, description, timeout=90):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        try:
            if check():
                return
        except (subprocess.SubprocessError, json.JSONDecodeError):
            pass
        time.sleep(1)
    raise RuntimeError('Timed out: ' + description)


class Cluster:
    def __init__(self, name, manifest):
        self.name = name
        self.manifest = manifest
        self.containers = []
        self.volumes = []
        self.network = False

    def member(self, short):
        return self.name + '-' + short

    def run(self, short, flags, image, command):
        member = self.member(short)
        self.containers.append(member)
        docker('run', '-d', '--pull=never', '--name', member, '--hostname', short,
               '--network', self.name, '--network-alias', short, '--cpus', '2', '--memory', '1120m',
               '--label', 'rivomesh.fixture='+self.name, '--user', '0', *flags, image, *command)
        return member

    def volume(self, short, destination):
        name = self.member(short)
        docker('volume', 'create', '--label', 'rivomesh.fixture='+self.name, name)
        self.volumes.append(name)
        return ['-v', name+':'+destination]

    def start(self, short, config, flags, command):
        settings = [argument for key, value in config.items() for argument in ('-e', key+'='+value)]
        return self.run(short, [*flags, *settings], IMAGE, ['bash', '-ec', command])

    def start_broker(self, broker):
        self.start(broker, {
            'metadataStoreUrl': METADATA, 'configurationMetadataStoreUrl': METADATA, 'clusterName': CLUSTER,
            'advertisedAddress': broker, 'managedLedgerDefaultEnsembleSize': '2',
            'managedLedgerDefaultWriteQuorum': '2', 'managedLedgerDefaultAckQuorum': '2',
            'allowAutoTopicCreation': 'false', 'brokerDeleteInactiveTopicsEnabled': 'false',
            'topicLevelPoliciesEnabled': 'true', 'systemTopicEnabled': 'true',
            'numIOThreads': '2', 'numHttpServerThreads': '4', 'numOrderedExecutorThreads': '2',
            'PULSAR_MEM': '-Xms256m -Xmx384m -XX:MaxDirectMemorySize=256m',
        }, [], 'bin/apply-config-from-env.py conf/broker.conf && exec bin/pulsar broker')

    def active_brokers(self, through, expected):
        reply = docker('exec', self.member(through), 'curl', '--max-time', '3', '-fsS',
                       f'http://{through}:8080/admin/v2/brokers/{CLUSTER}', check=False, timeout=8)
        return reply.returncode == 0 and set(json.loads(reply.stdout)) == {host+':8080' for host in expected}

    def bookies_registered(self, expected):
        reply = docker('exec', self.member('bookie-a'), 'bin/bookkeeper', 'shell', 'listbookies', '-rw',
                       check=False, timeout=20)
        observed = reply.stdout + reply.stderr
        return reply.returncode == 0 and {name for name in BOOKIES if 'BookieID:'+name+':3181,' in observed} == set(expected)

    def initialize(self):
        docker('network', 'create', '--internal', '--label', 'rivomesh.fixture='+self.name, self.name)
        self.network = True
        self.start('zookeeper', {'metadataStoreUrl': METADATA,
                    'PULSAR_MEM': '-Xms128m -Xmx256m -XX:MaxDirectMemorySize=128m'},
                   self.volume('zookeeper', '/pulsar/data/zookeeper'),
                   'bin/apply-config-from-env.py conf/zookeeper.conf && '
                   'bin/generate-zookeeper-config.sh conf/zookeeper.conf && exec bin/pulsar zookeeper')
        wait_for(lambda: docker('exec', self.member('zookeeper'), 'bin/pulsar-zookeeper-ruok.sh',
                                check=False, timeout=8).returncode == 0, 'ZooKeeper ready')
        docker('exec', self.member('zookeeper'), 'bin/pulsar', 'initialize-cluster-metadata',
               '--cluster', CLUSTER, '--metadata-store', METADATA, '--configuration-metadata-store', METADATA,
               '--web-service-url', 'http://broker-b:8080', '--broker-service-url', 'pulsar://broker-b:6650')
        for bookie in BOOKIES:
            self.start(bookie, {
                'clusterName': CLUSTER, 'metadataServiceUri': 'metadata-store:'+METADATA,
                'advertisedAddress': bookie, 'autoRecoveryDaemonEnabled': 'true',
                'BOOKIE_MEM': '-Xms128m -Xmx256m -XX:MaxDirectMemorySize=192m',
            }, self.volume(bookie, '/pulsar/data/bookkeeper'),
                'bin/apply-config-from-env.py conf/bookkeeper.conf && exec bin/pulsar bookie')
        wait_for(lambda: self.bookies_registered(BOOKIES), 'all three bookies registered')
        self.start_broker('broker-a')
        wait_for(lambda: self.active_brokers('broker-a', ('broker-a',)), 'first broker active')

    def scenario(self, args, markers):
        runner = self.run('runner', [
            '-v', str(args.artifact)+':/app/rivomesh-app.jar:ro',
            '-v', str(args.test_classes)+':/test-classes:ro',
            '-v', str(markers)+':/markers',
            '-e', 'RIVOMESH_PULSAR_RESTART_DIR=/markers',
        ], RUNTIME, ['java', '-Xmx256m', '-XX:ActiveProcessorCount=2', '-cp', '/app/rivomesh-app.jar:/test-classes',
                    'io.github.rivomesh.transport.pulsar.PulsarTransportAcceptance', 'quorum'])
        def marker_ready(stage):
            if (markers/(stage+'.request')).is_file():
                return True
            if docker('inspect', '-f', '{{.State.Running}}', runner).stdout.strip()=='false':
                raise RuntimeError('Pulsar acceptance exited before '+stage)
            return False

        stages = (
            ('second-broker', self.second_broker),
            ('kill-owner', self.kill_owner),
            ('lose-quorum', self.lose_quorum),
            ('restore-quorum', self.restore_quorum),
        )
        for name, run in stages:
            wait_for(lambda: marker_ready(name), name)
            print('Native stage '+name, flush=True)
            run()
            (markers/(name+'.ready')).touch(exist_ok=False)
        status = docker('wait', runner, timeout=120).stdout.strip()
        if status != '0':
            raise RuntimeError('Pulsar Java assertions failed')
        logs = docker('logs', runner)
        self.manifest['checks'] = [line[5:] for line in (logs.stdout + logs.stderr).splitlines() if line.startswith('PASS ')]
        if len(self.manifest['checks']) != 4:
            raise RuntimeError('Expected all four assertions')

    def second_broker(self):
        self.start_broker('broker-b')
        wait_for(lambda: self.active_brokers('broker-b', ('broker-a', 'broker-b')), 'both brokers active')

    def kill_owner(self):
        docker('kill', '--signal', 'KILL', self.member('broker-a'))
        wait_for(lambda: self.active_brokers('broker-b', ('broker-b',)), 'only surviving broker registered')

    def lose_quorum(self):
        docker('kill', '--signal', 'KILL', *[self.member(bookie) for bookie in BOOKIES[1:]])
        wait_for(lambda: self.bookies_registered(BOOKIES[:1]), 'one remaining registered bookie')

    def restore_quorum(self):
        docker('start', *[self.member(bookie) for bookie in BOOKIES[1:]])
        wait_for(lambda: self.bookies_registered(BOOKIES), 'three bookies registered after volume-preserving restart')

    def cleanup(self, output):
        for member in reversed(self.containers):
            logs = docker('logs', '--tail', '160', member, check=False)
            (output/(member.removeprefix(self.name+'-')+'.log')).write_text(logs.stdout+logs.stderr)
            docker('rm', '-f', member, check=False)
        runner_log = output/'runner.log'
        if not self.manifest['checks'] and runner_log.exists():
            self.manifest['checks'] = [line[5:] for line in runner_log.read_text().splitlines() if line.startswith('PASS ')]
        if self.network:
            docker('network', 'rm', self.name, check=False)
        for volume in self.volumes:
            docker('volume', 'rm', volume, check=False)
        label = 'label=rivomesh.fixture='+self.name
        resources = (('ps', '-aq'), ('volume', 'ls', '-q'), ('network', 'ls', '-q'))
        return all(not docker(*resource, '--filter', label).stdout.strip() for resource in resources)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for argument in ('artifact', 'test-classes', 'output'):
        parser.add_argument('--'+argument, type=Path, required=True)
    args = parser.parse_args()
    args.artifact = args.artifact.resolve(strict=True)
    args.test_classes = args.test_classes.resolve(strict=True)
    test_class = args.test_classes/'io/github/rivomesh/transport/pulsar/PulsarTransportAcceptance.class'
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {'status': 'FAIL', 'artifactSha256': sha256(args.artifact), 'testClassSha256': sha256(test_class),
                'harnessSourceSha256': sha256(Path(__file__)), 'brokerImage': IMAGE,
                'brokerImageId': docker('image', 'inspect', '-f', '{{.Id}}', IMAGE).stdout.strip(),
                'runnerImageId': docker('image', 'inspect', '-f', '{{.Id}}', RUNTIME).stdout.strip(),
                'brokerCount': 2, 'bookieCount': 3, 'metadataNodes': 1, 'topicQuorum': [2, 2, 2], 'checks': []}
    cluster = Cluster('rivomesh-pulsar-quorum-'+secrets.token_hex(5), manifest)
    try:
        cluster.initialize()
        with tempfile.TemporaryDirectory(prefix=cluster.name+'-', dir=args.output.resolve().parent) as markers:
            cluster.scenario(args, Path(markers))
        if manifest['artifactSha256'] != sha256(args.artifact) or manifest['testClassSha256'] != sha256(test_class):
            raise RuntimeError('Test inputs changed during the run')
        manifest['status'] = 'PASS'
        for check in manifest['checks']:
            print('PASS '+check, flush=True)
    finally:
        manifest['cleanupVerified'] = cluster.cleanup(args.output)
        (args.output/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    if not manifest['cleanupVerified']:
        raise RuntimeError('Fixture resources remain')


if __name__ == '__main__':
    main()
