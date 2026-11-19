#!/usr/bin/env python3
"""Exercise broker ownership and BookKeeper quorum recovery in one disposable Pulsar cluster."""
import argparse
import base64
import hashlib
import hmac
import json
import re
import secrets
import subprocess
import tempfile
import time
from pathlib import Path

IMAGE = 'apachepulsar/pulsar:4.0.14'
RUNTIME = 'eclipse-temurin:21-jre'
BOOKIES = ('bookie-a', 'bookie-b', 'bookie-c')
CLUSTER = 'local-quorum'
ZOOKEEPERS = ('zookeeper', 'zookeeper-b', 'zookeeper-c')


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


def generate_security(folder, openssl):
    credentials = folder/'tls'
    credentials.mkdir(mode=0o700)
    def run(*options):
        subprocess.run((openssl,*options), cwd=credentials, check=True, capture_output=True, timeout=20)
    def encode(value):
        return base64.urlsafe_b64encode(value).rstrip(b'=')
    key = secrets.token_bytes(32)
    unsigned = encode(b'{"alg":"HS256"}')+b'.'+encode(b'{"sub":"rivomesh-quorum"}')
    token = (unsigned+b'.'+encode(hmac.digest(key,unsigned,'sha256'))).decode()
    (credentials/'secret.key').write_bytes(key)
    (credentials/'token').write_text(token)
    (credentials/'auth.header').write_text('Authorization: Bearer '+token+'\n')
    run('req','-x509','-newkey','rsa:2048','-nodes','-days','2','-subj','/CN=Rivomesh disposable quorum',
        '-addext','basicConstraints=critical,CA:TRUE','-keyout','ca.key','-out','ca.pem')
    for broker in ('broker-a','broker-b'):
        (credentials/(broker+'.ext')).write_text('subjectAltName=DNS:'+broker+'\nextendedKeyUsage=serverAuth\n')
        run('req','-new','-newkey','rsa:2048','-nodes','-subj','/CN='+broker,
            '-keyout',broker+'.key','-out',broker+'.csr')
        run('x509','-req','-in',broker+'.csr','-CA','ca.pem','-CAkey','ca.key','-CAcreateserial',
            '-days','2','-extfile',broker+'.ext','-out',broker+'.pem')
    for name in ('secret.key','token','auth.header','ca.key','broker-a.key','broker-b.key'):
        (credentials/name).chmod(0o600)
    return credentials, (token,base64.b64encode(key).decode())


class Cluster:
    def __init__(self, name, manifest, credentials=None, redact=()):
        self.name = name
        self.manifest = manifest
        self.containers = []
        self.volumes = []
        self.network = False
        self.credentials = credentials
        self.redact = redact
        self.metadata_nodes = ZOOKEEPERS if credentials else ZOOKEEPERS[:1]
        self.metadata = 'zk:'+','.join(node+':2181' for node in self.metadata_nodes)
        self.scheme = 'https' if credentials else 'http'
        self.web_port = 8443 if credentials else 8080
        self.data_scheme = 'pulsar+ssl' if credentials else 'pulsar'
        self.data_port = 6651 if credentials else 6650

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
        settings = {
            'metadataStoreUrl': self.metadata, 'configurationMetadataStoreUrl': self.metadata, 'clusterName': CLUSTER,
            'advertisedAddress': broker, 'managedLedgerDefaultEnsembleSize': '2',
            'managedLedgerDefaultWriteQuorum': '2', 'managedLedgerDefaultAckQuorum': '2',
            'allowAutoTopicCreation': 'false', 'brokerDeleteInactiveTopicsEnabled': 'false',
            'topicLevelPoliciesEnabled': 'true', 'systemTopicEnabled': 'true',
            'numIOThreads': '2', 'numHttpServerThreads': '4', 'numOrderedExecutorThreads': '2',
            'PULSAR_MEM': '-Xms256m -Xmx384m -XX:MaxDirectMemorySize=256m',
        }
        flags = []
        if self.credentials:
            mount_names = ['ca.pem','auth.header','secret.key','token',broker+'.pem',broker+'.key']
            flags = [item for name in mount_names for item in
                     ('-v',str(self.credentials/name)+':/tls/'+name+':ro')]
            if broker=='broker-b':
                flags.extend(('--network-alias','broker-wrong'))
            settings.update(
                brokerServicePort='',webServicePort='',brokerServicePortTls='6651',webServicePortTls='8443',
                tlsTrustCertsFilePath='/tls/ca.pem',tlsCertificateFilePath='/tls/'+broker+'.pem',
                tlsKeyFilePath='/tls/'+broker+'.key',brokerClientTlsEnabled='true',
                brokerClientTrustCertsFilePath='/tls/ca.pem',
                authenticationEnabled='true',authorizationEnabled='true',superUserRoles='rivomesh-quorum',
                authenticationProviders='org.apache.pulsar.broker.authentication.AuthenticationProviderToken',
                tokenSecretKey='file:///tls/secret.key',
                brokerClientAuthenticationPlugin='org.apache.pulsar.client.impl.auth.AuthenticationToken',
                brokerClientAuthenticationParameters='file:///tls/token',
            )
        self.start(broker,settings,flags,'bin/apply-config-from-env.py conf/broker.conf && exec bin/pulsar broker')

    def active_brokers(self, through, expected):
        auth = ('--cacert','/tls/ca.pem','--header','@/tls/auth.header') if self.credentials else ()
        reply = docker('exec', self.member(through), 'curl', '--max-time', '3', '-fsS', *auth,
                       f'{self.scheme}://{through}:{self.web_port}/admin/v2/brokers/{CLUSTER}', check=False, timeout=8)
        return reply.returncode == 0 and {broker.removeprefix(self.scheme+'://') for broker in json.loads(reply.stdout)} == {
            f'{host}:{self.web_port}' for host in expected
        }

    def zookeeper_mode(self, node):
        result = docker('exec', self.member(node), 'sh', '-ec', 'printf srvr | nc -w 3 127.0.0.1 2181',
                        check=False, timeout=8)
        mode = re.search(r'(?m)^Mode: (leader|follower)$',result.stdout)
        return mode[1] if result.returncode==0 and mode else None

    def metadata_leader(self, members):
        modes = {node: self.zookeeper_mode(node) for node in members}
        leaders = [node for node,mode in modes.items() if mode=='leader']
        if len(leaders)==1 and all(modes.values()):
            return leaders[0]
        return None

    def lose_metadata_leader(self):
        leader = self.metadata_leader(self.metadata_nodes)
        if leader is None:
            raise RuntimeError('The metadata cluster did not expose one leader before the fault')
        docker('kill','--signal','KILL',self.member(leader))
        survivors = tuple(node for node in self.metadata_nodes if node!=leader)
        wait_for(lambda: self.metadata_leader(survivors), 'remaining ZooKeeper quorum chooses a leader')
        self.manifest['metadataElection'] = {'formerLeader':leader,'electedLeader':self.metadata_leader(survivors)}
        if self.manifest['metadataElection']['electedLeader'] not in survivors:
            raise RuntimeError('The surviving metadata election did not settle')

    def bookies_registered(self, expected):
        reply = docker('exec', self.member('bookie-a'), 'bin/bookkeeper', 'shell', 'listbookies', '-rw',
                       check=False, timeout=20)
        observed = reply.stdout + reply.stderr
        return reply.returncode == 0 and {name for name in BOOKIES if 'BookieID:'+name+':3181,' in observed} == set(expected)

    def initialize(self):
        docker('network', 'create', '--internal', '--label', 'rivomesh.fixture='+self.name, self.name)
        self.network = True
        if self.credentials:
            members = ''.join(f'server.{index}={node}:2888:3888\n'
                              for index,node in enumerate(self.metadata_nodes,1))
            zk_settings = self.credentials/'ensemble.properties'
            zk_settings.write_text('\n4lw.commands.whitelist=ruok,srvr\n'+members)
            for index,node in enumerate(self.metadata_nodes,1):
                self.start(node, {'ZOO_ID':str(index),
                    'PULSAR_MEM':'-Xms96m -Xmx192m -XX:MaxDirectMemorySize=96m'},
                    [*self.volume(node,'/pulsar/data/zookeeper'),'-v',str(zk_settings)+':/ensemble:ro'],
                    'printf "%s\n" "$ZOO_ID" > /pulsar/data/zookeeper/myid && '
                    'cat /ensemble >> conf/zookeeper.conf && exec bin/pulsar zookeeper')
            wait_for(lambda: self.metadata_leader(self.metadata_nodes), 'three-node ZooKeeper quorum ready')
        else:
            self.start('zookeeper', {'metadataStoreUrl': self.metadata,
                        'PULSAR_MEM': '-Xms128m -Xmx256m -XX:MaxDirectMemorySize=128m'},
                       self.volume('zookeeper', '/pulsar/data/zookeeper'),
                       'bin/apply-config-from-env.py conf/zookeeper.conf && '
                       'bin/generate-zookeeper-config.sh conf/zookeeper.conf && exec bin/pulsar zookeeper')
            wait_for(lambda: docker('exec', self.member('zookeeper'), 'bin/pulsar-zookeeper-ruok.sh',
                                    check=False, timeout=8).returncode == 0, 'ZooKeeper ready')
        cluster_url_flags = ('--web-service-url','http://broker-b:8080','--broker-service-url','pulsar://broker-b:6650')
        if self.credentials:
            cluster_url_flags += ('--tls-enable',
                '--web-service-url-tls',f'https://broker-b:{self.web_port}',
                '--broker-service-url-tls',f'pulsar+ssl://broker-b:{self.data_port}')
        initialized = docker('exec',self.member('zookeeper'),'bin/pulsar','initialize-cluster-metadata',
                             '--cluster',CLUSTER,'--metadata-store',self.metadata,
                             '--configuration-metadata-store',self.metadata,*cluster_url_flags,check=False)
        if initialized.returncode:
            raise RuntimeError('Pulsar cluster metadata initialization failed: ' + initialized.stderr[-2000:])
        for bookie in BOOKIES:
            self.start(bookie, {
                'clusterName': CLUSTER, 'metadataServiceUri': 'metadata-store:'+self.metadata,
                'advertisedAddress': bookie, 'autoRecoveryDaemonEnabled': 'true',
                'BOOKIE_MEM': '-Xms128m -Xmx256m -XX:MaxDirectMemorySize=192m',
            }, self.volume(bookie, '/pulsar/data/bookkeeper'),
                'bin/apply-config-from-env.py conf/bookkeeper.conf && exec bin/pulsar bookie')
        wait_for(lambda: self.bookies_registered(BOOKIES), 'all three bookies registered')
        self.start_broker('broker-a')
        wait_for(lambda: self.active_brokers('broker-a', ('broker-a',)), 'first broker active')

    def scenario(self, args, markers):
        secure_flags = [item for name in ('ca.pem','token') for item in
                        ('-v',str(self.credentials/name)+':/tls/'+name+':ro')] if self.credentials else []
        runner = self.run('runner', [
            *secure_flags,
            '-v', str(args.artifact)+':/app/rivomesh-app.jar:ro',
            '-v', str(args.test_classes)+':/test-classes:ro',
            '-v', str(markers)+':/markers',
            '-e', 'RIVOMESH_PULSAR_RESTART_DIR=/markers',
        ], RUNTIME, ['java', '-Xmx256m', '-XX:ActiveProcessorCount=2', '-cp', '/app/rivomesh-app.jar:/test-classes',
                    'io.github.rivomesh.transport.pulsar.PulsarTransportAcceptance',
                    'secure-quorum' if self.credentials else 'quorum'])
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
        if self.credentials:
            stages += (('lose-metadata-leader',self.lose_metadata_leader),)
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
        if len(self.manifest['checks']) != (6 if self.credentials else 4):
            raise RuntimeError('Expected all native assertions for the selected profile')

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
            output_text = logs.stdout+logs.stderr
            for secret in self.redact:
                output_text = output_text.replace(secret,'[REDACTED]')
            (output/(member.removeprefix(self.name+'-')+'.log')).write_text(output_text)
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
    parser.add_argument('--secure-metadata-quorum', action='store_true',
                        help='Use private TLS/JWT on both brokers and elect a ZooKeeper survivor')
    parser.add_argument('--openssl', default='openssl')
    args = parser.parse_args()
    args.artifact = args.artifact.resolve(strict=True)
    args.test_classes = args.test_classes.resolve(strict=True)
    test_class = args.test_classes/'io/github/rivomesh/transport/pulsar/PulsarTransportAcceptance.class'
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {'status': 'FAIL', 'artifactSha256': sha256(args.artifact), 'testClassSha256': sha256(test_class),
                'harnessSourceSha256': sha256(Path(__file__)), 'brokerImage': IMAGE,
                'brokerImageId': docker('image', 'inspect', '-f', '{{.Id}}', IMAGE).stdout.strip(),
                'runnerImageId': docker('image', 'inspect', '-f', '{{.Id}}', RUNTIME).stdout.strip(),
                'brokerCount': 2, 'bookieCount': 3, 'metadataNodes': 3 if args.secure_metadata_quorum else 1,
                'tlsAndToken': args.secure_metadata_quorum, 'topicQuorum': [2, 2, 2], 'checks': []}
    name = 'rivomesh-pulsar-quorum-'+secrets.token_hex(5)
    with tempfile.TemporaryDirectory(prefix=name+'-', dir=args.output.resolve().parent) as serial:
        workspace=Path(serial)
        security=generate_security(workspace,args.openssl) if args.secure_metadata_quorum else (None,())
        cluster = Cluster(name,manifest,*security)
        try:
            cluster.initialize()
            markers=workspace/'markers'
            markers.mkdir(mode=0o700)
            cluster.scenario(args,markers)
            if manifest['artifactSha256'] != sha256(args.artifact) or manifest['testClassSha256'] != sha256(test_class):
                raise RuntimeError('Test inputs changed during the run')
            manifest['status'] = 'PASS'
            for check in manifest['checks']:
                print('PASS '+check, flush=True)
        finally:
            manifest['cleanupVerified'] = cluster.cleanup(args.output)
            (args.output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    if not manifest['cleanupVerified']:
        raise RuntimeError('Fixture resources remain')


if __name__ == '__main__':
    main()
