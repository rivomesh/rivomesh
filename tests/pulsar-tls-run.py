#!/usr/bin/env python3
"""Verify the isolated release transport against one disposable TLS and JWT Pulsar broker."""
import argparse
import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--artifact', required=True, type=Path)
parser.add_argument('--test-classes', required=True, type=Path)
parser.add_argument('--output', required=True, type=Path)
parser.add_argument('--openssl', default='openssl')
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=True)
name = 'rivomesh-pulsar-tls-' + secrets.token_hex(5)
broker = name + '-broker'
runner = name + '-test'
image = 'apachepulsar/pulsar:4.0.14'
manifest = {'status': 'FAIL', 'brokerImage': image, 'checks': [],
            'artifactSha256': hashlib.sha256(args.artifact.read_bytes()).hexdigest(),
            'harnessSourceSha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}

def execute(*command, check=True, timeout=90):
    return subprocess.run(command, check=check, timeout=timeout, capture_output=True, text=True)

def docker(*command, **kwargs):
    return execute('docker', *command, **kwargs)

def encode(data):
    return base64.urlsafe_b64encode(data).rstrip(b'=')

with tempfile.TemporaryDirectory(prefix=name+'-',dir=args.output.resolve().parent) as temp:
    folder = Path(temp)
    materials = folder/'credentials'
    markers = folder/'markers'
    materials.mkdir(mode=0o700)
    markers.mkdir(mode=0o700)
    key = secrets.token_bytes(32)
    unsigned = encode(b'{"alg":"HS256"}') + b'.' + encode(b'{"sub":"rivomesh-fixture"}')
    token = (unsigned+b'.'+encode(hmac.digest(key,unsigned,'sha256'))).decode()
    (materials/'secret.key').write_bytes(key)
    (materials/'token').write_text(token)
    (materials/'bearer.header').write_text('Authorization: Bearer '+token+'\n')
    for file in materials.iterdir():
        file.chmod(0o600)
    os.environ['LC_ALL'] = 'C'
    for ca in ('ca','untrusted'):
        execute(args.openssl,'req','-x509','-newkey','rsa:2048','-nodes','-days','2',
                '-subj','/CN=Rivomesh disposable '+ca,
                '-addext','basicConstraints=critical,CA:TRUE',
                '-keyout',str(folder/(ca+'.key')),'-out',str(materials/(ca+'.pem')))
    execute(args.openssl,'req','-new','-newkey','rsa:2048','-nodes','-subj','/CN=pulsar',
            '-keyout',str(materials/'server.key'),'-out',str(folder/'pulsar.csr'))
    ext = folder/'server-ext'
    ext.write_text('subjectAltName=DNS:pulsar\nextendedKeyUsage=serverAuth\n')
    execute(args.openssl,'x509','-req','-in',str(folder/'pulsar.csr'),'-CA',str(materials/'ca.pem'),
            '-CAkey',str(folder/'ca.key'),'-CAserial',str(folder/'serial'),'-CAcreateserial','-days','2',
            '-extfile',str(ext),'-out',str(materials/'server.pem'))
    config = {
        'advertisedAddress':'pulsar', 'brokerServicePort':'', 'webServicePort':'',
        'brokerServicePortTls':'6651','webServicePortTls':'8443',
        'tlsTrustCertsFilePath':'/fixture/ca.pem', 'tlsCertificateFilePath':'/fixture/server.pem',
        'tlsKeyFilePath':'/fixture/server.key','brokerClientTlsEnabled':'true',
        'brokerClientTrustCertsFilePath':'/fixture/ca.pem',
        'authenticationEnabled':'true','authorizationEnabled':'true','superUserRoles':'rivomesh-fixture',
        'authenticationProviders':'org.apache.pulsar.broker.authentication.AuthenticationProviderToken',
        'tokenSecretKey':'file:///fixture/secret.key',
        'brokerClientAuthenticationPlugin':'org.apache.pulsar.client.impl.auth.AuthenticationToken',
        'brokerClientAuthenticationParameters':'file:///fixture/token',
        'allowAutoTopicCreation':'false','brokerDeleteInactiveTopicsEnabled':'false',
        'topicLevelPoliciesEnabled':'true','systemTopicEnabled':'true',
        'backlogQuotaDefaultRetentionPolicy':'producer_exception',
        'numIOThreads':'2','numHttpServerThreads':'4','numOrderedExecutorThreads':'2',
    }
    def sanitize(text):
        return text.replace(token,'[REDACTED]').replace(base64.b64encode(key).decode(),'[REDACTED]')

    def ready():
        limit=time.monotonic()+120
        while time.monotonic()<limit:
            probe=docker('exec',broker,'curl','--max-time','3','--cacert','/fixture/ca.pem',
                         '--fail','--silent','--header','@/fixture/bearer.header',
                         'https://pulsar:8443/admin/v2/namespaces/public/default',check=False,timeout=8)
            if probe.returncode==0:return
            time.sleep(1)
        raise AssertionError('Pulsar TLS admin readiness timed out')

    test = None
    try:
        docker('network','create',name)
        environment = [item for key,value in config.items() for item in ('-e','PULSAR_PREFIX_'+key+'='+value)]
        docker('run','-d','--name',broker,'--network',name,'--network-alias','pulsar','--network-alias','pulsar-other',
               '--user','0','-e','PULSAR_MEM=-Xms256m -Xmx512m -XX:MaxDirectMemorySize=256m',
               *environment,'-v',str(materials)+':/fixture:ro',image,'bash','-c',
               'bin/apply-config-from-env.py conf/standalone.conf && exec bin/pulsar standalone --no-functions-worker --no-stream-storage')
        manifest['brokerImageId']=docker('inspect','--format','{{.Image}}',broker).stdout.strip()
        ready()
        with (args.output/'transport.log').open('w') as log:
            test=subprocess.Popen(['docker','run','--rm','--name',runner,'--network',name,'--user','0',
                  '-v',str(materials)+':/fixture:ro','-v',str(markers)+':/markers',
                  '-v',str(args.artifact.resolve())+':/app/rivomesh-app.jar:ro',
                  '-v',str(args.test_classes.resolve())+':/test-classes:ro',
                  '-e','RIVOMESH_PULSAR_RESTART_DIR=/markers','eclipse-temurin:21-jre',
                  'java','-cp','/app/rivomesh-app.jar:/test-classes',
                  'io.github.rivomesh.transport.pulsar.PulsarTransportAcceptance','tls'],
                  stdout=log,stderr=subprocess.STDOUT)
            deadline=time.monotonic()+240
            while test.poll() is None and time.monotonic()<deadline:
                if (markers/'restart.request').exists() and not (markers/'restart.done').exists():
                    docker('restart',broker)
                    ready()
                    (markers/'restart.done').write_text('done')
                time.sleep(1)
        result=sanitize((args.output/'transport.log').read_text())
        (args.output/'transport.log').write_text(result)
        manifest['checks']=[line[5:] for line in result.splitlines() if line.startswith('PASS ')]
        for check in manifest['checks']:
            print('PASS '+check,flush=True)
        if test.poll()!=0 or len(manifest['checks'])!=3:
            raise AssertionError('Pulsar TLS failed. See the redacted transport log.')
        assert manifest['artifactSha256']==hashlib.sha256(args.artifact.read_bytes()).hexdigest()
        manifest['status']='PASS'
    finally:
        if test is not None and test.poll() is None:
            docker('rm','-f',runner,check=False)
            test.wait(timeout=10)
        transport_log=args.output/'transport.log'
        if transport_log.exists():
            transport_log.write_text(sanitize(transport_log.read_text()))
        if manifest['status']!='PASS':
            logs=docker('logs','--tail','180',broker,check=False)
            (args.output/'broker-redacted.log').write_text(sanitize(logs.stdout+logs.stderr))
        docker('rm','-f',broker,check=False)
        removed=docker('network','rm',name,check=False)
        manifest['cleanupExitCode']=removed.returncode
        (args.output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
