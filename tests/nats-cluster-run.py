#!/usr/bin/env python3
"""Verify Rivomesh transport against three native TLS JetStream replicas."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
NATS = 'nats:2.11.8-alpine'
JAVA = 'maven:3.9-eclipse-temurin-21'
MEMBERS = ('nats-a', 'nats-b', 'nats-c')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    artifact = args.artifact.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    token = secrets.token_hex(24)
    environment = dict(os.environ, RIVOMESH_NATS_FIXTURE_TOKEN=token)
    name = 'rivomesh-nats-quorum-' + secrets.token_hex(5)
    report = {'status':'FAIL','artifactSha256':digest,'sourceImage':NATS,'javaImage':JAVA,'checks':[]}

    def run(*command, env=None, check=True):
        return subprocess.run(command,cwd=ROOT,env=env,text=True,capture_output=True,check=check,timeout=180)

    def passed(label):
        report['checks'].append(label)
        print('PASS '+label,flush=True)

    with tempfile.TemporaryDirectory(prefix='nats-quorum-',dir=output) as directory:
        fixture = Path(directory)
        run('openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj','/CN=Rivomesh NATS fixture',
            '-addext','subjectAltName=DNS:nats-a,DNS:nats-b,DNS:nats-c',
            '-keyout',str(fixture/'key.pem'),'-out',str(fixture/'cert.pem'))
        for member in MEMBERS:
            routes = ', '.join('nats://'+peer+':6222' for peer in MEMBERS if peer != member)
            (fixture/(member+'.conf')).write_text(
                f'server_name: {member}\nport: 4222\n'
                'jetstream {store_dir: "/data", max_mem_store: 64MB, max_file_store: 128MB}\n'
                f'cluster {{name: rivomesh_native, listen: "0.0.0.0:6222", routes: [{routes}]}}\n'
                'tls {cert_file: "/fixture/cert.pem", key_file: "/fixture/key.pem"}\n'
                'authorization {token: $RIVOMESH_NATS_FIXTURE_TOKEN}\n')

        def docker(*arguments, check=True):
            return run('docker',*arguments,env=environment,check=check)

        def java(*arguments):
            return docker('run','--rm','--name',name+'-client','--network',name,'-e','RIVOMESH_NATS_FIXTURE_TOKEN',
                          '-v',str(fixture)+':/fixture','-v',str(artifact)+':/app/rivomesh.jar:ro',
                          '-v',str(ROOT/'tests/nats/ReplicatedJetStreamAcceptance.java')+':/app/Test.java:ro',
                          '--entrypoint','sh',JAVA,'-ec',*arguments)

        def phase(value):
            trusted = '' if value == 'untrusted' else '-Djavax.net.ssl.trustStore=/fixture/trust.jks -Djavax.net.ssl.trustStorePassword=fixtureonly'
            result = java(f'java {trusted} -cp /app/rivomesh.jar:/fixture io.github.rivomesh.acceptance.ReplicatedJetStreamAcceptance {value}')
            return result.stdout.strip().removeprefix('LEADER ')

        cleanup = []
        try:
            docker('network','create',name)
            for member in MEMBERS:
                docker('volume','create',name+'-'+member)
                docker('run','-d','--name',name+'-'+member,'--label','io.github.rivomesh.test=nats-quorum',
                       '--network',name,'--network-alias',member,'--network-alias',member.replace('nats-', 'alias-'),
                       '-e','RIVOMESH_NATS_FIXTURE_TOKEN',
                       '-v',str(fixture)+':/fixture:ro','-v',name+'-'+member+':/data',
                       NATS,'-c','/fixture/'+member+'.conf')
            report['sourceImageId']=docker('image','inspect',NATS,'--format','{{.Id}}').stdout.strip()
            report['javaVersion']=java('java -version').stderr.strip()
            java('mkdir -p /fixture/java && cp /app/Test.java /fixture/java/ReplicatedJetStreamAcceptance.java '
                 '&& javac -cp /app/rivomesh.jar -d /fixture /fixture/java/ReplicatedJetStreamAcceptance.java '
                 '&& keytool -importcert -noprompt -alias fixture -file /fixture/cert.pem -keystore /fixture/trust.jks -storepass fixtureonly')
            leader = phase('initial')
            assert leader in MEMBERS
            passed('Three file-backed replicas confirm and retain the original content over trusted client TLS')
            phase('untrusted')
            passed('The same endpoints refuse a JVM without the fixture trust anchor')
            phase('wrong-host')
            passed('The same certificate is rejected if its DNS names omit the configured entrance')
            docker('kill',name+'-'+leader)
            promoted = phase('promoted')
            assert promoted in MEMBERS and promoted != leader
            passed('Killing the stream leader elects a new leader and retains earlier message bytes')
            other = next(member for member in MEMBERS if member not in (leader,promoted))
            docker('kill',name+'-'+other)
            phase('no-quorum')
            passed('The surviving replica does not confirm a new write without a majority')
            docker('start',name+'-'+leader,name+'-'+other)
            assert phase('restored') in MEMBERS
            passed('Restoring quorum confirms the original event ID once and preserves message sequence')
            assert hashlib.sha256(artifact.read_bytes()).hexdigest()==digest
            report['status']='PASS'
        except subprocess.CalledProcessError as error:
            details=(error.stdout+'\n'+error.stderr).replace(token,'[REDACTED]')
            (output/'failure.log').write_text(details)
            raise
        finally:
            docker('rm','-f',name+'-client',check=False)
            for member in MEMBERS:
                cleanup.append(docker('rm','-f',name+'-'+member,check=False).returncode)
                cleanup.append(docker('volume','rm',name+'-'+member,check=False).returncode)
            cleanup.append(docker('network','rm',name,check=False).returncode)
            report['cleanupExitCode']=max(cleanup)
            (output/'manifest.json').write_text(json.dumps(report,indent=2)+'\n')
            if report['cleanupExitCode'] and report['status']=='PASS':
                raise RuntimeError('NATS fixture cleanup failed')


if __name__ == '__main__':
    main()
