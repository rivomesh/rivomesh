#!/usr/bin/env python3
"""Run a real Redis TLS/AOF check with an isolated host process and the packaged publisher."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time

def sha(path):
    with path.open('rb') as data:
        return hashlib.file_digest(data,'sha256').hexdigest()
def run(*args, env=None, check=True):
    return subprocess.run([str(arg) for arg in args], text=True, capture_output=True, env=env, check=check, timeout=30)
def available_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.bind(('127.0.0.1',0))
        return client.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--java-home', type=Path, default=os.environ.get('JAVA_HOME'))
    args = parser.parse_args()
    artifact, output = args.artifact.resolve(), args.output.resolve()
    java_bin = args.java_home.resolve() / 'bin' if args.java_home else Path()
    java = java_bin / 'java'
    keytool = java_bin / 'keytool'
    redis = 'redis-server'
    cli = 'redis-cli'
    fixture_class = 'io.github.rivomesh.acceptance.ReplicatedRedisAcceptance'
    source = Path(__file__).resolve().parent / 'redis/ReplicatedRedisAcceptance.java'
    stream = 'rivomesh-events'
    owners = '_rivomesh_publication_owners_v1'
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    report = {'status':'FAIL','artifactSha256':sha(artifact),
              'sourceSha256':sha(source),
              'runtimeVersion':run(java,'-version').stderr.splitlines()[0],
              'redisVersion':run(redis,'--version').stdout.strip(),'checks':[]}
    def passed(check):
        print('PASS '+check,flush=True)
        report['checks'].append(check)

    with tempfile.TemporaryDirectory(prefix='redis-tls-', dir=output) as folder:
        base=Path(folder)
        classes = base / 'classes'
        java_trust = base / 'trust.p12'
        trust_secret=secrets.token_hex(12)
        admin_secret=secrets.token_hex(20)
        publish_secret=secrets.token_hex(20)
        ca=base/'ca.crt'
        cert=base/'server.crt'
        port=available_port()
        process=None
        with (base/'openssl-host.cnf').open('w') as ext:
            ext.write('subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n')
        try:
            run(java_bin/'javac','--release','21','-cp',artifact,'-d',classes,source)
            run('openssl','req','-new','-x509','-nodes','-newkey','rsa:2048','-days','1',
                '-subj','/CN=Rivomesh Fixture CA','-keyout',base/'ca.key','-out',ca)
            run('openssl','req','-new','-nodes','-newkey','rsa:2048','-subj','/CN=localhost',
                '-keyout',base/'server.key','-out',base/'server.csr')
            run('openssl','x509','-req','-days','1','-in',base/'server.csr','-CA',ca,'-CAkey',base/'ca.key',
                '-CAcreateserial','-out',cert,'-extfile',base/'openssl-host.cnf', '-sha256')
            run(keytool,'-importcert','-noprompt','-alias','rivomesh-fixture-ca','-file',ca,'-keystore',java_trust,
                '-storetype','PKCS12','-storepass:env','RIVOMESH_REDIS_FIXTURE_TRUSTPASS',
                env=dict(os.environ,RIVOMESH_REDIS_FIXTURE_TRUSTPASS=trust_secret))
            report['caSha256']=sha(ca)
            acl=base/'users.acl'
            acl.write_text(f'user default off\nuser admin on >{admin_secret} ~* &* +@all\n'
                f'user publisher on >{publish_secret} ~{stream} ~{owners} '
                '+ping +info +select +client|id +client|setinfo +config|get +pttl +hget +hsetnx +exists '
                '+xgroup|create +type +xadd +waitaof\n')
            acl.chmod(0o600)
            config=base/'redis.conf'
            config.write_text(f'bind 127.0.0.1\nport 0\ntls-port {port}\ntls-cert-file "{cert}"\n'
                f'tls-key-file "{base/"server.key"}"\ntls-ca-cert-file "{ca}"\ntls-auth-clients no\n'
                f'aclfile "{acl}"\ndir "{base}"\nappendonly yes\nappendfsync always\nmaxmemory-policy noeviction\n'
                'save ""\n')
            config.chmod(0o600)
            admin_env=dict(os.environ,REDISCLI_AUTH=admin_secret)
            def redis_call(*args, db=2, user='admin', auth=admin_env, raw=False):
                response=run(cli,'--tls','--cacert',ca,'--sni','localhost','--user',user,
                             '-h','127.0.0.1','-p',port,'--raw' if raw else '--json','-n',db,*args,env=auth)
                return response.stdout if raw else json.loads(response.stdout)

            def boot():
                child=subprocess.Popen([redis,str(config)], stdout=log, stderr=subprocess.STDOUT)
                end=time.monotonic()+15
                try:
                    while time.monotonic()<end:
                        if child.poll() is not None:
                            raise RuntimeError('Disposable Redis process exited during startup')
                        try:
                            if redis_call('PING')=='PONG':
                                return child
                        except (subprocess.CalledProcessError, json.JSONDecodeError):
                            time.sleep(.1)
                    raise RuntimeError('Disposable Redis process did not become TLS-ready')
                except BaseException:
                    if child.poll() is None:
                        child.terminate()
                        child.wait(timeout=5)
                    raise

            def phase(name, host='localhost', trusted=True, password=publish_secret):
                options=['-Djava.net.preferIPv4Stack=true']
                if trusted:
                    options.extend([f'-Djavax.net.ssl.trustStore={java_trust}',
                                    f'-Djavax.net.ssl.trustStorePassword={trust_secret}'])
                env=dict(os.environ, RIVOMESH_TEST_REDIS_URL=f'rediss://{host}:{port}/2',
                         RIVOMESH_TEST_REDIS_USER='publisher', RIVOMESH_TEST_REDIS_PASSWORD=password,
                         RIVOMESH_TEST_REDIS_REPLICAS='1')
                result=run(java,*options,'-Xmx256m','-cp',f'{artifact}{os.pathsep}{classes}',fixture_class,name,base,env=env,check=False)
                if result.returncode:
                    (output/'product-error.log').write_text(
                        result.stderr.replace(admin_secret,'<redacted>').replace(publish_secret,'<redacted>')
                        .replace(password,'<redacted>').replace(trust_secret,'<redacted>'))
                    raise AssertionError('Packaged publisher phase '+name+' failed')
                return json.loads(result.stdout)

            def server_run_id():
                raw=redis_call('INFO','server',raw=True)
                return next(line.split(':',1)[1] for line in raw.splitlines() if line.startswith('run_id:'))

            with (output/'redis-server.log').open('w') as log:
                process=boot()
                initial_run=server_run_id()
                refused={
                    'untrustedCa':phase('connection-refused',trusted=False),
                    'ipWithoutCertificateSan':phase('connection-refused',host='127.0.0.1'),
                    'badAclPassword':phase('connection-refused',password=secrets.token_hex(20)),
                }
                assert refused['untrustedCa']['retryCode']==refused['ipWithoutCertificateSan']['retryCode']=='REDIS_CONNECTION_LOST', refused
                assert refused['badAclPassword']['retryCode']=='REDIS_COMMAND_REJECTED', refused
                assert redis_call('DBSIZE')==redis_call('DBSIZE',db=0)==0
                report['rejections']=refused
                passed('Private CA trust, certificate hostname and ACL password failures create no native keys')
                acl_denied=run(cli,'--tls','--cacert',ca,'--user','publisher','-h','127.0.0.1','-p',port,
                               'FLUSHDB',env=dict(os.environ, REDISCLI_AUTH=publish_secret))
                assert 'NOPERM' in acl_denied.stdout+acl_denied.stderr
                initial=phase('initial')
                first=redis_call('XRANGE',stream,'-','+')
                assert initial['journalAcknowledged']==1 and len(first)==7
                assert redis_call('DBSIZE',db=0)==0
                expected_owner='rivomesh:'+hashlib.sha256(b'redis-cluster:redis:fixture-generation').hexdigest()
                assert redis_call('HGET',owners,stream)==expected_owner
                passed('Authenticated restricted publisher writes a native snapshot and CDC to Redis DB 2 with AOF')
                process.kill()
                process.wait(timeout=10)
                process=None
                report['stoppedPublisher']=phase('connection-refused')
                assert report['stoppedPublisher']['retryCode']=='REDIS_CONNECTION_LOST'
                process=boot()
                assert server_run_id()!=initial_run
                assert redis_call('XRANGE',stream,'-','+')==first
                assert redis_call('HGET',owners,stream)==expected_owner
                after=phase('after-cold-restart')
                replay=phase('restarted')
                final=redis_call('XRANGE',stream,'-','+')
                assert len(final)==10 and final[:7]==first
                assert after['snapshotId']==replay['snapshotId']==initial['snapshotId']
                assert after['publicationReceipt']==replay['publicationReceipt']
                assert after['journalAcknowledged']==2 and redis_call('DBSIZE',db=0)==0
                report['unchangedSnapshotId']=after['snapshotId']
                report['nativeEntries']=[len(first),len(final)]
                passed('Cold AOF restart preserves native stream ids and original snapshot while JDBC ACK reaches 2')
            report['status']='PASS'
        finally:
            if process and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            report['fixtureProcessGone'] = process is None or process.poll() is not None
            (output/'manifest.json').write_text(json.dumps(report,indent=2)+'\n')



if __name__ == "__main__":
    main()
