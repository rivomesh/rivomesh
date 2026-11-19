#!/usr/bin/env python3
"""Actual Keycloak code flow through two Temurin 21 Controllers behind one HTTPS ingress."""
import argparse
import base64
from datetime import datetime, timezone
from hashlib import sha256
import http.client
import json
import os
from pathlib import Path
import secrets
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from uuid import uuid4

FIXTURE = Path(__file__).resolve().parent
CONSOLE = 'https://console.rivomesh.test:8443'
PROVIDER = 'https://identity.rivomesh.test:8443'
REALM = 'rivomesh-oidc-fixture'
ISSUER = PROVIDER + '/realms/' + REALM
DISCOVERY = '/realms/' + REALM + '/.well-known/openid-configuration'
CLIENT = 'rivomesh-console'
CALLBACK = CONSOLE + '/auth/oidc/callback'


class FixtureFailure(RuntimeError):
    pass


def command(*arguments, stage, timeout=120, env=None, input=None):
    try:
        result = subprocess.run(arguments, text=True, env=env, input=input, timeout=timeout, capture_output=True)
    except (OSError, subprocess.TimeoutExpired):
        raise FixtureFailure(stage) from None
    if result.returncode:
        raise FixtureFailure(stage)
    return result.stdout


def write_json(path, value, mode=0o600):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(fd, 'w') as output:
        json.dump(value, output)


def line_hash(path):
    with path.open('rb') as stream:
        return sha256(stream.read()).hexdigest()


def certificates(home, jdk):
    directory = home / 'tls'
    directory.mkdir(0o755)
    root = directory / 'root.pem'
    root_key = home / 'root.key'
    command('openssl', 'req', '-new', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '2',
            '-subj', '/CN=Rivomesh disposable test root', '-addext', 'basicConstraints=critical,CA:TRUE',
            '-addext', 'keyUsage=critical,keyCertSign,cRLSign', '-keyout', str(root_key), '-out', str(root),
            stage='GENERATE_FIXTURE_CA')
    for name in ('console', 'identity'):
        host = name + '.rivomesh.test'
        key = directory / (name + '.key')
        cert = directory / (name + '.pem')
        request = home / (name + '.csr')
        extensions = home / (name + '.ext')
        extensions.write_text('basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\n'
                              'extendedKeyUsage=serverAuth\nsubjectAltName=DNS:' + host + ',IP:127.0.0.1\n')
        command('openssl', 'req', '-new', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=' + host,
                '-keyout', str(key), '-out', str(request), stage='GENERATE_FIXTURE_TLS_KEY')
        command('openssl', 'x509', '-req', '-in', str(request), '-CA', str(root), '-CAkey', str(root_key),
                '-CAcreateserial', '-days', '2', '-sha256', '-extfile', str(extensions), '-out', str(cert),
                stage='SIGN_FIXTURE_TLS_LEAF')
        command('openssl', 'verify', '-CAfile', str(root), '-verify_hostname', host, str(cert),
                stage='VERIFY_FIXTURE_TLS_LEAF')
        # The ephemeral parent directory is owner-only on the host. Keycloak's non-root UID
        # receives only its explicit file mounts inside the test network.
        key.chmod(0o644)
    trust = directory / 'trust.p12'
    command(str(jdk / 'bin/keytool'), '-importcert', '-noprompt', '-alias', 'fixture-root', '-file',
            str(root), '-keystore', str(trust), '-storepass', 'changeit', '-storetype', 'PKCS12',
            stage='IMPORT_JVM_CA')
    root_key.unlink()
    return ssl.create_default_context(cafile=str(root))


def der(tag, content):
    size = len(content)
    if size < 128:
        length = bytes([size])
    else:
        octets = size.to_bytes((size.bit_length() + 7) // 8, 'big')
        length = bytes([128 + len(octets)]) + octets
    return bytes([tag]) + length + content


def rsa_spki(key):
    def integer(field):
        text = key[field]
        value = base64.urlsafe_b64decode(text + '=' * (-len(text) % 4)).lstrip(b'\0')
        if not value:
            raise FixtureFailure('INVALID_KEYCLOAK_RSA_KEY')
        return der(2, (b'\0' if value[0] & 128 else b'') + value)

    rsa = der(48, integer('n') + integer('e'))
    algorithm = bytes.fromhex('300d06092a864886f70d0101010500')
    encoded = der(48, algorithm + der(3, b'\0' + rsa))
    return base64.b64encode(encoded).decode('ascii')


def realm(secret, subject, password):
    return {
        'realm': REALM, 'enabled': True, 'sslRequired': 'all', 'registrationAllowed': False,
        'accessTokenLifespan': 300,
        'clients': [{
            'clientId': CLIENT, 'enabled': True, 'protocol': 'openid-connect', 'publicClient': False,
            'clientAuthenticatorType': 'client-secret', 'secret': secret, 'standardFlowEnabled': True,
            'directAccessGrantsEnabled': False, 'implicitFlowEnabled': False, 'serviceAccountsEnabled': False,
            'redirectUris': [CALLBACK], 'webOrigins': [CONSOLE],
            'attributes': {'pkce.code.challenge.method': 'S256', 'id.token.signed.response.alg': 'RS256'},
            'protocolMappers': [{
                'name': 'fixture-admin-looking-claim', 'protocol': 'openid-connect',
                'protocolMapper': 'oidc-hardcoded-claim-mapper',
                'config': {'claim.name': 'role', 'claim.value': 'ADMIN', 'jsonType.label': 'String',
                           'id.token.claim': 'true', 'access.token.claim': 'false'}
            }]
        }],
        'users': [{
            'id': subject, 'username': 'fixture-user', 'enabled': True, 'emailVerified': True,
            'firstName': 'Disposable', 'lastName': 'Tester', 'email': 'fixture-user@example.invalid',
            'credentials': [{'type': 'password', 'value': password, 'temporary': False}]
        }]
    }


def platform(member, profiles):
    return {
        'bind': '0.0.0.0', 'port': 8080, 'authToken': '${RM_OIDC_ADMIN}', 'pollMillis': 250, 'autoStart': False,
        'tasksDirectory': '/test/state/tasks', 'dataDirectory': '/test/state/controller',
        'sessions': {'enabled': True, 'secureCookie': True, 'publicOrigin': CONSOLE},
        'oidc': {'issuer': ISSUER, 'authorizationEndpoint': profiles['authorization_endpoint'],
                 'tokenEndpoint': profiles['token_endpoint'], 'clientId': CLIENT, 'clientSecret': '${RM_OIDC_CLIENT}',
                 'keyId': profiles['kid'], 'rsaPublicKey': profiles['spki']},
        'cluster': {'id': 'fixture-global', 'nodeId': member, 'sharedAccess': True, 'sharedTasks': True,
                    'taskKey': '${RM_OIDC_TASK_KEY}', 'url': 'jdbc:postgresql://store:5432/fixture',
                    'username': 'postgres', 'password': '${RM_OIDC_STORE}', 'namespace': 'fixture_control',
                    'heartbeatMillis': 1000, 'leaseMillis': 15000}
    }


def composition(home, artifact, options):
    tls = home / 'tls'
    services = {
        'store': {'image': options.postgres_image, 'environment': {'POSTGRES_DB': 'fixture',
                  'POSTGRES_PASSWORD': '${RM_OIDC_STORE:?required}'},
                  'healthcheck': {'test': ['CMD', 'pg_isready', '-U', 'postgres', '-d', 'fixture'],
                                  'interval': '2s', 'timeout': '3s', 'retries': 45}},
        'keycloak': {'image': options.keycloak_image, 'ports': ['127.0.0.1::8443'],
                     'networks': {'default': {'aliases': ['identity.rivomesh.test']}},
                     'environment': {'JAVA_OPTS_APPEND': '-Xms128m -Xmx768m'},
                     'command': ['start-dev', '--import-realm', '--hostname=' + PROVIDER, '--http-enabled=false',
                                 '--https-certificate-file=/cert.pem', '--https-certificate-key-file=/cert.key'],
                     'volumes': [str(home / 'realm.json') + ':/opt/keycloak/data/import/' + REALM + '-realm.json:ro',
                                 str(tls / 'identity.key') + ':/cert.key:ro', str(tls / 'identity.pem') + ':/cert.pem:ro']},
        'ingress': {'image': options.ingress_image, 'ports': ['127.0.0.1::8443'],
                    'networks': {'default': {'aliases': ['console.rivomesh.test']}},
                    'command': ['python3', '/test/oidc-ingress.py'],
                    'volumes': [str(FIXTURE / 'oidc-ingress.py') + ':/test/oidc-ingress.py:ro', str(tls) + ':/test/tls:ro']},
        'browser': {'image': options.browser_image, 'environment': {'NODE_EXTRA_CA_CERTS': '/test/root.pem', 'RM_OIDC_FIXTURE_BROWSER': '1'},
                    'security_opt': ['no-new-privileges:true'],
                    'entrypoint': ['node', '/test/oidc-browser.mjs'],
                    'volumes': [str(FIXTURE / name) + ':/test/' + name + ':ro'
                                for name in ('chromium.mjs', 'oidc-browser.mjs')]
                               + [str(tls / 'root.pem') + ':/test/root.pem:ro']}
    }
    for member in ('a', 'b'):
        name = 'controller-' + member
        services[name] = {
            'image': options.runtime_image, 'user': f'{os.getuid()}:{os.getgid()}',
            'command': ['java', '-Xmx256m', '-Djavax.net.ssl.trustStore=/test/trust.p12',
                        '-Djavax.net.ssl.trustStorePassword=changeit', '-jar', '/test/rivomesh.jar',
                        'serve', '/test/platform.json'],
            'environment': {key: '${' + key + ':?required}'
                            for key in ('RM_OIDC_STORE', 'RM_OIDC_ADMIN', 'RM_OIDC_CLIENT', 'RM_OIDC_TASK_KEY')},
            'volumes': [str(artifact) + ':/test/rivomesh.jar:ro',
                        str(home / (name + '.json')) + ':/test/platform.json:ro',
                        str(home / name) + ':/test/state', str(tls / 'trust.p12') + ':/test/trust.p12:ro'],
            'depends_on': {'store': {'condition': 'service_healthy'}}
        }
    return {'services': services}


def read_json(opener, url, headers=None):
    request = urllib.request.Request(url, headers=headers or {})
    with opener.open(request, timeout=5) as response:
        content = response.read(256 * 1024 + 1)
        if len(content) > 256 * 1024:
            raise FixtureFailure('FIXTURE_RESPONSE_TOO_LARGE')
        return json.loads(content)


def ready(check, stage, seconds=180):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (OSError, urllib.error.URLError, ValueError, http.client.HTTPException):
            pass
        time.sleep(1)
    raise FixtureFailure(stage)


def probe_provider(opener, port):
    local = 'https://127.0.0.1:' + port
    profile = ready(lambda: read_json(opener, local + DISCOVERY), 'WAIT_FOR_KEYCLOAK')
    prefix = ISSUER + '/protocol/openid-connect/'
    if profile.get('issuer') != ISSUER or profile.get('authorization_endpoint') != prefix + 'auth' \
            or profile.get('token_endpoint') != prefix + 'token' or profile.get('jwks_uri') != prefix + 'certs' \
            or 'S256' not in profile.get('code_challenge_methods_supported', []) \
            or 'client_secret_basic' not in profile.get('token_endpoint_auth_methods_supported', []):
        raise FixtureFailure('KEYCLOAK_PROFILE_MISMATCH')
    jwks = read_json(opener, local + '/realms/' + REALM + '/protocol/openid-connect/certs')
    matches = [key for key in jwks['keys'] if key.get('kty') == 'RSA' and key.get('alg') == 'RS256'
               and key.get('use') == 'sig']
    if len(matches) != 1:
        raise FixtureFailure('KEYCLOAK_RSA_SIGNING_KEY_REQUIRED')
    return {**profile, 'kid': matches[0]['kid'], 'spki': rsa_spki(matches[0])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact', required=True, type=Path)
    parser.add_argument('--jdk-home', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    browser = parser.add_mutually_exclusive_group()
    browser.add_argument('--browser-image', help='An existing fixture browser image. The harness leaves it in place.')
    browser.add_argument('--build-browser', action='store_true', help='Build a uniquely tagged browser image and remove it after this run.')
    parser.add_argument('--runtime-image', default='eclipse-temurin:21-jre-jammy')
    parser.add_argument('--keycloak-image', default='quay.io/keycloak/keycloak:26.7.4')
    parser.add_argument('--postgres-image', default='postgres:17.12')
    parser.add_argument('--ingress-image', default='python:3.13-slim')
    parser.add_argument('--prepare-only', action='store_true', help='Check certificates and configuration without using Docker')
    options = parser.parse_args()
    artifact = options.artifact.resolve(strict=True)
    jdk = options.jdk_home.resolve(strict=True)
    if 'JAVA_VERSION="21.' not in (jdk / 'release').read_text():
        parser.error('--jdk-home must point to JDK 21')
    output = options.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    report = {'status': 'FAIL', 'artifactSha256': line_hash(artifact), 'keycloakImage': options.keycloak_image,
              'runtimeImage': options.runtime_image, 'checks': [], 'fixtureImageDigests': {},
              'fixtureSourcesSha256': {name: line_hash(FIXTURE / name) for name in
                                      ('keycloak-oidc.py', 'oidc-ingress.py', 'oidc-browser.mjs',
                                       'chromium.mjs', 'Browser.Dockerfile')}}
    credentials = {key: secrets.token_urlsafe(40)
                   for key in ('RM_OIDC_STORE', 'RM_OIDC_ADMIN', 'RM_OIDC_CLIENT', 'viewerToken', 'providerPassword')}
    credentials['RM_OIDC_TASK_KEY'] = base64.b64encode(secrets.token_bytes(32)).decode('ascii')
    credentials['providerSubject'] = str(uuid4())
    env = dict(os.environ, **{key: value for key, value in credentials.items() if key.startswith('RM_')})
    project = 'rivomesh-keycloak-oidc-' + uuid4().hex[:10]
    options.browser_image = project + ':chromium' if options.build_browser else (
        options.browser_image or 'rivomesh-oidc-browser:fixture')
    browser_built = False
    try:
        with tempfile.TemporaryDirectory(prefix=project + '-', dir=output) as directory:
            home = Path(directory)
            trust = certificates(home, jdk)
            report['checks'].append('One trusted ephemeral root signs both distinct HTTPS origins')
            write_json(home / 'realm.json', realm(credentials['RM_OIDC_CLIENT'], credentials['providerSubject'],
                                                  credentials['providerPassword']), mode=0o644)
            for member in ('controller-a', 'controller-b'):
                (home / member).mkdir()
            spec = composition(home, artifact, options)
            if options.prepare_only:
                assert set(spec['services']) == {'store', 'keycloak', 'ingress', 'browser', 'controller-a', 'controller-b'}
                assert all(port.startswith('127.0.0.1::') for service in spec['services'].values()
                           for port in service.get('ports', []))
                report['checks'].append('Only the IdP and ingress publish disposable loopback ports')
                report['status'] = 'PREPARED'
                return 0
            if options.build_browser:
                command('docker', 'build', '-f', str(FIXTURE / 'Browser.Dockerfile'),
                        '--label', 'io.github.rivomesh.fixture=' + project,
                        '-t', options.browser_image, str(FIXTURE), stage='BUILD_FIXTURE_BROWSER', timeout=900)
                browser_built = True
            compose_file = home / 'compose.json'
            write_json(compose_file, spec)
            compose = ['docker', 'compose', '-p', project, '-f', str(compose_file)]
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=trust))

            def docker(*args, stage, timeout=210, input=None):
                return command(*compose, *args, stage=stage, env=env, timeout=timeout, input=input)

            def port(name):
                address = docker('port', name, '8443', stage='FIXTURE_LOOPBACK_PORT').strip()
                if not address.startswith('127.0.0.1:') or '\n' in address:
                    raise FixtureFailure('FIXTURE_PORT_NOT_LOOPBACK')
                return address.rsplit(':', 1)[-1]

            try:
                docker('up', '-d', 'store', 'keycloak', stage='START_KEYCLOAK_AND_STORE')
                profile = probe_provider(opener, port('keycloak'))
                for member in ('a', 'b'):
                    write_json(home / ('controller-' + member + '.json'), platform(member, profile))
                report['checks'].append('Keycloak discovery matches fixed TLS endpoints and provides its RS256 public key')
                docker('up', '-d', 'controller-a', 'controller-b', 'ingress', stage='START_CONTROLLERS_AND_INGRESS')
                base = 'https://127.0.0.1:' + port('ingress')
                for member in ('a', 'b'):
                    ready(lambda: read_json(opener, base + '/readyz', {'X-Rivomesh-Fixture-Entry': member}).get('status') == 'UP',
                          'WAIT_FOR_CONTROLLER_' + member.upper())
                    runtime = docker('exec', '-T', 'controller-' + member, 'java', '--version', stage='VERIFY_JVM_RUNTIME')
                    if not runtime.startswith('openjdk 21.') or 'Temurin' not in runtime:
                        raise FixtureFailure('EXPECTED_TEMURIN_21')
                report['checks'].append('Both actual ingress Controller routes run the same Temurin 21 JAR')
                payload = {'console': CONSOLE, 'issuer': ISSUER, 'clientId': CLIENT,
                           'adminToken': credentials['RM_OIDC_ADMIN'], 'clientSecret': credentials['RM_OIDC_CLIENT'],
                           **{key: credentials[key] for key in ('viewerToken', 'providerPassword', 'providerSubject')}}
                raw = docker('run', '--rm', '-T', '--no-deps', 'browser', stage='REAL_CHROME_KEYCLOAK_FLOW',
                             timeout=300, input=json.dumps(payload))
                browser = json.loads(raw)
                if browser.get('status') != 'PASS':
                    report['browser'] = browser
                    raise FixtureFailure('REAL_CHROME_KEYCLOAK_FLOW')
                report['browser'] = browser
                report['checks'].extend(browser['checks'])
                for image in dict.fromkeys(service['image'] for service in spec['services'].values()):
                    report['fixtureImageDigests'][image] = command('docker', 'image', 'inspect', image, '--format', '{{.Id}}',
                                                                  stage='RECORD_FIXTURE_IMAGE').strip()
                if line_hash(artifact) != report['artifactSha256']:
                    raise FixtureFailure('CANDIDATE_JAR_CHANGED')
                report['status'] = 'PASS'
            finally:
                try:
                    docker('down', '-v', '--remove-orphans', stage='CLEANUP_FIXTURE', timeout=100)
                    report['cleanupExitCode'] = 0
                except FixtureFailure:
                    report['cleanupExitCode'] = 1
                    report['status'] = 'FAIL'
                    raise
    except FixtureFailure as error:
        report['failureStage'] = str(error)
    except (ValueError, KeyError, TypeError, OSError):
        report['failureStage'] = 'FIXTURE_METADATA_INVALID'
    finally:
        if browser_built:
            try:
                label = command('docker', 'image', 'inspect', options.browser_image, '--format',
                                '{{index .Config.Labels "io.github.rivomesh.fixture"}}',
                                stage='CHECK_FIXTURE_IMAGE_OWNER').strip()
                if label != project:
                    raise FixtureFailure('FIXTURE_IMAGE_OWNER_CHANGED')
                command('docker', 'image', 'rm', options.browser_image, stage='CLEANUP_BROWSER_IMAGE')
                report['browserImageCleanupExitCode'] = 0
            except FixtureFailure as error:
                report['browserImageCleanupExitCode'] = 1
                report['status'] = 'FAIL'
                report.setdefault('failureStage', str(error))
        report['finishedAt'] = datetime.now(timezone.utc).isoformat()
        (output / 'manifest.json').write_text(json.dumps(report, indent=2) + '\n')
        print(report['status'], report.get('failureStage', 'KEYCLOAK_OIDC'), 'artifact=' + report['artifactSha256'])
    return 0 if report['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
