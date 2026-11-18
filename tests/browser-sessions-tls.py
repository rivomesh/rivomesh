#!/usr/bin/env python3
"""Candidate-JAR HTTPS session acceptance, with an isolated controller and TLS proxy.
No credentials or private state are printed. Only this script's container is removed.
"""
import argparse
import hashlib
import http.client
import http.cookiejar
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import ssl
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('--jar', type=Path, default=ROOT / 'sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar')
args = parser.parse_args()
jar = args.jar.resolve()
sha = hashlib.sha256(jar.read_bytes()).hexdigest()
container = None
proxy = None
proxy_started = False


def command(*args, **kwargs):
    return subprocess.check_output(args, text=True, timeout=60, **kwargs).strip()


try:
    with tempfile.TemporaryDirectory(prefix='browser-sessions-tls-', dir=ROOT / 'target') as home:
        home = Path(home)
        os.chmod(home, 0o700)
        backend = {'port': None}

        class Proxy(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *_):
                pass

            def relay(self):
                connection = http.client.HTTPConnection('127.0.0.1', backend['port'], timeout=10)
                try:
                    headers = {key: value for key, value in self.headers.items() if key.lower() not in ('connection', 'transfer-encoding')}
                    body = self.rfile.read(int(self.headers.get('Content-Length', '0')))
                    connection.request(self.command, self.path, body=body, headers=headers)
                    response = connection.getresponse()
                    content = response.read()
                    self.send_response(response.status)
                    for key, value in response.getheaders():
                        if key.lower() not in ('connection', 'transfer-encoding', 'content-length', 'server', 'date'):
                            self.send_header(key, value)
                    self.send_header('Content-Length', str(len(content)))
                    self.end_headers()
                    self.wfile.write(content)
                finally:
                    connection.close()

            do_GET = relay
            do_POST = relay

        proxy = ThreadingHTTPServer(('127.0.0.1', 0), Proxy)
        origin = 'https://127.0.0.1:' + str(proxy.server_port)
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1', '-subj', '/CN=localhost',
                        '-addext', 'subjectAltName=IP:127.0.0.1', '-keyout', str(home / 'key.pem'), '-out', str(home / 'cert.pem')],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(home / 'cert.pem', home / 'key.pem')
        proxy.socket = context.wrap_socket(proxy.socket, server_side=True)
        token = secrets.token_urlsafe(32)
        config = {'bind': '0.0.0.0', 'port': 8080, 'authToken': '${RIVOMESH_SESSION_TOKEN}',
                  'tasksDirectory': '/test/tasks', 'dataDirectory': '/test/controller',
                  'sessions': {'secureCookie': True, 'publicOrigin': origin}}
        (home / 'platform.yml').write_text(json.dumps(config))
        environment = dict(os.environ, RIVOMESH_SESSION_TOKEN=token)
        container = command('docker', 'run', '--rm', '-d', '-e', 'RIVOMESH_SESSION_TOKEN',
                            '-p', '127.0.0.1::8080', '-v', str(jar) + ':/app/rivomesh.jar:ro',
                            '-v', str(home) + ':/test', 'eclipse-temurin:21-jre-jammy',
                            'java', '-Xmx128m', '-jar', '/app/rivomesh.jar', 'serve', '/test/platform.yml', env=environment)
        backend['port'] = int(command('docker', 'port', container, '8080/tcp').rsplit(':', 1)[1])
        deadline = time.monotonic() + 60
        while True:
            try:
                with urllib.request.urlopen('http://127.0.0.1:' + str(backend['port']) + '/readyz', timeout=2) as response:
                    assert response.status == 200
                break
            except (OSError, urllib.error.URLError):
                if time.monotonic() > deadline:
                    raise RuntimeError('Controller readiness timed out') from None
                time.sleep(0.25)
        threading.Thread(target=proxy.serve_forever, daemon=True).start()
        proxy_started = True
        trust = ssl.create_default_context(cafile=str(home / 'cert.pem'))
        cookies = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=trust), urllib.request.HTTPCookieProcessor(cookies))

        def request(path, method='GET', headers=None, base=origin):
            req = urllib.request.Request(base + path, data=b'' if method == 'POST' else None, method=method, headers=headers or {})
            try:
                response = opener.open(req, timeout=10)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                return response.status, json.loads(response.read()), response.headers

        status, identity, headers = request('/api/session', 'POST', {'Authorization': 'Bearer ' + token, 'Origin': origin})
        assert status == 200
        saved_cookie = next(iter(cookies))
        saved_id = saved_cookie.value
        csrf = identity['csrfToken']
        assert saved_cookie.name == '__Host-rivomesh-session' and saved_cookie.secure
        assert saved_cookie.has_nonstandard_attr('HttpOnly') and saved_cookie.get_nonstandard_attr('SameSite') == 'Strict'
        assert saved_cookie.discard and saved_cookie.path == '/' and not saved_cookie.domain_specified
        assert headers['Cache-Control'] == 'no-store' and token not in json.dumps(identity) and saved_id not in json.dumps(identity)
        assert request('/api/session')[0] == 200  # New navigation without Authorization.
        assert request('/api/tasks')[0] == 200
        # A real cookie jar must withhold the Secure credential over plaintext HTTP.
        assert request('/api/session', base='http://127.0.0.1:' + str(backend['port']))[0] == 401
        assert request('/api/session/logout', 'POST', {'Origin': origin})[0] == 403
        assert request('/api/session/logout', 'POST', {'Origin': 'https://evil.invalid', 'X-CSRF-Token': csrf})[0] == 403
        assert request('/api/session/logout', 'POST', {'Origin': origin, 'X-CSRF-Token': csrf})[0] == 200
        assert request('/api/session')[0] == 401
        # Replaying an old cookie after logout must fail even when CookieJar removed it.
        assert request('/api/session', headers={'Cookie': saved_cookie.name + '=' + saved_id})[0] == 401
        assert request('/api/session', headers={'Authorization': 'Bearer ' + token})[0] == 200
        for path in (home / 'controller').rglob('*'):
            if path.is_file():
                content = path.read_bytes()
                assert token.encode() not in content and saved_id.encode() not in content and csrf.encode() not in content
        print('PASS candidate-JAR TLS/cookie/refresh/CSRF/logout/Bearer/no-secret-state')
        print('artifactSha256=' + sha)
finally:
    if proxy is not None:
        if proxy_started:
            proxy.shutdown()
        proxy.server_close()
    if container is not None:
        subprocess.run(['docker', 'rm', '-f', container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
