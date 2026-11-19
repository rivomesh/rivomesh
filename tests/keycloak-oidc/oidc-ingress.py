"""Disposable HTTPS frontend. Retain response counts only, without URLs or headers."""
from collections import Counter
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import ssl
from threading import Lock
from urllib.parse import urlsplit

COUNTS = Counter()
LOCK = Lock()
NO_FORWARD = {'server', 'date', 'connection', 'content-length', 'transfer-encoding',
              'upgrade', 'proxy-connection', 'keep-alive', 'x-rivomesh-fixture-entry'}


class Ingress(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def reply(self, status, body, headers=()):
        self.send_response(status)
        for key, value in headers:
            if key.lower() not in NO_FORWARD:
                self.send_header(key, value)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
            pass

    def relay(self):
        route = urlsplit(self.path)
        if route.scheme or route.netloc:
            self.reply(400, b'')
            return
        if route.path == '/__fixture/oidc-counts' and self.command == 'GET':
            with LOCK:
                snapshot = dict(COUNTS)
            self.reply(200, json.dumps(snapshot).encode(), [('Content-Type', 'application/json')])
            return
        forced = self.headers.get('X-Rivomesh-Fixture-Entry', '')
        if forced not in ('', 'a', 'b'):
            self.reply(400, b'')
            return
        stage = {'/auth/oidc/start': 'start', '/auth/oidc/callback': 'callback'}.get(route.path)
        entry = 'b' if stage == 'callback' else 'a' if stage == 'start' else forced or 'a'
        try:
            length = int(self.headers.get('Content-Length', '0'))
            if not 0 <= length <= 1024 * 1024 or self.headers.get('Transfer-Encoding'):
                self.reply(400, b'')
                return
        except ValueError:
            self.reply(400, b'')
            return
        headers = {key: value for key, value in self.headers.items() if key.lower() not in NO_FORWARD}
        upstream = HTTPConnection('controller-' + entry, 8080, timeout=15)
        try:
            upstream.request(self.command, self.path, self.rfile.read(length) if length else None, headers)
            response = upstream.getresponse()
            body = response.read(4 * 1024 * 1024 + 1)
            if len(body) > 4 * 1024 * 1024:
                self.reply(502, b'')
                return
            if stage:
                with LOCK:
                    COUNTS['.'.join((entry, stage, str(response.status)))] += 1
            self.reply(response.status, body, response.getheaders())
        except OSError:
            self.reply(503, b'')
        finally:
            upstream.close()

    do_GET = do_POST = do_PUT = do_DELETE = relay


context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.minimum_version = ssl.TLSVersion.TLSv1_2
context.load_cert_chain('/test/tls/console.pem', '/test/tls/console.key')
server = ThreadingHTTPServer(('0.0.0.0', 8443), Ingress)
server.socket = context.wrap_socket(server.socket, server_side=True)
server.serve_forever()
