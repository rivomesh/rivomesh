#!/usr/bin/env python3
"""Check the running local preview without creating/changing tasks or accounts."""
import hashlib
import http.cookiejar
import json
from pathlib import Path
import urllib.error
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[1]
PREVIEW_HOME = ROOT / 'target/development-preview'
credentials = json.loads((PREVIEW_HOME / 'credentials.json').read_text())
base = credentials['url']
assert base == 'http://127.0.0.1:18088', 'Only the local development preview is supported'


def get(path, authenticated=False):
    headers = {'Authorization': 'Bearer ' + credentials['token']} if authenticated else {}
    with urllib.request.urlopen(urllib.request.Request(base + path, headers=headers), timeout=10) as response:
        return response.read(), response.headers


for path in ('/healthz', '/readyz'):
    body, _ = get(path)
    assert json.loads(body)['status'] == 'UP'
for name in ('index.html', 'app.js', 'app.css', 'configuration-editor.js', 'accounts.js', 'audit.js', 'validation.js'):
    body, headers = get('/' if name == 'index.html' else '/' + name)
    assert body == (ROOT / 'sync-app/src/main/resources/console' / name).read_bytes(), 'Stale UI: ' + name
    assert headers['Cache-Control'] == 'no-store'
for path in ('/api/tasks', '/api/capabilities', '/api/accounts'):
    try:
        get(path)
        raise AssertionError('Unauthenticated resource exposed: ' + path)
    except urllib.error.HTTPError as error:
        assert error.code == 401
session = json.loads(get('/api/session', True)[0])
assert session['id'] == 'admin' and session['role'] == 'ADMIN'
capabilities = json.loads(get('/api/capabilities', True)[0])
assert any(row['id'] == 'GBASE_8C' and row['role'] == 'SINK' for row in capabilities)
release = json.loads((PREVIEW_HOME / 'runtime/release.json').read_text())
jar = PREVIEW_HOME / 'runtime/rivomesh.jar'
assert hashlib.sha256(jar.read_bytes()).hexdigest() == release['sha256']
with zipfile.ZipFile(jar) as archive:
    assert 'com.gbase8c.Driver' in archive.read('META-INF/services/java.sql.Driver').decode().splitlines()
    archive.getinfo('META-INF/rivomesh-licenses/gbase8c-jdbc.txt')
# Exercise the actual local HTTP cookie configuration, without changing accounts/tasks.
cookies = http.cookiejar.CookieJar()
browser = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookies))
with browser.open(urllib.request.Request(base + '/api/session', data=b'', headers={
        'Authorization': 'Bearer ' + credentials['token'], 'Origin': base}), timeout=10) as response:
    browser_session = json.loads(response.read())
assert browser_session['id'] == 'admin' and browser_session['authentication'] == 'COOKIE'
try:
    cookie = next(iter(cookies))
    assert cookie.name == 'rivomesh-dev-session' and not cookie.secure
    assert cookie.has_nonstandard_attr('HttpOnly') and cookie.get_nonstandard_attr('SameSite') == 'Strict'
    with browser.open(base + '/api/session', timeout=10) as response:
        assert json.loads(response.read())['id'] == 'admin'
    # Distinct hostnames deliberately isolate browser cookies across preview tabs.
    # Send this private test cookie explicitly to verify the shared authority.
    cookie_header = cookie.name + '=' + cookie.value
    other = 'http://localhost:18089'
    with urllib.request.urlopen(urllib.request.Request(other + '/api/session', headers={
            'Cookie': cookie_header}), timeout=10) as response:
        assert json.load(response)['id'] == 'admin'
finally:
    with urllib.request.urlopen(urllib.request.Request(other + '/api/session/logout', data=b'', headers={
            'Cookie': cookie_header, 'Origin': other, 'X-CSRF-Token': browser_session['csrfToken']}), timeout=10) as response:
        assert response.status == 200
try:
    browser.open(base + '/api/session', timeout=10)
    raise AssertionError('Signed-out browser remained authorized')
except urllib.error.HTTPError as error:
    assert error.code == 401
store_ids = set()
for endpoint, local_id in ((base, 'preview-a'), ('http://localhost:18089', 'preview-b')):
    req = urllib.request.Request(endpoint + '/api/nodes', headers={'Authorization': 'Bearer ' + credentials['token']})
    with urllib.request.urlopen(req, timeout=10) as response:
        nodes = json.load(response)
    with urllib.request.urlopen(urllib.request.Request(endpoint + '/api/accounts', headers={
            'Authorization': 'Bearer ' + credentials['token']}), timeout=10) as response:
        accounts = json.load(response)
        assert accounts['mode'] == 'MANAGED' and accounts['authority'] == 'SHARED'
    with urllib.request.urlopen(urllib.request.Request(endpoint + '/api/audit', headers={
            'Authorization': 'Bearer ' + credentials['token']}), timeout=10) as response:
        audit = json.load(response)
        assert audit['authority'] == 'SHARED'
        store_ids.add(audit['storeId'])
    assert nodes['mode'] == 'SHARED_REGISTRY' and nodes['localNodeId'] == local_id
    assert nodes['taskControlScope'] == 'LOCAL'
    assert {node['id'] for node in nodes['nodes']} == {'preview-a', 'preview-b'}
    assert all(node['artifactSha256'] == release['sha256'] and node['jvm'] and node['architecture'] for node in nodes['nodes'])
assert len(store_ids) == 1
print('DEVELOPMENT_PREVIEW_ACCEPTED: health/readiness, seven source UI resources/no-store, two-endpoint shared registry/same artifact, shared accounts/audit, administrator Bearer/cross-node cookie restoration+logout, unauthorized denial and copied vendor-driver JAR')
print('URL:', base, 'account=admin; credentials omitted')
print('Runtime SHA-256:', release['sha256'])
print('GBase catalog:', json.dumps([row for row in capabilities if row['id'].startswith('GBASE_')]))
