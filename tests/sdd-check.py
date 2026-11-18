#!/usr/bin/env python3
"""Check SDD IDs, bilingual task/acceptance traces, dependency graph and completion evidence."""
from collections import Counter
from hashlib import sha256
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
package = ROOT / 'docs/sdd/001-control-recovery-compatibility'
data = json.loads((package / 'requirements.json').read_text())
requirements = data['requirements']
states = ('DONE', 'PARTIAL', 'NEW')
ids = {item['id'] for item in requirements}
assert len(ids) == len(requirements), 'Duplicate requirement ID'
assert all(item['status'] in states for item in requirements)
counts = Counter(item['status'] for item in requirements)
completed = {item['id']: item for item in requirements if item['status'] == 'DONE'}
progress = json.loads((package / 'progress-reports.json').read_text())
assert progress['currentSdd'] == {state: counts[state] for state in states}, 'Progress totals differ from the requirements'
baseline = json.loads((ROOT / 'docs/verification/baseline.json').read_text())
archived = {item['id']: item['verification'] for item in baseline['acceptedRequirements']}
assert len(archived) == len(baseline['acceptedRequirements']), 'Duplicate accepted evidence'
assert archived.keys() == completed.keys(), 'Accepted evidence and completed requirements differ'
visiting, visited = set(), set()
index = {item['id']: item for item in requirements}


def visit(identity):
    assert identity in ids, 'Unknown dependency: ' + identity
    assert identity not in visiting, 'Dependency cycle: ' + identity
    if identity in visited:
        return
    visiting.add(identity)
    for dependency in index[identity]['dependsOn']:
        visit(dependency)
    visiting.remove(identity)
    visited.add(identity)


for identity in ids:
    visit(identity)
for suffix in ('', '.en'):
    locale = 'en' if suffix else 'zh-CN'
    tasks = (package / ('tasks' + suffix + '.md')).read_text()
    acceptance = (package / ('acceptance' + suffix + '.md')).read_text()
    specification = (package / ('spec' + suffix + '.md')).read_text()
    assert 'v' + data['version'] in specification, 'Specification version differs'
    assert Counter(re.findall(r'^- \[[ x]\] T-([A-Z]{2}-\d{2}) ', tasks, re.M)) == Counter(ids)
    rows = {}
    for line in acceptance.splitlines():
        if line.startswith('| AC-'):
            _, key, requirement, criteria, verdict, end = line.split('|')
            identity = requirement.strip()
            assert key.strip() == 'AC-' + identity and identity not in rows, 'Invalid acceptance row ' + line
            assert identity in index and criteria.strip() == index[identity]['acceptance'][locale], (
                'Acceptance text differs: ' + identity + ' ' + locale)
            rows[identity] = verdict.strip()
    assert rows.keys() == ids, 'Acceptance IDs differ from the requirements'
    for item in requirements:
        identity = item['id']
        expected = 'x' if item['status'] == 'DONE' else ' '
        assert '- [' + expected + '] T-' + identity + ' ' in tasks, 'Task status differs: ' + identity
        assert '| ' + identity + ' | ' + item['priority'] + ' / ' + item['status'] + ' |' in specification
        if item['status'] == 'DONE':
            assert re.search(r'PASS|DONE|Accepted|已验收|通过', rows[identity], re.I), 'Missing accepted verdict: ' + identity
for item in requirements:
    identity = item['id']
    assert re.fullmatch(r'[A-Z]{2}-\d{2}', identity), 'Invalid requirement ID: ' + identity
    assert re.fullmatch(r'P[0-9]', item['priority']), 'Invalid priority: ' + identity
    for field in ('title', 'gap', 'acceptance'):
        for locale in ('zh-CN', 'en'):
            assert item[field][locale].strip(), 'Missing ' + locale + ' ' + field + ': ' + identity
    for evidence in item['evidence']:
        assert (ROOT / evidence).exists(), 'Missing evidence: ' + evidence
    if item['status'] == 'DONE':
        evidence = item['verification']
        assert evidence == archived[identity], 'Completed evidence differs from archive: ' + identity
        assert re.fullmatch(r'[a-f0-9]{64}', evidence['artifactSha256']), 'Invalid artifact hash: ' + identity
        archive = ROOT / evidence['archive']
        assert archive.is_file() and evidence['artifactSha256'] in archive.read_text(), 'Unlinked completion evidence'
        criteria = json.dumps(item['acceptance'], sort_keys=True, ensure_ascii=False, separators=(',', ':'))
        assert sha256(criteria.encode()).hexdigest() == evidence['criteriaSha256'], 'Accepted criteria changed: ' + identity
for path in list(package.glob('*.md')) + [ROOT / 'docs/zh-CN/browser-sessions.md', ROOT / 'docs/en/browser-sessions.md']:
    for target in re.findall(r'\[[^\]]*\]\(([^)]+)\)', path.read_text()):
        if '://' not in target and not target.startswith('#'):
            assert (path.parent / target.split('#')[0]).exists(), str(path.relative_to(ROOT)) + ': broken link ' + target
print('SDD_ACCEPTED', data['specId'], 'v' + data['version'], len(ids), 'requirements;', dict(counts))
