#!/usr/bin/env python3
"""Check SDD IDs, bilingual task/acceptance traces, dependency graph and completion evidence."""
from collections import Counter
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
package = ROOT / 'docs/sdd/001-control-recovery-compatibility'
data = json.loads((package / 'requirements.json').read_text())
requirements = data['requirements']
ids = {item['id'] for item in requirements}
assert len(ids) == len(requirements), 'Duplicate requirement ID'
assert all(item['status'] in ('NEW', 'PARTIAL', 'DONE') for item in requirements)
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
    tasks = (package / ('tasks' + suffix + '.md')).read_text()
    acceptance = (package / ('acceptance' + suffix + '.md')).read_text()
    specification = (package / ('spec' + suffix + '.md')).read_text()
    assert set(re.findall(r'T-([A-Z]{2}-\d{2})', tasks)) == ids
    assert set(re.findall(r'\| AC-([A-Z]{2}-\d{2}) \|', acceptance)) == ids
    for item in requirements:
        identity = item['id']
        expected = 'x' if item['status'] == 'DONE' else ' '
        assert '- [' + expected + '] T-' + identity + ' ' in tasks, 'Task status differs: ' + identity
        assert '| ' + identity + ' | ' + item['priority'] + ' / ' + item['status'] + ' |' in specification
for item in requirements:
    for evidence in item['evidence']:
        assert (ROOT / evidence).exists(), 'Missing evidence: ' + evidence
    if item['status'] == 'DONE':
        evidence = item['verification']
        assert re.fullmatch(r'[a-f0-9]{64}', evidence['artifactSha256'])
        archive = ROOT / evidence['archive']
        assert archive.is_file() and evidence['artifactSha256'] in archive.read_text(), 'Unlinked completion evidence'
for path in list(package.glob('*.md')) + [ROOT / 'docs/zh-CN/browser-sessions.md', ROOT / 'docs/en/browser-sessions.md']:
    for target in re.findall(r'\[[^\]]*\]\(([^)]+)\)', path.read_text()):
        if '://' not in target and not target.startswith('#'):
            assert (path.parent / target.split('#')[0]).exists(), str(path.relative_to(ROOT)) + ': broken link ' + target
print('SDD_ACCEPTED', data['specId'], 'v' + data['version'], len(ids), 'requirements;', dict(Counter(item['status'] for item in requirements)))
