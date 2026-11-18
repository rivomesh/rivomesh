#!/usr/bin/env python3
"""Validate shipped configuration examples and check exact-candidate release evidence."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def run(*args, env=None):
    subprocess.run(args, cwd=ROOT, env=env, check=True, timeout=180)


def check_examples(artifact, java_home):
    examples = sorted((ROOT / 'config').glob('*.yml')) + [ROOT / 'deploy/linux/platform.yml']
    names = set()
    for example in examples:
        names.update(re.findall(r'\$\{([A-Z][A-Z0-9_]*)\}', example.read_text()))
    test_env = dict(os.environ)
    for name in names:
        test_env[name] = 'example-only-' + hashlib.sha256(name.encode()).hexdigest()[:32]
    test_env['RIVOMESH_TASK_KEY'] = base64.b64encode(bytes(32)).decode()
    with tempfile.TemporaryDirectory(prefix='rivomesh-release-examples-') as classes:
        java = java_home / 'bin' / 'java' if java_home else 'java'
        javac = java_home / 'bin' / 'javac' if java_home else 'javac'
        run(str(javac), '-cp', str(artifact), '-d', classes, str(ROOT / 'tests/examples/ConfigurationExamples.java'))
        run(str(java), '-cp', os.pathsep.join((str(artifact), classes)), 'io.github.rivomesh.ConfigurationExamples',
            *[str(path.relative_to(ROOT)) for path in examples], env=test_env)
    return len(examples)


def accepted(path, digest):
    record = json.loads(path.read_text())
    assert record.get('artifactSha256') == digest, 'Artifact SHA differs: ' + str(path)
    assert record.get('status') in ('PASS', 'PASSED'), 'Failed evidence: ' + str(path)
    assert isinstance(record.get('checks'), list) and record['checks'], 'Checks missing: ' + str(path)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    if not __debug__:
        parser.error('Run without Python optimization so evidence checks remain active')
    parser.add_argument('--artifact', required=True, type=Path)
    parser.add_argument('--java-home', type=Path)
    parser.add_argument('--examples-only', action='store_true', help='Check examples and artifact structure without accepting release evidence')
    parser.add_argument('--browser', type=Path)
    parser.add_argument('--kubernetes', type=Path)
    parser.add_argument('--native-cdc', type=Path)
    options = parser.parse_args()
    artifact = options.artifact.resolve()
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    proofs = [options.browser, options.kubernetes, options.native_cdc]
    if options.examples_only and any(proofs):
        parser.error('Choose examples-only or candidate evidence')
    if not options.examples_only and not all(proofs):
        parser.error('Candidate evidence requires browser, Kubernetes and native CDC manifests')
    run(sys.executable, str(ROOT / 'tests/sdd-check.py'))
    run(sys.executable, str(ROOT / 'tests/verify-artifact.py'), str(artifact))
    checked = check_examples(artifact, options.java_home)
    print(f'CONFIG_EXAMPLES_PASSED {checked}', flush=True)
    if options.examples_only:
        return

    browser = accepted(options.browser, digest)
    assert browser.get('browser', '').startswith(('Chrome/', 'Chromium/')), 'Real browser version missing'
    assert len(browser['checks']) >= 5, 'Browser scenario coverage missing'
    native = accepted(options.native_cdc, digest)
    assert native.get('cleanupExitCode') == 0, 'Native fixture cleanup failed'
    assert native.get('sourceVersion') and native.get('executionProfile') == 'SEPARATE_AGENT', 'Native CDC authority missing'

    kube = json.loads(options.kubernetes.read_text())
    assert kube.get('serverVersion') and kube.get('csiDriver'), 'Live Kubernetes metadata missing'
    for mode in ('helm', 'native'):
        profile = kube['profiles'][mode]
        assert profile.get('status') == 'PASS' and profile.get('artifactSha256') == digest, f'{mode} candidate SHA differs'
        assert len(profile.get('checks', [])) >= 4, f'{mode} live checks missing'
    assert hashlib.sha256(artifact.read_bytes()).hexdigest() == digest, 'Candidate changed during the gate'
    print('RELEASE_EVIDENCE_GATE_PASSED', digest)


if __name__ == '__main__':
    main()
