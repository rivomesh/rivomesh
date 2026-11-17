#!/usr/bin/env python3
"""Five-JRE failed-transaction foundation contracts; not a source/target/JDK acceptance matrix."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--artifact', type=Path, required=True)
parser.add_argument('--test-classes', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
jar, classes, output = args.artifact.resolve(), args.test_classes.resolve(), args.output.resolve()
for path in (jar, classes, output):
    path.relative_to(ROOT)
assert jar.is_file() and classes.is_dir()
output.mkdir(parents=True, exist_ok=True)
test_hash = hashlib.sha256()
for entry in sorted(classes.rglob('*')):
    if entry.is_file():
        test_hash.update(str(entry.relative_to(classes)).encode() + b'\0' + entry.read_bytes())
scope = ['io.rivomesh.cdc.' + name for name in ['FailureReviewTest','FanoutDispatcherTest','JournalTest','NormalizationTest','SnapshotDispatcherTest','FailureEvidenceStoreTest']] + ['io.rivomesh.platform.ManagedFailureReviewTest']
manifest = dict(artifactSha256=hashlib.sha256(jar.read_bytes()).hexdigest(), testClassesSha256=test_hash.hexdigest(),
                scope=scope, harnessSourceSha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), profiles=[],
                notCovered='Real source/destination/JDK/TLS/SDK matrix, manual skip/patch or UI raw review')
cp = '/project/' + str(jar.relative_to(ROOT)) + ':/project/' + str(classes.relative_to(ROOT)) + ':/project/target/tools/junit-platform-console-standalone-1.11.0.jar'
for jdk in (8, 11, 17, 21, 25):
    image = 'eclipse-temurin:' + str(jdk) + '-jre'
    docker = ['docker', 'run', '--rm', '-v', str(ROOT) + ':/project:ro', '-v', str(output) + ':/evidence', image]
    log = output / ('java' + str(jdk) + '.log')
    with log.open('w') as stream:
        subprocess.run(docker + ['java', '-version'], stdout=stream, stderr=subprocess.STDOUT, check=True, timeout=60)
        result = subprocess.run(docker + ['java', '-Xmx256m', '-cp', cp, 'org.junit.platform.console.ConsoleLauncher', 'execute', '--disable-banner', '--fail-if-no-tests',
                                         *['--select-class=' + name for name in scope], '--reports-dir=/evidence/java' + str(jdk)],
                                stdout=stream, stderr=subprocess.STDOUT, timeout=180)
    totals = {key: 0 for key in ('tests', 'failures', 'errors', 'skipped')}
    for report in (output / ('java' + str(jdk))).glob('TEST-*.xml'):
        suite = ET.parse(report).getroot()
        for key in totals:
            totals[key] += int(suite.attrib.get(key, 0))
    metadata = json.loads(subprocess.check_output(['docker', 'image', 'inspect', image], text=True))[0]
    manifest['profiles'].append(dict(jdk=jdk, image=image, imageId=metadata['Id'], architecture=metadata['Architecture'], exitCode=result.returncode, results=totals, log=log.name))
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    assert result.returncode == 0 and totals == {'tests': 52, 'failures': 0, 'errors': 0, 'skipped': 0}, (jdk, totals)
    print('Java' + str(jdk) + ': PASS 52 cases', flush=True)
