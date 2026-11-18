#!/usr/bin/env python3
"""Run release-first control contracts or actual management and CDC processes on five JREs."""
import argparse
import hashlib
import json
import re
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--artifact', type=Path, default=ROOT / 'sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar')
parser.add_argument('--test-classes', type=Path, default=ROOT / 'sync-app/target/test-classes')
parser.add_argument('--jdk', type=int, nargs='+', choices=(8,11,17,21,25), default=(8,11,17,21,25))
parser.add_argument('--output', type=Path, default=ROOT / 'target/control-runtime')
parser.add_argument('--suite', choices=('baseline','extended','global','routed','maintenance','execution','processes'), default='baseline')
parser.add_argument('--heap-mib', type=int, choices=(128,256,512), default=128)
args = parser.parse_args()
jar = args.artifact.resolve()
classes_directory = args.test_classes.resolve()
jar_container = '/project/' + str(jar.relative_to(ROOT))
classes_container = '/project/' + str(classes_directory.relative_to(ROOT))
output = args.output.resolve()
output.relative_to(ROOT)
output.mkdir(parents=True,exist_ok=True)
output_container = '/project/' + str(output.relative_to(ROOT))
if args.suite == 'processes':
    manifest = {'artifactSha256': hashlib.sha256(jar.read_bytes()).hexdigest(),
                'scope': 'Actual separate Controller, Agent and Worker with MySQL source and PostgreSQL/MySQL targets',
                'profiles': []}
    for jdk in dict.fromkeys(args.jdk):
        profile = output / f'java{jdk}'
        profile.mkdir(exist_ok=True)
        with (profile / 'run.log').open('w') as log:
            result = subprocess.run(['python3', str(ROOT / 'tests/global-control-pipeline.py'),
                                     '--artifact', str(jar), '--output', str(profile),
                                     '--separate-execution', '--runtime-jdk', str(jdk)],
                                    stdout=log, stderr=subprocess.STDOUT)
        record = json.loads((profile / 'manifest.json').read_text()) if (profile / 'manifest.json').is_file() else {}
        passed = (result.returncode == 0 and record.get('status') == 'PASS' and record.get('cleanupExitCode') == 0
                  and record.get('artifactSha256') == manifest['artifactSha256'])
        manifest['profiles'].append({'jdk': jdk, 'status': 'PASS' if passed else 'FAIL',
                                     'manifest': f'java{jdk}/manifest.json', 'exitCode': result.returncode,
                                     'processes': record.get('runtimeProcesses'), 'imageId': record.get('runtimeImageId'),
                                     'architecture': record.get('runtimeArchitecture')})
        (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        print(f'Java {jdk}: ' + ('PASS' if passed else 'FAIL'), flush=True)
    raise SystemExit(0 if all(profile['status']=='PASS' for profile in manifest['profiles']) else 1)

classes = ['BrowserSessionsTest', 'ControlStoreTest', 'SharedAccessTest', 'WorkerEnvironmentTest']
if args.suite in ('extended','global','routed','maintenance','execution'):
    classes += ['ControlCipherTest','GlobalTaskStoreTest','TaskPlaneTest','LocalJournalLifecycleTest','DurableCommandsTest']
if args.suite in ('global','routed','maintenance','execution'):
    classes += ['AgentDirectoryTest','CommandDispatchTest','OwnerExecutionTest','OwnerQueriesTest','GlobalTaskPlaneTest','GlobalConfigurationTest']
if args.suite in ('routed','maintenance','execution'):
    classes += ['OwnerRuntimeTest','GlobalResourcesTest','GlobalControlHttpTest']
if args.suite in ('maintenance','execution'):
    classes += ['ManagedValidationTest','ManagedRepairTest']
if args.suite == 'execution':
    classes += ['ExecutionRolesTest']
manifest = {'artifactSha256': hashlib.sha256(jar.read_bytes()).hexdigest(), 'scope': classes, 'heapMiB': args.heap_mib, 'notCovered': 'Full source/destination/JDK/TLS-driver compatibility', 'profiles': []}
for jdk in args.jdk:
    image = f'eclipse-temurin:{jdk}-jre'
    log = output / f'java{jdk}.log'
    with log.open('w') as stream:
        subprocess.run(['docker', 'run', '--rm', image, 'java', '-version'], stdout=stream, stderr=subprocess.STDOUT, check=True, timeout=120)
        result = subprocess.run(['docker', 'run', '--rm', '-v', str(ROOT) + ':/project', '-w', '/project', image,
                                 'java', '-Xmx' + str(args.heap_mib) + 'm', '-cp', jar_container + ':' + classes_container + ':/project/target/tools/junit-platform-console-standalone-1.11.0.jar',
                                 'org.junit.platform.console.ConsoleLauncher', 'execute', '--disable-banner', '--fail-if-no-tests',
                                 *['--select-class=io.github.rivomesh.platform.' + name for name in classes], '--reports-dir=' + output_container + '/java' + str(jdk)],
                                stdout=stream, stderr=subprocess.STDOUT, timeout=600)
    inspect = json.loads(subprocess.check_output(['docker', 'image', 'inspect', image], text=True))[0]
    count=re.search(r'\[\s*(\d+) tests found\s*\]',log.read_text())
    manifest['profiles'].append({'jdk': jdk, 'image': image, 'imageId': inspect['Id'], 'architecture': inspect['Architecture'], 'exitCode': result.returncode, 'log': log.name, 'cases':int(count.group(1)) if count else None})
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print('Java' + str(jdk) + ': ' + ('PASS' if result.returncode == 0 else 'FAIL'), flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)
