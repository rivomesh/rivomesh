#!/usr/bin/env python3
"""Run the candidate-JAR browser-session HTTP contract on five JREs, not the full source matrix."""
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
jar = ROOT / 'sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar'
output = ROOT / 'target/browser-sessions-runtime'
output.mkdir(exist_ok=True)
manifest = {'artifactSha256': hashlib.sha256(jar.read_bytes()).hexdigest(), 'scope': 'BrowserSessionsTest; eight HTTP/session contract tests per JRE; not a source/target compatibility matrix', 'profiles': []}
for jdk in (8, 11, 17, 21, 25):
    image = f'eclipse-temurin:{jdk}-jre'
    log = output / f'java{jdk}.log'
    with log.open('w') as stream:
        subprocess.run(['docker', 'run', '--rm', image, 'java', '-version'], stdout=stream, stderr=subprocess.STDOUT, check=True, timeout=120)
        result = subprocess.run(['docker', 'run', '--rm', '-v', str(ROOT) + ':/project', '-w', '/project', image,
                                 'java', '-Xmx256m', '-cp', '/project/sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:/project/sync-app/target/test-classes:/project/target/tools/junit-platform-console-standalone-1.11.0.jar',
                                 'org.junit.platform.console.ConsoleLauncher', 'execute', '--disable-banner', '--fail-if-no-tests',
                                 '--select-class=io.github.rivomesh.platform.BrowserSessionsTest', '--reports-dir=/project/target/browser-sessions-runtime/java' + str(jdk)],
                                stdout=stream, stderr=subprocess.STDOUT, timeout=300)
    manifest['profiles'].append({'jdk': jdk, 'image': image, 'architecture': subprocess.check_output(['docker', 'image', 'inspect', '--format', '{{.Architecture}}', image], text=True).strip(), 'imageId': subprocess.check_output(['docker', 'image', 'inspect', '--format', '{{.Id}}', image], text=True).strip(), 'exitCode': result.returncode, 'log': log.name})
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print('Java' + str(jdk) + ': ' + ('PASS' if result.returncode == 0 else 'FAIL'), flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)
