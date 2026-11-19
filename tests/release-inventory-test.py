#!/usr/bin/env python3
"""Exercise package inventory evidence and distribution review on small synthetic JARs."""

from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from zipfile import ZipFile


SCANNER = Path(__file__).with_name('release-inventory.py')


def jar(entries):
    buffer = BytesIO()
    with ZipFile(buffer, 'w') as archive:
        for entry, payload in entries.items():
            archive.writestr(entry, payload)
    return buffer.getvalue()


def maven_properties(group, artifact, version):
    return {f'META-INF/maven/{group}/{artifact}/pom.properties':
            f'groupId={group}\nartifactId={artifact}\nversion={version}\n'}


class ReleaseInventoryTest(unittest.TestCase):
    def setUp(self):
        self.home = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.artifact = self.home / 'app.jar'
        self.bom = self.home / 'bom.json'
        self.review = self.home / 'test-review.json'
        self.report = self.home / 'report.json'
        self.bom.write_text(json.dumps({
            'bomFormat': 'CycloneDX', 'specVersion': '1.6',
            'metadata': {'component': {'group': 'io.github.rivomesh', 'name': 'rivomesh'}},
            'components': [{'group': 'example', 'name': 'outer', 'version': '1.0'}],
        }))

    def scan(self, entries, *options):
        self.artifact.write_bytes(jar({
            **maven_properties('example', 'outer', '1.0'),
            'META-INF/LICENSE': 'synthetic license file', 'META-INF/NOTICE': 'synthetic notice file',
            **entries,
        }))
        return self.run_scan(*options)

    def run_scan(self, *options):
        result = subprocess.run(
            [sys.executable, str(SCANNER), '--artifact', str(self.artifact),
             '--maven-bom', str(self.bom), '--output', str(self.report), *options],
            text=True, capture_output=True,
        )
        return result, json.loads(self.report.read_text())

    def write_synthetic_review(self, report, components):
        self.review.write_text(json.dumps({
            'formatVersion': 1, 'artifactSha256': report['artifactSha256'],
            'mavenBomSha256': report['mavenBomSha256'],
            'sourceRightsEvidence': 'synthetic test', 'packagedContentsEvidence': 'synthetic test',
            'packageLicenseFiles': {item['entry']: item['sha256'] for item in report['embeddedLicenseAndNoticeFiles']},
            'componentReviews': {component: {'license': 'synthetic', 'evidence': 'synthetic test'}
                                 for component in components},
        }))

    def test_nested_artifact_review_and_driver_detection(self):
        driver = jar({'META-INF/services/java.sql.Driver': 'com.mysql.cj.jdbc.Driver\n'})
        sdk = jar({
            **maven_properties('example', 'nested-sdk', '2.0'),
            'META-INF/LICENSE': 'nested license',
            'nested/driver.jar': driver,
        })
        result, report = self.scan({'sdk.jar': sdk}, '--expect-external-mysql')
        self.assertEqual(1, result.returncode)
        self.assertEqual(['JAR!/sdk.jar!/nested/driver.jar'], report['mysqlConnectorJLocations'])
        self.assertEqual(['example:outer:1.0'], report['embeddedMavenCoordinates'])
        self.assertEqual(['example:nested-sdk:2.0'], report['packagedCoordinatesOutsideMavenBom'])
        self.assertIn('example:nested-sdk:2.0', report['requiredComponentReviews'])
        self.assertEqual([{
            'location': 'JAR!/sdk.jar', 'sha256': sha256(sdk).hexdigest(),
            'componentCoordinates': ['example:nested-sdk:2.0'],
            'coordinatesOutsideMavenBom': ['example:nested-sdk:2.0'],
            'licenseAndNoticeFiles': [{'entry': 'META-INF/LICENSE',
                                       'sha256': sha256(b'nested license').hexdigest()}],
        }, {
            'location': 'JAR!/sdk.jar!/nested/driver.jar',
            'sha256': sha256(driver).hexdigest(),
            'componentCoordinates': [], 'coordinatesOutsideMavenBom': [], 'licenseAndNoticeFiles': [],
        }], report['embeddedNestedJars'])
        self.assertEqual(['JAR!/sdk.jar!/nested/driver.jar'], report['unidentifiedNestedJars'])

    def test_filename_and_unrelated_metadata_do_not_assign_identity(self):
        unproven = jar({
            'META-INF/MANIFEST.MF': 'Bundle-SymbolicName: bcprov\nBundle-Version: 1.86\n',
            'META-INF/maven/example/mismatch/pom.properties':
                'groupId=example.other\nartifactId=mismatch\nversion=1.0\n',
        })
        kafka = {'kafka/kafka-version.properties': 'commitId=0123456789abcdef\nversion=3.9.2\n'}
        result, report = self.scan({'lib/bcprov-jdk18on-1.86.jar': unproven, **kafka})
        self.assertEqual(0, result.returncode)
        self.assertEqual(['example:outer:1.0'], report['packagedComponentCoordinates'])
        self.assertEqual(['JAR!/lib/bcprov-jdk18on-1.86.jar'], report['unidentifiedNestedJars'])
        kafka['org/apache/kafka/clients/consumer/KafkaConsumer.class'] = 'synthetic class marker'
        result, report = self.scan(kafka)
        self.assertEqual(0, result.returncode)
        self.assertEqual('KAFKA_BUILD_PROPERTIES',
                         report['packagedComponentEvidence']['org.apache.kafka:kafka-clients:3.9.2'][0]['kind'])

    def test_review_missing_nested_component_is_blocked(self):
        entries = {'sdk.jar': jar(maven_properties('example', 'nested', '2.0'))}
        result, report = self.scan(entries)
        self.assertEqual(0, result.returncode)
        self.write_synthetic_review(report, ['example:outer:1.0'])
        # ZIP metadata includes time, so reuse the exact archive from the first run.
        result, checked = self.run_scan('--asf-review', str(self.review))
        self.assertEqual(1, result.returncode)
        review = checked['asfDistributionReview']
        self.assertEqual('BLOCKED', review['status'])
        self.assertEqual([], review['blockingFindings'])
        self.assertEqual(['MISSING_COMPONENT_REVIEW: example:nested:2.0'], review['recordErrors'])

    def test_review_blocks_unidentified_nested_jar_with_location_and_hash(self):
        unknown = jar({'example/Unknown.class': 'synthetic bytecode'})
        parent = jar({
            **maven_properties('example', 'nested', '2.0'),
            'plugins/unidentified.jar': unknown,
        })
        result, report = self.scan({'sdk.jar': parent})
        self.assertEqual(0, result.returncode)
        location = 'JAR!/sdk.jar!/plugins/unidentified.jar'
        finding = f'UNIDENTIFIED_NESTED_JAR: {location} (sha256={sha256(unknown).hexdigest()})'
        self.assertEqual([location], report['unidentifiedNestedJars'])
        self.assertEqual('NOT_RECORDED', report['asfDistributionReview']['status'])
        self.assertEqual([finding], report['asfDistributionReview']['blockingFindings'])

        self.write_synthetic_review(report, report['requiredComponentReviews'])
        result, checked = self.run_scan('--asf-review', str(self.review))
        self.assertEqual(1, result.returncode)
        self.assertIn('ASF_DISTRIBUTION_REVIEW_BLOCKED', result.stdout)
        review = checked['asfDistributionReview']
        self.assertEqual('BLOCKED', review['status'])
        self.assertEqual([finding], review['blockingFindings'])
        self.assertEqual([], review['recordErrors'])


if __name__ == '__main__':
    unittest.main()
