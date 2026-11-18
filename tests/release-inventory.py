#!/usr/bin/env python3
"""Bind a Maven dependency SBOM to the packaged Rivomesh JAR and inventory embedded evidence."""

import argparse
from hashlib import sha256
import json
from pathlib import Path
import re
from zipfile import BadZipFile, ZipFile

PACKAGE_LICENSE_FILES = ('META-INF/LICENSE', 'META-INF/NOTICE')
VENDOR_DRIVERS = {
    'gsjdbc4:gsjdbc4:1.1': (
        'io/github/rivomesh/gaussdb/jdbc/Driver.class',
        'META-INF/rivomesh-licenses/gaussdb-jdbc.txt',
        'e844d854526072894acab3da8f49210213e7e445553010dd0fa2912f63542130',
    ),
    'GBase8c vendor JDBC 3.0.0B02': (
        'com/gbase8c/Driver.class',
        'META-INF/rivomesh-licenses/gbase8c-jdbc.txt',
        '5be1f67a4d62692cc2fc8b81a71088943fc4c1462bd76fd2f04bc0e965ca6d53',
    ),
}
CONNECTOR_J_MARKER = re.compile(
    r'(?:^|/)(?:com/mysql/(?:cj|jdbc)/.+\.class'
    r'|META-INF/maven/(?:com\.mysql/mysql-connector-j|mysql/mysql-connector-java)/pom\.properties)$'
)
MYSQL_JDBC_PROVIDERS = {b'com.mysql.cj.jdbc.Driver', b'com.mysql.jdbc.Driver'}


def stream_digest(source):
    value = sha256()
    for chunk in iter(lambda: source.read(1024 * 1024), b''):
        value.update(chunk)
    return value.hexdigest()


def digest(path):
    with path.open('rb') as source:
        return stream_digest(source)


def coordinate(component):
    return tuple(component.get(field, '') for field in ('group', 'name', 'version'))


def properties(data):
    parsed = {}
    for line in data.decode('latin-1').splitlines():
        if not line.startswith(('#', '!')) and '=' in line:
            key, value = line.split('=', 1)
            parsed[key.strip()] = value.strip()
    return parsed


def embedded_metadata(artifact):
    versions = set()
    notices = {}
    binaries = {}
    with ZipFile(artifact) as archive:
        paths = archive.namelist()
        names = set(paths)
        if len(paths) != len(names):
            raise ValueError('The JAR has duplicate entries')
        for entry in sorted(name for name in names if not name.endswith('/')):
            if re.fullmatch(r'META-INF/maven/[^/]+/[^/]+/pom.properties', entry):
                p = properties(archive.read(entry))
                if p.keys() >= {'groupId', 'artifactId', 'version'}:
                    versions.add((p['groupId'], p['artifactId'], p['version']))
            if is_license_notice(entry):
                with archive.open(entry) as content:
                    notices[entry] = stream_digest(content)
            if re.search(r'\.(?:jar|so(?:\.\d+)*|dll|dylib|jnilib|a)$', entry, flags=re.IGNORECASE):
                with archive.open(entry) as content:
                    binaries[entry] = stream_digest(content)
        mysql_locations = connector_j_locations(archive)
    vendors = [{'component': component, 'embeddedClass': marker, 'requiredLicenseEntry': license_entry,
                'licenseMatchesUpstream': notices.get(license_entry) == license_digest}
               for component, (marker, license_entry, license_digest) in VENDOR_DRIVERS.items() if marker in names]
    return versions, notices, binaries, vendors, mysql_locations


def connector_j_locations(archive, location='JAR'):
    names = archive.namelist()
    provider_entry = 'META-INF/services/java.sql.Driver'
    providers = archive.read(provider_entry).splitlines() if provider_entry in names else []
    bundled = any(CONNECTOR_J_MARKER.search(name) for name in names)
    connector_provider = any(line.partition(b'#')[0].strip() in MYSQL_JDBC_PROVIDERS for line in providers)
    found = [location] if bundled or connector_provider else []
    for name in sorted(entry for entry in names if entry.lower().endswith('.jar')):
        nested_location = location + '!/' + name
        with archive.open(name) as packaged:
            try:
                with ZipFile(packaged) as nested:
                    found.extend(connector_j_locations(nested, nested_location))
            except BadZipFile as error:
                raise ValueError('Cannot inspect embedded JAR: ' + nested_location) from error
    return found


def is_license_notice(entry):
    if entry.startswith(('META-INF/license/', 'META-INF/licenses/', 'META-INF/rivomesh-licenses/')):
        return True
    filename = entry.removeprefix('META-INF/')
    return bool(re.fullmatch(r'(?:[\w.-]+[-_.])?(?:LICEN[CS]E|NOTICE|COPYING)(?:[-_.][\w.-]+)?', filename,
                             flags=re.IGNORECASE) and not filename.endswith('.class'))


def declared_terms(component):
    terms = set()
    for declaration in component.get('licenses', []):
        license_info = declaration.get('license', {})
        name = declaration.get('expression') or license_info.get('id') or license_info.get('name')
        if name:
            terms.add(name)
    return sorted(terms)


def asf_review_blockers(notices, vendors, mysql_locations):
    blockers = []
    # The shipped 8.4.0 community connector uses GPLv2 with the Universal FOSS
    # Exception, which is not an ASF policy exception for bundled software.
    if mysql_locations:
        blockers.append('BUNDLED_MYSQL_CONNECTOR_J')
    blockers.extend('MISSING_OR_CHANGED_VENDOR_LICENSE: ' + item['requiredLicenseEntry']
                    for item in vendors if not item['licenseMatchesUpstream'])
    blockers.extend('MISSING_PACKAGE_LICENSE_FILE: ' + entry for entry in PACKAGE_LICENSE_FILES if entry not in notices)
    return blockers


def verify_asf_review(review, report, notices, components):
    def evidence(value):
        return isinstance(value, str) and bool(value.strip())

    errors = []
    if not isinstance(review, dict) or review.get('formatVersion') != 1:
        return ['INVALID_REVIEW_FORMAT']
    for key in ('artifactSha256', 'mavenBomSha256'):
        if review.get(key) != report[key]:
            errors.append('REVIEW_DIGEST_MISMATCH: ' + key)
    for key in ('sourceRightsEvidence', 'packagedContentsEvidence'):
        if not evidence(review.get(key)):
            errors.append('MISSING_REVIEW_EVIDENCE: ' + key)
    expected = {entry: notices[entry] for entry in PACKAGE_LICENSE_FILES if entry in notices}
    if review.get('packageLicenseFiles') != expected:
        errors.append('PACKAGE_LICENSE_FILES_DO_NOT_MATCH')
    recorded = review.get('componentReviews', {})
    if not isinstance(recorded, dict):
        return errors + ['INVALID_COMPONENT_REVIEWS']
    errors.extend('UNEXPECTED_COMPONENT_REVIEW: ' + key for key in sorted(recorded.keys() - components.keys()))
    for key in components:
        decision = recorded.get(key)
        if not isinstance(decision, dict) or not all(evidence(decision.get(k)) for k in ('license', 'evidence')):
            errors.append('MISSING_COMPONENT_REVIEW: ' + key)
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact', required=True, type=Path)
    parser.add_argument('--maven-bom', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--asf-review', type=Path, help='Require an exact-JAR manual ASF distribution review record')
    parser.add_argument('--expect-external-mysql', action='store_true',
                        help='Fail if the JAR or any nested JAR contains Connector/J')
    args = parser.parse_args()
    inputs = [args.artifact, args.maven_bom]
    if args.asf_review:
        inputs.append(args.asf_review)
    if any(args.output.resolve() == source.resolve()
           or (args.output.exists() and args.output.samefile(source)) for source in inputs):
        parser.error('The inventory output must differ from all inputs')
    bom_data = args.maven_bom.read_bytes()
    bom = json.loads(bom_data)
    product = bom['metadata']['component']
    if bom.get('bomFormat') != 'CycloneDX' or product.get('group') != 'io.github.rivomesh' or product.get('name') != 'rivomesh':
        parser.error('Provide the aggregated Rivomesh CycloneDX Maven BOM')
    dependencies = {coordinate(component): component for component in bom['components']}
    artifact_sha256 = digest(args.artifact)
    try:
        embedded, notices, binaries, vendors, mysql_locations = embedded_metadata(args.artifact)
    except (ValueError, BadZipFile) as error:
        parser.error(str(error))
    if digest(args.artifact) != artifact_sha256:
        parser.error('The JAR changed during inventory')
    if not embedded or not notices:
        parser.error('The JAR must contain dependency metadata and embedded notices')

    def entries(components):
        return [':'.join(parts) for parts in sorted(components)]

    reviews = {
        ':'.join(key): declared_terms(dependencies.get(key, {})) for key in sorted(embedded)
        if key[0] != 'io.github.rivomesh'
    }
    for vendor in vendors:
        reviews.setdefault(vendor['component'], [])
    report = {
        'artifactSha256': artifact_sha256,
        'mavenBomSha256': sha256(bom_data).hexdigest(),
        'cycloneDxVersion': bom['specVersion'],
        'mavenComponentCount': len(dependencies),
        'embeddedMavenCoordinates': entries(embedded),
        'embeddedCoordinatesOutsideMavenBom': entries(embedded.difference(dependencies)),
        'mavenComponentsWithoutEmbeddedCoordinates': entries(dependencies.keys() - embedded),
        'mavenComponentsWithoutDeclaredLicense': entries(key for key, component in dependencies.items()
                                                         if not component.get('licenses')),
        'vendoredBinaryMarkers': vendors,
        'embeddedLicenseAndNoticeFiles': [{'entry': name, 'sha256': value} for name, value in notices.items()],
        'embeddedBinaryFiles': [{'entry': name, 'sha256': value} for name, value in binaries.items()],
        'mysqlConnectorJLocations': mysql_locations,
        'requiredComponentReviews': reviews,
        'asfDistributionReview': {
            'status': 'NOT_RECORDED',
            'blockingFindings': asf_review_blockers(notices, vendors, mysql_locations),
        },
    }
    review_status = report['asfDistributionReview']
    if args.asf_review:
        approval_data = args.asf_review.read_bytes()
        review_status['recordSha256'] = sha256(approval_data).hexdigest()
        try:
            record = json.loads(approval_data)
        except (UnicodeError, json.JSONDecodeError):
            record = None
        review_status['recordErrors'] = verify_asf_review(record, report, notices, reviews)
        review_status['status'] = ('BLOCKED' if review_status['blockingFindings'] or review_status['recordErrors']
                                   else 'RECORDED')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(f'INVENTORY_GENERATED {len(dependencies)} Maven components, {len(embedded)} embedded coordinates, '
          f'{len(vendors)} vendor markers, {len(notices)} notice/license files')
    if args.asf_review:
        print('ASF_DISTRIBUTION_REVIEW_' + review_status['status'])
    if args.expect_external_mysql:
        print('MYSQL_JDBC_EXTERNAL_CHECK_' + ('FAILED' if mysql_locations else 'PASSED'))
    return int(review_status['status'] == 'BLOCKED' or (args.expect_external_mysql and bool(mysql_locations)))


if __name__ == '__main__':
    raise SystemExit(main())
