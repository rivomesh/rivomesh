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

VENDOR_JDBC_MARKERS = {
    'gaussdb': (
        re.compile(r'(?:^|/)io/github/rivomesh/gaussdb/jdbc/.+\.class$'
                   r'|^META-INF/maven/(?:io\.github\.rivomesh/rivomesh-gaussdb-driver'
                   r'|gsjdbc4/gsjdbc4|org\.opengauss/opengauss-jdbc)/pom\.(?:xml|properties)$'
                   r'|^META-INF/rivomesh-licenses/gaussdb-jdbc\.txt$'),
        b'io.github.rivomesh.gaussdb.jdbc.Driver', 'gsjdbc4:gsjdbc4:1.1',
    ),
    'gbase8c': (
        re.compile(r'(?:^|/)com/gbase8c/.+\.class$'
                   r'|^META-INF/maven/(?:io\.github\.rivomesh/rivomesh-gbase8c-driver'
                   r'|cn\.gbase/gbase8c-jdbc)/pom\.(?:xml|properties)$'
                   r'|^META-INF/rivomesh-licenses/gbase8c-jdbc\.txt$'),
        b'com.gbase8c.Driver', 'GBase8c vendor JDBC 3.0.0B02',
    ),
    'kingbase': (
        re.compile(r'(?:^|/)com/kingbase8/.+\.class$'
                   r'|^META-INF/maven/cn\.com\.kingbase/kingbase8/pom\.(?:xml|properties)$'
                   r'|^META-INF/rivomesh-licenses/kingbase-jdbc\.txt$'),
        b'com.kingbase8.Driver', 'cn.com.kingbase:kingbase8:9.0.1',
    ),
    'dameng': (
        re.compile(r'(?:^|/)dm/jdbc/.+\.class$'
                   r'|^META-INF/maven/com\.dameng/DmJdbcDriver[^/]*/pom\.(?:xml|properties)$'
                   r'|^META-INF/rivomesh-licenses/dameng-jdbc\.txt$'),
        b'dm.jdbc.driver.DmDriver', 'com.dameng:DmJdbcDriver18:8.1.3.140',
    ),
}
JDBC_NOTICE_OWNERS = {
    '8f1e56c81a8eb0f9eb12efd5b722bf068efb9db8decc85bebe02fb58a0ec7bbf': 'mysql',
    VENDOR_DRIVERS['gsjdbc4:gsjdbc4:1.1'][2]: 'gaussdb',
    VENDOR_DRIVERS['GBase8c vendor JDBC 3.0.0B02'][2]: 'gbase8c',
}
GAUSSDB_UNRELOCATED_DRIVER_SHA256 = 'c140b8a47715d13a206f9db1d66a58a514745e48c12bd5a5a15c20f33bf2a53c'
# These upstream JARs omit Maven properties. Each key is the release .jar.sha256 from Maven Central.
PUBLISHED_JAR_COORDINATES = {
    '8d8b41a4b149bdae8d331f059a578870954d1e3ff29f8269a9eb3ee19a7e4ef7': ('org.bouncycastle', 'bcpkix-jdk18on', '1.86'),
    '2af190b300cbb0b35e248ccf5f4a06b6072030aeb3da7a98ec73abe5b4cb371f': ('org.bouncycastle', 'bcprov-jdk18on', '1.86'),
    '1c268e15f785aafb1e4670d8d6e5f856f69475e96c162dadcd1ff9b350e9139f': ('org.bouncycastle', 'bcutil-jdk18on', '1.86'),
}
MAVEN_PROPERTIES = re.compile(r'META-INF/maven/([^/]+)/([^/]+)/pom\.properties')
KAFKA_BUILD_PROPERTIES = 'kafka/kafka-version.properties'
KAFKA_CLIENT_CLASS = 'org/apache/kafka/clients/consumer/KafkaConsumer.class'
BINARY_ENTRY = re.compile(r'\.(?:jar|so(?:\.\d+)*|dll|dylib|jnilib|a)$', flags=re.IGNORECASE)


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
    contents = {}
    jdbc_locations = {driver: [] for driver in ('mysql', *VENDOR_JDBC_MARKERS)}
    with ZipFile(artifact) as archive:
        scan_archive(archive, 'JAR', None, contents, jdbc_locations)
        names = set(archive.namelist())
    notices = contents['JAR']['notices']
    vendors = [{'component': component, 'embeddedClass': marker, 'requiredLicenseEntry': license_entry,
                'licenseMatchesUpstream': notices.get(license_entry) == license_digest}
               for component, (marker, license_entry, license_digest) in VENDOR_DRIVERS.items() if marker in names]
    return contents, vendors, jdbc_locations


def scan_archive(archive, location, jar_sha256, contents, jdbc_locations):
    if location in contents:
        raise ValueError('Ambiguous nested archive location: ' + location)
    paths = archive.namelist()
    names = set(paths)
    if len(paths) != len(names):
        raise ValueError('Duplicate archive entries in ' + location)
    components = {}
    notices = {}
    binaries = {}
    found = set()
    for entry in sorted(name for name in names if not name.endswith('/')):
        match = MAVEN_PROPERTIES.fullmatch(entry)
        if match:
            data = archive.read(entry)
            fields = properties(data)
            if (fields.get('groupId'), fields.get('artifactId')) == match.groups() and fields.get('version'):
                components[(*match.groups(), fields['version'])] = {
                    'kind': 'MAVEN_PROPERTIES', 'entry': entry, 'sha256': sha256(data).hexdigest(),
                }
        if is_license_notice(entry):
            with archive.open(entry) as content:
                notices[entry] = stream_digest(content)
            owner = JDBC_NOTICE_OWNERS.get(notices[entry])
            if owner is not None:
                found.add(owner)
        if BINARY_ENTRY.search(entry):
            with archive.open(entry) as content:
                binaries[entry] = stream_digest(content)
    if KAFKA_BUILD_PROPERTIES in names and KAFKA_CLIENT_CLASS in names:
        data = archive.read(KAFKA_BUILD_PROPERTIES)
        fields = properties(data)
        version = fields.get('version', '')
        if (re.fullmatch(r'\d+\.\d+\.\d+(?:[-.][A-Za-z0-9.-]+)?', version)
                and re.fullmatch(r'[a-fA-F0-9]{7,64}', fields.get('commitId', ''))):
            components.setdefault(('org.apache.kafka', 'kafka-clients', version), {
                'kind': 'KAFKA_BUILD_PROPERTIES', 'entry': KAFKA_BUILD_PROPERTIES, 'sha256': sha256(data).hexdigest(),
            })
    if jar_sha256 in PUBLISHED_JAR_COORDINATES:
        components.setdefault(PUBLISHED_JAR_COORDINATES[jar_sha256], {
            'kind': 'PUBLISHED_JAR_SHA256', 'sha256': jar_sha256,
        })
    contents[location] = {'components': components, 'notices': notices, 'binaries': binaries, 'sha256': jar_sha256}

    provider_entry = 'META-INF/services/java.sql.Driver'
    providers = (archive.read(provider_entry).splitlines() if provider_entry in names else [])
    providers = {line.partition(b'#')[0].strip() for line in providers}
    if MYSQL_JDBC_PROVIDERS.intersection(providers) or any(CONNECTOR_J_MARKER.search(name) for name in names):
        found.add('mysql')
    for driver, (marker, provider, _) in VENDOR_JDBC_MARKERS.items():
        if provider in providers or any(marker.search(name) for name in names):
            found.add(driver)
    # Huawei's unrelocated package uses the standard PostgreSQL class path.
    postgres_marker = 'org/postgresql/Driver.class'
    if postgres_marker in names and sha256(archive.read(postgres_marker)).hexdigest() == GAUSSDB_UNRELOCATED_DRIVER_SHA256:
        found.add('gaussdb')
    for driver in found:
        jdbc_locations[driver].append(location)
    for name, nested_sha256 in binaries.items():
        if not name.lower().endswith('.jar'):
            continue
        nested_location = location + '!/' + name
        with archive.open(name) as packaged:
            try:
                with ZipFile(packaged) as nested:
                    scan_archive(nested, nested_location, nested_sha256, contents, jdbc_locations)
            except BadZipFile as error:
                raise ValueError('Cannot inspect embedded JAR: ' + nested_location) from error


def is_license_notice(entry):
    if entry.startswith(('META-INF/license/', 'META-INF/licenses/', 'META-INF/licenses-binary/',
                         'META-INF/rivomesh-licenses/')):
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


def asf_review_blockers(notices, vendors, mysql_locations, unidentified_nested_jars):
    blockers = []
    # The shipped 8.4.0 community connector uses GPLv2 with the Universal FOSS
    # Exception, which is not an ASF policy exception for bundled software.
    if mysql_locations:
        blockers.append('BUNDLED_MYSQL_CONNECTOR_J')
    blockers.extend('MISSING_OR_CHANGED_VENDOR_LICENSE: ' + item['requiredLicenseEntry']
                    for item in vendors if not item['licenseMatchesUpstream'])
    blockers.extend('MISSING_PACKAGE_LICENSE_FILE: ' + entry for entry in PACKAGE_LICENSE_FILES if entry not in notices)
    blockers.extend(f"UNIDENTIFIED_NESTED_JAR: {item['location']} (sha256={item['sha256']})"
                    for item in unidentified_nested_jars)
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
    parser.add_argument('--expect-external-vendor-jdbc', action='store_true',
                        help='Require MySQL, GaussDB, GBase8c, Kingbase and Dameng JDBC components to be external')
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
        contents, vendors, jdbc_locations = embedded_metadata(args.artifact)
    except (ValueError, BadZipFile) as error:
        parser.error(str(error))
    if digest(args.artifact) != artifact_sha256:
        parser.error('The JAR changed during inventory')
    root = contents['JAR']
    embedded = {key for key, item in root['components'].items() if item['kind'] == 'MAVEN_PROPERTIES'}
    notices, binaries = root['notices'], root['binaries']
    if not embedded or not notices:
        parser.error('The JAR must contain dependency metadata and embedded notices')

    def entries(components):
        return [':'.join(parts) for parts in sorted(components)]

    packaged = {}
    nested_jars = []
    for location, archive in contents.items():
        components = archive['components']
        for key, evidence in components.items():
            packaged.setdefault(key, []).append({'location': location, **evidence})
        if location != 'JAR':
            nested_jars.append({
                'location': location, 'sha256': archive['sha256'],
                'componentCoordinates': entries(components),
                'coordinatesOutsideMavenBom': entries(components.keys() - dependencies.keys()),
                'licenseAndNoticeFiles': [{'entry': name, 'sha256': value}
                                          for name, value in archive['notices'].items()],
            })
    reviews = {
        ':'.join(key): declared_terms(dependencies.get(key, {})) for key in sorted(packaged)
        if key[0] != 'io.github.rivomesh'
    }
    mysql_locations = jdbc_locations.pop('mysql')
    for driver, matches in jdbc_locations.items():
        if matches:
            _, _, component = VENDOR_JDBC_MARKERS[driver]
            reviews.setdefault(component, declared_terms(dependencies.get(tuple(component.split(':')), {})))
    unidentified_nested_jars = [item for item in nested_jars if not item['componentCoordinates']]
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
        'packagedComponentCoordinates': entries(packaged),
        'packagedCoordinatesOutsideMavenBom': entries(packaged.keys() - dependencies.keys()),
        'mavenComponentsWithoutPackagedEvidence': entries(dependencies.keys() - packaged.keys()),
        'packagedComponentEvidence': {':'.join(key): packaged[key] for key in sorted(packaged)},
        'embeddedNestedJars': nested_jars,
        'unidentifiedNestedJars': [item['location'] for item in unidentified_nested_jars],
        'vendoredBinaryMarkers': vendors,
        'embeddedLicenseAndNoticeFiles': [{'entry': name, 'sha256': value} for name, value in notices.items()],
        'embeddedBinaryFiles': [{'entry': name, 'sha256': value} for name, value in binaries.items()],
        'mysqlConnectorJLocations': mysql_locations,
        'vendorJdbcLocations': jdbc_locations,
        'requiredComponentReviews': reviews,
        'asfDistributionReview': {
            'status': 'NOT_RECORDED',
            'blockingFindings': asf_review_blockers(notices, vendors, mysql_locations, unidentified_nested_jars),
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
    print(f'INVENTORY_GENERATED {len(dependencies)} Maven components, {len(embedded)} outer Maven coordinates, '
          f'{len(packaged)} packaged coordinates, {len(packaged.keys() - dependencies.keys())} outside the BOM, '
          f'{len(nested_jars)} nested JARs, {len(vendors)} vendor markers, {len(notices)} outer notice/license files')
    if args.asf_review:
        print('ASF_DISTRIBUTION_REVIEW_' + review_status['status'])
    if args.expect_external_mysql:
        print('MYSQL_JDBC_EXTERNAL_CHECK_' + ('FAILED' if mysql_locations else 'PASSED'))
    contains_vendor_jdbc = bool(mysql_locations) or any(jdbc_locations.values())
    if args.expect_external_vendor_jdbc:
        print('VENDOR_JDBC_EXTERNAL_CHECK_' + ('FAILED' if contains_vendor_jdbc else 'PASSED'))
    return int(review_status['status'] == 'BLOCKED'
               or (args.expect_external_mysql and bool(mysql_locations))
               or (args.expect_external_vendor_jdbc and contains_vendor_jdbc))


if __name__ == '__main__':
    raise SystemExit(main())
