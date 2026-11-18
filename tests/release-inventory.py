#!/usr/bin/env python3
"""Bind a Maven dependency SBOM to the packaged Rivomesh JAR and inventory embedded evidence."""

import argparse
from hashlib import sha256
import json
from pathlib import Path
import re
from zipfile import ZipFile


def digest(path):
    value = sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


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
    notices = []
    with ZipFile(artifact) as archive:
        names = set(archive.namelist())
        for entry in sorted(names):
            if re.fullmatch(r'META-INF/maven/[^/]+/[^/]+/pom.properties', entry):
                p = properties(archive.read(entry))
                if p.keys() >= {'groupId', 'artifactId', 'version'}:
                    versions.add((p['groupId'], p['artifactId'], p['version']))
            if re.fullmatch(r'(?:META-INF/)?(?:LICENSE|LICENCE|NOTICE)(?:[.\-_][^/]*)?', entry,
                            flags=re.IGNORECASE) or entry.startswith('META-INF/rivomesh-licenses/') and not entry.endswith('/'):
                notices.append({'entry': entry, 'sha256': sha256(archive.read(entry)).hexdigest()})
        vendors = []
        if 'io/github/rivomesh/gaussdb/jdbc/Driver.class' in names:
            vendors.append({'component': 'gsjdbc4:gsjdbc4:1.1',
                            'embeddedClass': 'io/github/rivomesh/gaussdb/jdbc/Driver.class'})
        if 'com/gbase8c/Driver.class' in names:
            vendors.append({'component': 'GBase8c vendor JDBC 3.0.0B02',
                            'embeddedClass': 'com/gbase8c/Driver.class',
                            'inputJarSha256': 'fe115f05124f6b9f37e49c829a0def28f7d09a6d871dd62a0c9f856b0aaa7e91'})
    return versions, notices, vendors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact', required=True, type=Path)
    parser.add_argument('--maven-bom', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    bom = json.loads(args.maven_bom.read_text())
    product = bom['metadata']['component']
    if bom.get('bomFormat') != 'CycloneDX' or product.get('group') != 'io.github.rivomesh' or product.get('name') != 'rivomesh':
        parser.error('Provide the aggregated Rivomesh CycloneDX Maven BOM')
    dependencies = {coordinate(component): component for component in bom['components']}
    embedded, notices, vendors = embedded_metadata(args.artifact)
    if not embedded or not notices:
        parser.error('The JAR must contain dependency metadata and embedded notices')

    def entries(components):
        return [':'.join(parts) for parts in sorted(components)]

    report = {
        'artifactSha256': digest(args.artifact),
        'mavenBomSha256': digest(args.maven_bom),
        'cycloneDxVersion': bom['specVersion'],
        'mavenComponentCount': len(dependencies),
        'embeddedMavenCoordinates': entries(embedded),
        'embeddedCoordinatesOutsideMavenBom': entries(embedded.difference(dependencies)),
        'mavenComponentsWithoutDeclaredLicense': entries(key for key, component in dependencies.items()
                                                         if not component.get('licenses')),
        'vendoredBinaryMarkers': vendors,
        'embeddedLicenseAndNoticeFiles': notices,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(f'INVENTORY_ACCEPTED {len(dependencies)} Maven components, {len(embedded)} embedded coordinates, '
          f'{len(vendors)} vendor markers, {len(notices)} notice/license files')


if __name__ == '__main__':
    main()
