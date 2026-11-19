#!/usr/bin/env python3
"""Assemble externally reviewed LICENSE and NOTICE bytes into a separate shaded JAR."""

import argparse
from copy import copy
from hashlib import sha256
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
from shutil import copyfileobj
import sys
from zipfile import BadZipFile, ZIP_DEFLATED, ZipFile, ZipInfo


SPEC = importlib.util.spec_from_file_location('release_inventory', Path(__file__).resolve().with_name('release-inventory.py'))
inventory = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(inventory)

PROVENANCE_ENTRY = 'META-INF/rivomesh-release-documents.json'
ATTRIBUTION_DIRECTORY = 'META-INF/rivomesh-licenses'
SIGNATURE_ENTRY = re.compile(r'META-INF/(?:SIG-[^/]+|[^/]+\.(?:SF|RSA|DSA|EC))', re.IGNORECASE)


class PreparationError(ValueError):
    pass


def reviewed_document(path, expected, label):
    content = path.read_bytes()
    if not content.strip() or sha256(content).hexdigest() != expected:
        raise PreparationError('RELEASE_DOCUMENT_DIGEST_OR_CONTENT_INVALID: ' + label)
    if expected in inventory.DEPENDENCY_DOCUMENTS:
        raise PreparationError('RELEASE_DOCUMENT_MATCHES_DEPENDENCY: ' + label)
    return content


def open_output_directory(path):
    # Walk through directory descriptors so symlinks cannot redirect creation or publication.
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    root, *parts = path.absolute().parts
    directory = os.open(root, flags)
    try:
        for part in parts:
            child = os.open(part, flags, dir_fd=directory)
            os.close(directory)
            directory = child
        return directory
    except OSError as error:
        os.close(directory)
        raise PreparationError('RELEASE_OUTPUT_DIRECTORY_UNAVAILABLE') from error


def write_document(archive, entry, content):
    member = ZipInfo(entry)
    member.compress_type = ZIP_DEFLATED
    archive.writestr(member, content)


def assemble_archive(source, output, documents, input_digest):
    entries = source.namelist()
    names = set(entries)
    if len(entries) != len(names):
        raise PreparationError('RELEASE_INPUT_DUPLICATE_ZIP_ENTRY')
    if any(SIGNATURE_ENTRY.fullmatch(entry) for entry in names):
        raise PreparationError('RELEASE_INPUT_SIGNED_JAR')
    if not {'META-INF/MANIFEST.MF', 'io/github/rivomesh/Main.class'}.issubset(names):
        raise PreparationError('RELEASE_INPUT_NOT_SHADED_APPLICATION')
    if PROVENANCE_ENTRY in names:
        raise PreparationError('RELEASE_INPUT_ALREADY_CONTAINS_PREPARATION_METADATA')

    provenance = {'formatVersion': 1, 'sourceArtifactSha256': input_digest,
                  'projectDocuments': [], 'preservedOuterDocuments': []}
    with ZipFile(output, 'w', compression=ZIP_DEFLATED) as candidate:
        candidate.comment = source.comment
        for member in source.infolist():
            if member.filename not in documents:
                copied = copy(member)
                with source.open(member) as original, candidate.open(copied, 'w') as unchanged:
                    copyfileobj(original, unchanged, 1024 * 1024)
                # zipfile fills in default attributes for a zero value. Keep the input central-directory field.
                copied.external_attr = member.external_attr
        for entry, document in documents.items():
            if entry in names:
                previous = source.read(entry)
                checksum = sha256(previous).hexdigest()
                owner = inventory.DEPENDENCY_DOCUMENTS.get(checksum)
                preserved = {'originalEntry': entry, 'sha256': checksum}
                directory = ATTRIBUTION_DIRECTORY + '/from-shaded-package'
                if owner:
                    component, upstream_entry = owner
                    slug = re.sub(r'[^A-Za-z0-9_.-]+', '_', component)
                    directory = ATTRIBUTION_DIRECTORY + '/upstream/' + slug
                    preserved.update(referenceComponent=component, referenceEntry=upstream_entry)
                location = directory + '/' + entry
                if location in names:
                    raise PreparationError('RELEASE_INPUT_ATTRIBUTION_PATH_COLLISION: ' + location)
                write_document(candidate, location, previous)
                provenance['preservedOuterDocuments'].append({'entry': location, **preserved})
            write_document(candidate, entry, document)
            provenance['projectDocuments'].append({'entry': entry, 'sha256': sha256(document).hexdigest()})
        write_document(candidate, PROVENANCE_ENTRY, json.dumps(provenance, indent=2) + '\n')

    output.seek(0)
    with ZipFile(output) as prepared:
        for entry, document in documents.items():
            if prepared.read(entry) != document:
                raise PreparationError('RELEASE_OUTPUT_PROJECT_DOCUMENT_MISMATCH: ' + entry)
        for document in provenance['preservedOuterDocuments']:
            if sha256(prepared.read(document['entry'])).hexdigest() != document['sha256']:
                raise PreparationError('RELEASE_OUTPUT_PRESERVED_DOCUMENT_MISMATCH: ' + document['entry'])
    return provenance


def prepare(args):
    if args.output.name in ('', '.', '..'):
        raise PreparationError('RELEASE_OUTPUT_PATH_INVALID')
    supplied = {'LICENSE': reviewed_document(args.license, args.license_sha256, 'LICENSE'),
                'NOTICE': reviewed_document(args.notice, args.notice_sha256, 'NOTICE')}
    documents = {entry: supplied[entry.rsplit('/', 1)[-1]] for entry in inventory.OUTER_PACKAGE_DOCUMENTS}
    with args.artifact.open('rb') as input_stream:
        if inventory.stream_digest(input_stream) != args.artifact_sha256:
            raise PreparationError('RELEASE_INPUT_ARTIFACT_DIGEST_MISMATCH')
        directory = open_output_directory(args.output.parent)
        try:
            try:
                os.stat(args.output.name, dir_fd=directory, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                raise PreparationError('RELEASE_OUTPUT_EXISTS')
            staging_name = '.rivomesh-release-' + secrets.token_hex(12)
            os.mkdir(staging_name, 0o700, dir_fd=directory)
            try:
                flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
                staging = os.open(staging_name, flags, dir_fd=directory)
                try:
                    provenance, digest = prepare_candidate(input_stream, args.artifact_sha256, documents,
                                                           staging, directory, args.output.name)
                finally:
                    os.close(staging)
            finally:
                os.rmdir(staging_name, dir_fd=directory)
        finally:
            os.close(directory)
    return {'status': 'RELEASE_DOCUMENTS_PREPARED', 'artifactSha256': digest, **provenance}


def prepare_candidate(input_stream, input_digest, documents, staging, directory, name):
    temporary_name = 'candidate.jar'
    flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    temporary = os.open(temporary_name, flags, 0o600, dir_fd=staging)
    try:
        with os.fdopen(temporary, 'w+b') as output, ZipFile(input_stream) as source:
            provenance = assemble_archive(source, output, documents, input_digest)
            input_stream.seek(0)
            if inventory.stream_digest(input_stream) != input_digest:
                raise PreparationError('RELEASE_INPUT_ARTIFACT_CHANGED')
            output.seek(0)
            digest = inventory.stream_digest(output)
            os.fsync(output.fileno())
        try:
            os.link(temporary_name, name, src_dir_fd=staging, dst_dir_fd=directory, follow_symlinks=False)
        except FileExistsError as error:
            raise PreparationError('RELEASE_OUTPUT_EXISTS') from error
        return provenance, digest
    finally:
        os.unlink(temporary_name, dir_fd=staging)


def checksum(value):
    if not re.fullmatch('[a-f0-9]{64}', value):
        raise argparse.ArgumentTypeError('Provide a lowercase SHA-256 digest')
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('artifact', 'license', 'notice', 'output'):
        parser.add_argument('--' + name, required=True, type=Path)
    for name in ('artifact', 'license', 'notice'):
        parser.add_argument('--' + name + '-sha256', required=True, type=checksum)
    try:
        result = prepare(parser.parse_args())
    except PreparationError as error:
        print(str(error), file=sys.stderr)
        return 1
    except (BadZipFile, NotImplementedError, RuntimeError, OSError) as error:
        print('RELEASE_PREPARATION_FAILED: ' + type(error).__name__, file=sys.stderr)
        return 1
    print(json.dumps(result, separators=(',', ':')))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
