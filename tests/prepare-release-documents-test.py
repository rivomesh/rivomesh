#!/usr/bin/env python3
"""Check opt-in release document preparation without licensing material or build dependencies."""

from argparse import Namespace
from hashlib import sha256
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import warnings
from zipfile import ZIP_DEFLATED, ZIP_STORED, ZipFile, ZipInfo


TOOL = Path(__file__).with_name('prepare-release-documents.py')
SPEC = importlib.util.spec_from_file_location('prepare_release_documents', TOOL)
prepare = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(prepare)
KNOWN = b'Synthetic upstream notice for example:dependency:1.0\n'
KNOWN_REFERENCE = {sha256(KNOWN).hexdigest(): ('example:dependency:1.0', 'META-INF/NOTICE')}
MANIFEST = 'META-INF/MANIFEST.MF'
APP_CLASS = 'io/github/rivomesh/Main.class'


class PrepareReleaseDocumentsTest(unittest.TestCase):
    def setUp(self):
        temporary = self.enterContext(tempfile.TemporaryDirectory())
        self.directory = Path(temporary).resolve()
        self.artifact = self.directory / 'original.jar'
        self.output = self.directory / 'prepared.jar'
        self.license = self.directory / 'approved-license'
        self.notice = self.directory / 'approved-notice'
        self.license.write_bytes(b'Synthetic project licensing input\r\n')
        self.notice.write_bytes(b'Synthetic project attributions\n')
        self.source()

    def source(self, entries=None, duplicate=False):
        with ZipFile(self.artifact, 'w', compression=ZIP_DEFLATED) as archive:
            archive.comment = b'original JAR comment'
            archive.writestr(MANIFEST, b'Manifest-Version: 1.0\r\nMain-Class: io.github.rivomesh.Main\r\n\r\n')
            stored = ZipInfo(APP_CLASS, date_time=(2025, 1, 1, 0, 0, 0))
            stored.compress_type = ZIP_STORED
            stored.comment = b'original member comment'
            stored.external_attr = 0o100644 << 16
            archive.writestr(stored, b'synthetic bytes')
            directory = ZipInfo('javax/ejb/')
            archive.writestr(directory, b'')
            directory.external_attr = 0
            archive.writestr('META-INF/NOTICE', KNOWN)
            archive.writestr('NOTICE', KNOWN)
            archive.writestr('META-INF/LICENSE', b'Unattributed license text from original package')
            archive.writestr('META-INF/rivomesh-licenses/vendor.txt', b'Original dedicated license attribution')
            for entry, content in (entries or {}).items():
                archive.writestr(entry, content)
            if duplicate:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore', UserWarning)
                    archive.writestr('META-INF/NOTICE', b'Duplicate notice')

    def arguments(self, **overrides):
        result = dict(artifact=self.artifact, license=self.license, notice=self.notice, output=self.output)
        for field in ('artifact', 'license', 'notice'):
            result[field + '_sha256'] = sha256(result[field].read_bytes()).hexdigest()
        return Namespace(**(result | overrides))

    def test_installs_exact_documents_and_preserves_original_archive_members(self):
        expected = self.arguments().artifact_sha256
        with patch.dict(prepare.inventory.DEPENDENCY_DOCUMENTS, KNOWN_REFERENCE):
            result = prepare.prepare(self.arguments())
            second = prepare.prepare(self.arguments(output=self.directory / 'repeat.jar'))
        self.assertEqual(result, second)
        self.assertEqual(expected, sha256(self.artifact.read_bytes()).hexdigest())
        self.assertEqual(result['artifactSha256'], sha256(self.output.read_bytes()).hexdigest())
        with ZipFile(self.artifact) as original, ZipFile(self.output) as prepared:
            self.assertEqual(original.comment, prepared.comment)
            self.assertEqual(prepare.inventory.OUTER_PACKAGE_DOCUMENTS, tuple(item['entry'] for item in result['projectDocuments']))
            self.assertEqual(len(prepared.namelist()), len(set(prepared.namelist())))
            for entry in prepare.inventory.OUTER_PACKAGE_DOCUMENTS:
                self.assertEqual(self.license.read_bytes() if entry.endswith('LICENSE') else self.notice.read_bytes(),
                                 prepared.read(entry))
            for entry in set(original.namelist()) - set(prepare.inventory.OUTER_PACKAGE_DOCUMENTS):
                self.assertEqual(original.read(entry), prepared.read(entry))
                before, after = original.getinfo(entry), prepared.getinfo(entry)
                for field in ('compress_type', 'comment', 'date_time', 'external_attr', 'extra'):
                    self.assertEqual(getattr(before, field), getattr(after, field))
            metadata = json.loads(prepared.read(prepare.PROVENANCE_ENTRY))
            self.assertEqual(expected, metadata['sourceArtifactSha256'])
            self.assertEqual(result['preservedOuterDocuments'], metadata['preservedOuterDocuments'])
            self.assertEqual(3, len(metadata['preservedOuterDocuments']))
            for document in metadata['preservedOuterDocuments']:
                self.assertEqual(original.read(document['originalEntry']), prepared.read(document['entry']))
                if document['originalEntry'].endswith('NOTICE'):
                    self.assertIn('/upstream/', document['entry'])
                    self.assertEqual('example:dependency:1.0', document['referenceComponent'])
                else:
                    self.assertIn('/from-shaded-package/', document['entry'])
                    self.assertNotIn('referenceComponent', document)
        self.assertFalse(list(self.directory.glob('.rivomesh-release-*')))

    def test_requires_document_pins_and_rejects_invalid_inputs(self):
        invocation = subprocess.run([sys.executable, '-B', str(TOOL), '--artifact', str(self.artifact)],
                                    text=True, capture_output=True)
        self.assertEqual(2, invocation.returncode)
        self.assertIn('--license-sha256', invocation.stderr)
        with self.assertRaisesRegex(prepare.PreparationError, 'RELEASE_DOCUMENT_DIGEST_OR_CONTENT_INVALID: NOTICE'):
            prepare.prepare(self.arguments(notice_sha256='0' * 64))
        with self.assertRaisesRegex(prepare.PreparationError, 'RELEASE_INPUT_ARTIFACT_DIGEST_MISMATCH'):
            prepare.prepare(self.arguments(artifact_sha256='0' * 64))
        self.notice.write_bytes(b'  \n')
        with self.assertRaisesRegex(prepare.PreparationError, 'RELEASE_DOCUMENT_DIGEST_OR_CONTENT_INVALID: NOTICE'):
            prepare.prepare(self.arguments())
        self.notice.write_bytes(KNOWN)
        with patch.dict(prepare.inventory.DEPENDENCY_DOCUMENTS, KNOWN_REFERENCE):
            with self.assertRaisesRegex(prepare.PreparationError, 'RELEASE_DOCUMENT_MATCHES_DEPENDENCY: NOTICE'):
                prepare.prepare(self.arguments())
        self.assertFalse(self.output.exists())

    def test_refuses_signed_duplicate_and_reserved_archive_members(self):
        inputs = [({'META-INF/SIGNER.SF': b'synthetic signature'}, False, 'RELEASE_INPUT_SIGNED_JAR'),
                  ({}, True, 'RELEASE_INPUT_DUPLICATE_ZIP_ENTRY'),
                  ({prepare.PROVENANCE_ENTRY: '{}'}, False, 'RELEASE_INPUT_ALREADY_CONTAINS_PREPARATION_METADATA')]
        for entries, duplicate, reason in inputs:
            with self.subTest(reason=reason):
                self.source(entries, duplicate)
                with self.assertRaisesRegex(prepare.PreparationError, reason):
                    prepare.prepare(self.arguments())
                self.assertFalse(self.output.exists())
                self.assertFalse(list(self.directory.glob('.rivomesh-release-*')))

    def test_output_never_replaces_existing_file_or_follows_symlinks(self):
        baseline = self.artifact.read_bytes()
        with self.assertRaisesRegex(prepare.PreparationError, 'RELEASE_OUTPUT_EXISTS'):
            prepare.prepare(self.arguments(output=self.artifact))
        self.assertEqual(baseline, self.artifact.read_bytes())
        missing = self.directory / 'not-created.jar'
        self.output.symlink_to(missing)
        with self.assertRaisesRegex(prepare.PreparationError, 'RELEASE_OUTPUT_EXISTS'):
            prepare.prepare(self.arguments())
        self.assertTrue(self.output.is_symlink())
        self.assertFalse(missing.exists())
        real_parent = self.directory / 'target'
        real_parent.mkdir()
        linked_parent = self.directory / 'symlink'
        linked_parent.symlink_to(real_parent, target_is_directory=True)
        with self.assertRaisesRegex(prepare.PreparationError, 'RELEASE_OUTPUT_DIRECTORY_UNAVAILABLE'):
            prepare.prepare(self.arguments(output=linked_parent / 'candidate.jar'))
        self.assertFalse(list(real_parent.iterdir()))

    def test_concurrent_destination_appearance_preserves_winner(self):
        original_link = os.link

        def competing_output(source, destination, **options):
            self.output.write_bytes(b'Existing builder output')
            return original_link(source, destination, **options)

        with patch.object(prepare.os, 'link', side_effect=competing_output):
            with self.assertRaisesRegex(prepare.PreparationError, 'RELEASE_OUTPUT_EXISTS'):
                prepare.prepare(self.arguments())
        self.assertEqual(b'Existing builder output', self.output.read_bytes())
        self.assertFalse(list(self.directory.glob('.rivomesh-release-*')))


if __name__ == '__main__':
    unittest.main()
