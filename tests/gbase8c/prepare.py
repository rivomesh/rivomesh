#!/usr/bin/env python3
"""Verify a separately obtained vendor installer and prepare the native 8c fixture."""
import argparse
import hashlib
from pathlib import Path
import shutil
import subprocess
import tarfile

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / 'target/gbase-research'
OUTER_SHA = '6946abaa7666cbfadea3957682020262a102e52c6f7b4632d1f93fee9d3400b7'
KERNEL_SHA = '69663acfbb786c3f05cc434fb9f6d42806609b69c818c6e6ce0c4234748ad582'


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', type=Path, help='GBase8cV6_S1.3.0B19_Unified_centos7.6_aarch64.tar.gz')
    args = parser.parse_args()
    if digest(args.archive) != OUTER_SHA:
        raise ValueError('GBASE8C_INSTALLER_DIGEST_MISMATCH')
    OUTPUT.mkdir(parents=True, exist_ok=True)
    nested = OUTPUT / 'gbase8c-kernel.tar.gz'
    with tarfile.open(args.archive) as archive:
        with archive.extractfile('GBase8cV6_S1.3.0B19_centos7.6_aarch64.tar.gz') as source, nested.open('wb') as out:
            shutil.copyfileobj(source, out)
    kernel = OUTPUT / 'kernel.tar.bz2'
    with tarfile.open(nested) as archive:
        member = next(m for m in archive if m.name.endswith('_aarch64.tar.bz2'))
        with archive.extractfile(member) as source, kernel.open('wb') as out:
            shutil.copyfileobj(source, out)
    if digest(kernel) != KERNEL_SHA:
        raise ValueError('GBASE8C_KERNEL_DIGEST_MISMATCH')
    with tarfile.open(kernel) as archive:
        archive.extractall(OUTPUT / 'kernel', filter='data')
    (OUTPUT / '.dockerignore').write_text('*\n!kernel/\n!kernel/**\n')
    subprocess.run(['docker', 'build', '-t', 'rivomesh-gbase8c-fixture:V6_S1.3.0B19', '-f',
                    str(ROOT / 'tests/gbase8c/Dockerfile'), str(OUTPUT)], cwd=ROOT, check=True)
    print('GBASE8C_FIXTURE_PREPARED: vendor installer/kernel verified; actual gbased, separate Linux userland')


if __name__ == '__main__':
    main()
