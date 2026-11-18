#!/usr/bin/env python3
"""Check a quiescent LOCAL journal and matching target against one release JAR."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile


def digest(path):
    with path.open("rb") as archive:
        return hashlib.file_digest(archive, "sha256").hexdigest()


def run(command):
    return subprocess.run(command, check=True, text=True, capture_output=True, timeout=90)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--java", type=Path, required=True, help="Java 21 runtime executable")
    parser.add_argument("--javac", type=Path, required=True, help="Compiler executable supporting --release 21")
    parser.add_argument("--output", type=Path, required=True, help="Write the verification JSON here")
    args = parser.parse_args()
    artifact, java, javac = (path.resolve(strict=True) for path in (args.artifact, args.java, args.javac))
    artifact_digest = digest(artifact)
    version = run([java, "-version"]).stderr
    if not re.search(r'version "21(?:[."]|$)', version):
        parser.error("--java must be a Java 21 runtime")
    fixture = Path(__file__).resolve().parent / "roles" / "LocalStateRestoreAcceptance.java"
    with tempfile.TemporaryDirectory(prefix="rivomesh-local-state-restore-") as directory:
        root = Path(directory)
        classes = root / "classes"
        classes.mkdir()
        try:
            run([javac, "--release", "21", "-cp", artifact, "-d", classes, fixture])
            output = run([java, "-Dh2.bindAddress=127.0.0.1", "-cp", os.pathsep.join(map(str, (artifact, classes))),
                          "io.github.rivomesh.cdc.LocalStateRestoreAcceptance", root / "state"])
        except subprocess.CalledProcessError as error:
            raise SystemExit(error.stderr or error.stdout) from error
        proof = json.loads(output.stdout.splitlines()[-1])
        if proof.get("status") != "PASS" or digest(artifact) != artifact_digest:
            raise SystemExit("Fixture acceptance or frozen artifact verification failed")
    proof.update(artifactSha256=artifact_digest, java=version.splitlines()[0], cleanup=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(proof, indent=2) + "\n")
    print("LOCAL_STATE_RESTORE_ACCEPTED artifact=" + artifact_digest)


if __name__ == "__main__":
    main()
