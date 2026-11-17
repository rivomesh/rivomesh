#!/usr/bin/env python3
"""Run real SQL snapshot/CDC/content checks across JRE and journal combinations."""
import argparse
import datetime
import hashlib
import json
import re
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]


def artifact_hash():
    digest = hashlib.sha256()
    with (ROOT / "sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar").open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java", nargs="+", choices=("8", "11", "17", "21", "25"),
                        default=["8", "11", "17", "21", "25"])
    parser.add_argument("--journal-modes", nargs="+", choices=("LOCAL", "EXTERNAL"),
                        default=["LOCAL", "EXTERNAL"])
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--storage-description", required=True)
    args = parser.parse_args()
    if not (10 <= args.rows <= 1000000 and 30 <= args.timeout <= 86400):
        parser.error("rows 10..1000000 and timeout 30..86400 required")
    directory = ROOT / "target/runtime-matrix" / uuid.uuid4().hex[:12]
    directory.mkdir(parents=True, mode=0o700)
    report = {"formatVersion": 1, "status": "RUNNING", "artifactSha256": artifact_hash(),
              "startedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "scope": "MySQL source -> PostgreSQL target, one keyed ASCII table, snapshot/CDC "
                       "and exact content checks. Not whole-platform/JDBC/MQ/TLS/failover acceptance "
                       "or a hardware throughput standard.", "profiles": []}

    def save():
        temporary = directory / "matrix.json.tmp"
        temporary.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
        temporary.replace(directory / "matrix.json")

    save()
    try:
        for runtime in dict.fromkeys(args.java):
            for mode in dict.fromkeys(args.journal_modes):
                if artifact_hash() != report["artifactSha256"]:
                    raise RuntimeError("ARTIFACT_CHANGED_DURING_MATRIX")
                profile = {"java": runtime, "journalMode": mode, "status": "RUNNING"}
                report["profiles"].append(profile)
                save()
                print("Starting Java " + runtime + " / " + mode, flush=True)
                command = [sys.executable, str(ROOT / "tests/benchmark.py"), "--rows", str(args.rows),
                           "--java", runtime, "--journal-mode", mode, "--timeout", str(args.timeout),
                           "--storage-description", args.storage_description]
                name = "java-" + runtime + "-" + mode.lower()
                with (directory / (name + ".stdout")).open("w") as out, \
                        (directory / (name + ".stderr")).open("w") as err:
                    result = subprocess.run(command, cwd=ROOT, stdout=out, stderr=err)
                profile["exitCode"] = result.returncode
                profile["status"] = "FAILED"
                lines = (directory / (name + ".stdout")).read_text().splitlines()
                candidate = Path(lines[-1]).resolve() if lines else None
                benchmark_root = (ROOT / "target/benchmarks").resolve()
                if candidate and benchmark_root in candidate.parents and candidate.is_file():
                    raw = json.loads(candidate.read_text())
                    profile["report"] = str(candidate.relative_to(ROOT))
                    profile["actualJavaVersion"] = raw.get("worker", {}).get("javaVersion")
                    pattern = r'version "1\.8[._"]' if runtime == "8" else r'version "' + runtime + r'[.+"-]'
                    profile["runtimeMatches"] = bool(re.search(pattern, profile["actualJavaVersion"] or ""))
                    profile["artifactMatches"] = raw.get("artifactSha256") == report["artifactSha256"]
                    values = raw.get("measurements", {})
                    content_matches = all(values.get(phase + "SourceContentSha256")
                                          and values[phase + "SourceContentSha256"] == values.get(phase + "TargetContentSha256")
                                          for phase in ("snapshot", "cdc"))
                    if (result.returncode == 0 and raw.get("status") == "PASSED"
                            and raw.get("cleanupExitCode") == 0 and profile["artifactMatches"]
                            and profile["runtimeMatches"]
                            and raw.get("journalEvidence", {}).get("mode") == mode
                            and content_matches):
                        profile["status"] = "PASSED"
                save()
                print("Java " + runtime + " / " + mode + ": " + profile["status"], flush=True)
        report["status"] = "PASSED" if all(p["status"] == "PASSED" for p in report["profiles"]) else "FAILED"
    except BaseException as failure:
        report["status"] = "FAILED"
        report["failureClass"] = type(failure).__name__
        raise
    finally:
        report["finishedAt"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        save()
        print(str(directory / "matrix.json"), flush=True)
    return 0 if report["status"] == "PASSED" else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as failure:
        print("Runtime matrix failed (" + type(failure).__name__ + "); inspect its report.", file=sys.stderr)
        sys.exit(1)
