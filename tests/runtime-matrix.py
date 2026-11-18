#!/usr/bin/env python3
"""Run real SQL snapshot/CDC/content checks on Temurin 21 with both journal modes."""
import argparse
import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
import re
from pathlib import Path
import statistics
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
BENCHMARK = ROOT / "tests/benchmark.py"
METRICS = ("snapshotRowsPerSecond", "cdcChangesPerSecond", "afterWritesCatchUpSeconds")


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--java", nargs="+", choices=("21",),
                        default=["21"])
    parser.add_argument("--journal-modes", nargs="+", choices=("LOCAL", "EXTERNAL"),
                        default=["LOCAL", "EXTERNAL"])
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--artifact", type=Path, default=ROOT / "sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar")
    parser.add_argument("--cpu-limits", nargs="+",
                        help="CPU quota per container for each profile, e.g. 1 2")
    parser.add_argument("--repeat", type=int, default=1, help="Independent trials per CPU and journal configuration")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--storage-description", required=True)
    args = parser.parse_args()
    if not (10 <= args.rows <= 1000000 and 30 <= args.timeout <= 86400):
        parser.error("rows 10..1000000 and timeout 30..86400 required")
    try:
        quotas = [Decimal(value) for value in (args.cpu_limits or [os.environ.get("RIVOMESH_BENCH_CPUS", "1")])]
        valid_quotas = all(q.is_finite() and Decimal("0.01") <= q <= 32
                           and q * 10**9 == (q * 10**9).to_integral_value() for q in quotas)
    except InvalidOperation:
        valid_quotas = False
    if not 1 <= args.repeat <= 10 or not valid_quotas:
        parser.error("repeat 1..10 and cpu-limits 0.01..32 required")
    artifact = args.artifact.resolve()
    if not artifact.is_file():
        parser.error("release artifact does not exist")
    limits = [format(q.normalize(), "f") for q in dict.fromkeys(quotas)]
    directory = ROOT / "target/runtime-matrix" / uuid.uuid4().hex[:12]
    directory.mkdir(parents=True, mode=0o700)
    report = {"formatVersion": 2, "status": "RUNNING", "artifactSha256": file_hash(artifact),
              "benchmarkSha256": file_hash(BENCHMARK),
              "startedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "scope": "MySQL source -> PostgreSQL target, one keyed ASCII table, snapshot/CDC "
                       "and exact content checks. Not whole-platform/JDBC/MQ/TLS/failover acceptance "
                       "or a hardware throughput standard.", "profiles": [], "results": []}

    def save():
        temporary = directory / "matrix.json.tmp"
        temporary.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
        temporary.replace(directory / "matrix.json")

    save()
    try:
        configurations = [(runtime, mode, cpus) for runtime in dict.fromkeys(args.java)
                          for mode in dict.fromkeys(args.journal_modes) for cpus in limits]
        for trial in range(1, args.repeat + 1):
            for index, (runtime, mode, cpus) in enumerate(configurations, 1):
                profile = {"java": runtime, "journalMode": mode, "cpusPerContainer": cpus,
                           "trial": trial, "status": "RUNNING"}
                report["profiles"].append(profile)
                save()
                run_profile(args, artifact, directory, profile, report, index)
                save()
        for runtime, mode, cpus in configurations:
            trials = [p for p in report["profiles"] if
                      (p["java"], p["journalMode"], p["cpusPerContainer"]) == (runtime, mode, cpus)]
            if all(p["status"] == "PASSED" for p in trials):
                result = {"java": runtime, "journalMode": mode, "cpusPerContainer": cpus, "trials": len(trials)}
                for metric in METRICS:
                    values = [p["measurements"][metric] for p in trials]
                    result[metric] = {"minimum": min(values), "median": statistics.median(values), "maximum": max(values)}
                report["results"].append(result)
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


def run_profile(args, artifact, directory, profile, report, index):
    if file_hash(artifact) != report["artifactSha256"]:
        raise RuntimeError("ARTIFACT_CHANGED_DURING_MATRIX")
    if file_hash(BENCHMARK) != report["benchmarkSha256"]:
        raise RuntimeError("BENCHMARK_CHANGED_DURING_MATRIX")
    runtime, mode, cpus, trial = (profile[key] for key in ("java", "journalMode", "cpusPerContainer", "trial"))
    label = f"Java {runtime} / {mode} / {cpus} CPU / trial {trial}"
    name = f"profile-{index}-trial-{trial}"
    print("Starting " + label, flush=True)
    command = [sys.executable, str(BENCHMARK), "--artifact", str(artifact),
               "--rows", str(args.rows), "--java", runtime, "--journal-mode", mode,
               "--timeout", str(args.timeout), "--storage-description", args.storage_description]
    environment = dict(os.environ, RIVOMESH_BENCH_CPUS=cpus)
    with (directory / (name + ".stdout")).open("w") as out, (directory / (name + ".stderr")).open("w") as err:
        result = subprocess.run(command, cwd=ROOT, env=environment, stdout=out, stderr=err)
    if file_hash(BENCHMARK) != report["benchmarkSha256"]:
        raise RuntimeError("BENCHMARK_CHANGED_DURING_MATRIX")
    profile.update(status="FAILED", exitCode=result.returncode)
    lines = (directory / (name + ".stdout")).read_text().splitlines()
    candidate = Path(lines[-1]).resolve() if lines else None
    benchmark_root = (ROOT / "target/benchmarks").resolve()
    if candidate and benchmark_root in candidate.parents and candidate.is_file():
        raw = json.loads(candidate.read_text())
        profile["report"] = str(candidate.relative_to(ROOT))
        worker = raw.get("worker", {})
        profile["actualJavaVersion"] = worker.get("javaVersion")
        pattern = r'version "' + runtime + r'[.+"-]'
        profile["runtimeMatches"] = bool(re.search(pattern, profile["actualJavaVersion"] or ""))
        profile["artifactMatches"] = raw.get("artifactSha256") == report["artifactSha256"]
        containers = raw.get("containers", [])
        expected_nanos = int(Decimal(cpus) * 10**9)
        profile["quotaMatches"] = len(containers) == 2 and all(
            item.get("nanoCpus") == expected_nanos for item in [worker, *containers])
        values = raw.get("measurements", {})
        content_matches = all(values.get(phase + "SourceContentSha256")
                              and values[phase + "SourceContentSha256"] == values.get(phase + "TargetContentSha256")
                              for phase in ("snapshot", "cdc"))
        if (result.returncode == 0 and raw.get("status") == "PASSED"
                and raw.get("cleanupExitCode") == 0 and profile["artifactMatches"]
                and profile["runtimeMatches"] and profile["quotaMatches"]
                and raw.get("journalEvidence", {}).get("mode") == mode and content_matches):
            profile["status"] = "PASSED"
            profile["measurements"] = {metric: values[metric] for metric in METRICS}
    print(label + ": " + profile["status"], flush=True)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as failure:
        print("Runtime matrix failed (" + type(failure).__name__ + "); inspect its report.", file=sys.stderr)
        sys.exit(1)
