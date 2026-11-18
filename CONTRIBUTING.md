# Contributing to Rivomesh

Use the [SDD requirement list](docs/sdd/001-control-recovery-compatibility/requirements.json) to identify the scope of a change. Keep the English and Chinese specifications, task list and acceptance records aligned when behavior changes.

Build and verify with Java 8. The [current status](docs/en/implementation-status.md) lists the tested JDK, source, target and deployment profiles.

```sh
mkdir -p target/test-tmp
mvn -B -s build/settings.xml -Djava.io.tmpdir="$PWD/target/test-tmp" verify
python3 tests/sdd-check.py
npm --prefix tests/ui ci
npm --prefix tests/ui test
```

Use disposable databases for recovery tests. Include the pinned versions, artifact hash, commands and results in the change description. Upload concise diagnostics with authentication material and business rows removed.

Run the [candidate evidence gate](docs/en/release-inventory.md#candidate-evidence-gate) for release preparation.

See the [release inventory guide](docs/en/release-inventory.md) for the SBOM, embedded dependency markers and the distribution review still required. See the [Chinese project and acceptance notes](docs/zh-CN/requirements-and-decisions.md) for the same SDD workflow.
