#!/usr/bin/env bash
set -euo pipefail
candidate="${RIVOMESH_TEST_ARTIFACT:-sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar}"
mkdir -p target/validation-barrier-tests
javac -cp "$candidate" -d target/validation-barrier-tests tests/validation/ValidationBarrierAcceptance.java
java -Xmx256m -cp "$candidate:target/validation-barrier-tests" io.github.rivomesh.acceptance.ValidationBarrierAcceptance
