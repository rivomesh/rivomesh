#!/usr/bin/env bash
set -euo pipefail
mkdir -p target/oceanbase-pipeline-tests
artifact="${RIVOMESH_TEST_ARTIFACT:-sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar}"
javac -cp "$artifact" -d target/oceanbase-pipeline-tests tests/oceanbase/OceanBasePipelineAcceptance.java
java -Xmx256m -cp "$artifact":target/oceanbase-pipeline-tests io.github.rivomesh.acceptance.OceanBasePipelineAcceptance
