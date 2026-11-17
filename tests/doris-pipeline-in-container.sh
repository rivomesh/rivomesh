#!/usr/bin/env bash
set -euo pipefail
mkdir -p target/doris-pipeline-tests
javac -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar -d target/doris-pipeline-tests tests/doris/DorisPipelineAcceptance.java
java -Xmx256m -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:target/doris-pipeline-tests io.github.rivomesh.acceptance.DorisPipelineAcceptance
