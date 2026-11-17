#!/usr/bin/env bash
set -euo pipefail
mkdir -p target/hive-pipeline-tests
javac -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar -d target/hive-pipeline-tests tests/hive/HivePipelineAcceptance.java
java -Xmx384m -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:target/hive-pipeline-tests io.github.rivomesh.acceptance.HivePipelineAcceptance
