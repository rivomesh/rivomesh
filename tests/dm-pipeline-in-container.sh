#!/usr/bin/env bash
set -euo pipefail
mkdir -p target/dm-pipeline-tests
javac -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar -d target/dm-pipeline-tests tests/dm/DmPipelineAcceptance.java
java -Xmx256m -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:target/dm-pipeline-tests io.github.rivomesh.acceptance.DmPipelineAcceptance
