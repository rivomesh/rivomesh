#!/usr/bin/env bash
set -euo pipefail
mkdir -p target/gbase8c-pipeline-tests
javac -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar -d target/gbase8c-pipeline-tests tests/gbase8c/Gbase8cPipelineAcceptance.java
java -Xmx256m -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:target/gbase8c-pipeline-tests io.github.rivomesh.acceptance.Gbase8cPipelineAcceptance
