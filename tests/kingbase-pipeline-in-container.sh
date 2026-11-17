#!/usr/bin/env bash
set -euo pipefail
mkdir -p target/kingbase-pipeline-tests
javac -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar -d target/kingbase-pipeline-tests tests/kingbase/KingbasePipelineAcceptance.java
java -Xmx256m -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:target/kingbase-pipeline-tests io.github.rivomesh.acceptance.KingbasePipelineAcceptance
