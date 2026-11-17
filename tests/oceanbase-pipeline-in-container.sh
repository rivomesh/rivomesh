#!/usr/bin/env bash
set -euo pipefail
mkdir -p target/oceanbase-pipeline-tests
javac -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar -d target/oceanbase-pipeline-tests tests/oceanbase/OceanBasePipelineAcceptance.java
java -Xmx256m -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:target/oceanbase-pipeline-tests io.github.rivomesh.acceptance.OceanBasePipelineAcceptance
