#!/usr/bin/env bash
set -euo pipefail
if [ "${RIVOMESH_TEST_PREBUILT:-0}" != "1" ]; then mvn -B -s build/settings.xml -DskipTests package; fi
sha256sum sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar
mkdir -p target/configuration-acceptance
javac -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar -d target/configuration-acceptance tests/configuration/ConfigurationAcceptance.java
java -Xmx256m -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:target/configuration-acceptance io.github.rivomesh.acceptance.ConfigurationAcceptance
