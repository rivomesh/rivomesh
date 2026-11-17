#!/usr/bin/env bash
set -euo pipefail
if [ "${RIVOMESH_TEST_PREBUILT:-0}" != "1" ]; then mvn -B -s build/settings.xml -DskipTests package; fi
sha256sum sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar
mkdir -p target/classic-native-tests
javac -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar -d target/classic-native-tests tests/classic/ClassicAcceptance.java tests/classic/ClassicInspector.java tests/classic/ClassicPipelineAcceptance.java
java -Xmx256m -Dsync.test.jar=/project/sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:target/classic-native-tests io.github.rivomesh.acceptance.ClassicPipelineAcceptance
