#!/usr/bin/env bash
set -euo pipefail
mkdir -p target/hive-native-tests
javac -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar -d target/hive-native-tests tests/hive/HiveMaterializerAcceptance.java
java -Xmx384m -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:target/hive-native-tests io.github.rivomesh.cdc.HiveMaterializerAcceptance
