#!/usr/bin/env bash
set -euo pipefail
mkdir -p target/doris-native-tests
javac -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar -d target/doris-native-tests tests/doris/DorisMaterializerAcceptance.java
java -Xmx256m -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:target/doris-native-tests io.github.rivomesh.cdc.DorisMaterializerAcceptance
