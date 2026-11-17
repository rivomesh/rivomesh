#!/usr/bin/env bash
set -euo pipefail
python3 --version >/dev/null 2>&1 || true
mkdir -p target/classic-native-tests
javac -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar -d target/classic-native-tests tests/classic/ClassicAcceptance.java
java -Xmx256m -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:target/classic-native-tests io.github.rivomesh.acceptance.ClassicAcceptance
