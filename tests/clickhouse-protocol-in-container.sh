#!/usr/bin/env bash
set -euo pipefail
mkdir -p target/clickhouse-protocol-tests
javac -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar -d target/clickhouse-protocol-tests \
  sync-app/src/test/java/io/github/rivomesh/cdc/ClickHouseProtocolAcceptance.java
java -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:target/clickhouse-protocol-tests \
  io.github.rivomesh.cdc.ClickHouseProtocolAcceptance
