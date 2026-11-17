#!/usr/bin/env bash
set -euo pipefail
mkdir -p target/clickhouse-materializer-tests
# Production code comes exclusively from the release; compile only the fixture.
javac -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar -d target/clickhouse-materializer-tests \
  sync-app/src/test/java/io/github/rivomesh/cdc/ClickHouseMaterializerAcceptance.java
java -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:target/clickhouse-materializer-tests \
  io.github.rivomesh.cdc.ClickHouseMaterializerAcceptance
