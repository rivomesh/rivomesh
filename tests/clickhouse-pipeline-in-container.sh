#!/usr/bin/env bash
set -euo pipefail
mvn -B -s build/settings.xml -DskipTests package
java -Dsync.clickhouse.integration=true \
  -Dsync.test.jar=/project/sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar \
  -cp /project/sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:/project/sync-app/target/test-classes:/project/target/tools/junit-platform-console-standalone-1.11.0.jar \
  org.junit.platform.console.ConsoleLauncher execute --disable-banner --fail-if-no-tests \
  --select-class io.github.rivomesh.ClickHousePipelineIntegrationTest \
  --select-class io.github.rivomesh.PipelineConfigTest \
  --select-class io.github.rivomesh.cdc.ClickHouseHttpClientTest \
  --select-class io.github.rivomesh.cdc.ClickHouseIndexPlanTest \
  --select-class io.github.rivomesh.cdc.MessageSinkTest \
  --reports-dir /project/sync-app/target/surefire-reports/clickhouse-pipeline
