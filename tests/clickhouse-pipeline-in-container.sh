#!/usr/bin/env bash
set -euo pipefail
if [[ -n "${RIVOMESH_TEST_ARTIFACT:-}" || -n "${RIVOMESH_TEST_CLASSES:-}" ]]; then
  test -f "${RIVOMESH_TEST_ARTIFACT:?Specify the packaged application}"
  test -d "${RIVOMESH_TEST_CLASSES:?Specify the corresponding compiled tests}"
else
  mvn -B -s build/settings.xml -DskipTests package
  RIVOMESH_TEST_ARTIFACT=/project/sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar
  RIVOMESH_TEST_CLASSES=/project/sync-app/target/test-classes
fi
java -Dsync.clickhouse.integration=true \
  -Dsync.test.jar="$RIVOMESH_TEST_ARTIFACT" \
  -cp "$RIVOMESH_TEST_ARTIFACT:$RIVOMESH_TEST_CLASSES:/project/target/tools/junit-platform-console-standalone-1.11.0.jar" \
  org.junit.platform.console.ConsoleLauncher execute --disable-banner --fail-if-no-tests \
  --select-class io.github.rivomesh.ClickHousePipelineIntegrationTest \
  --select-class io.github.rivomesh.PipelineConfigTest \
  --select-class io.github.rivomesh.cdc.ClickHouseHttpClientTest \
  --select-class io.github.rivomesh.cdc.ClickHouseIndexPlanTest \
  --select-class io.github.rivomesh.cdc.MessageSinkTest \
  --reports-dir /project/sync-app/target/surefire-reports/clickhouse-pipeline
