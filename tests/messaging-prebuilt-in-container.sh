#!/usr/bin/env bash
set -euo pipefail
java -Dsync.messaging.integration=true \
  -Dsync.test.jar=/project/sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar \
  -cp /project/sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:/project/sync-app/target/test-classes:/project/target/tools/junit-platform-console-standalone-1.11.0.jar \
  org.junit.platform.console.ConsoleLauncher execute --disable-banner --fail-if-no-tests \
  --select-class io.github.rivomesh.MessagePipelineIntegrationTest \
  --reports-dir /project/sync-app/target/surefire-reports/messaging-prebuilt
