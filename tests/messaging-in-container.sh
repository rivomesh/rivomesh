#!/usr/bin/env bash
set -euo pipefail
# Package first: every integration worker executes the distributable shaded JAR.
mvn -B -s build/settings.xml -DskipTests package
mvn -B -s build/settings.xml -Dsync.messaging.integration=true \
    -Dsync.test.jar=/project/sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar \
    -DtestSuite=messaging \
    -Dtest=MessagePipelineIntegrationTest,RedisMessagePublisherIntegrationTest,MessageSinkTest,PipelineConfigTest,PlatformTest,JournalTest,NormalizationTest,FanoutDispatcherTest \
    -Dsurefire.failIfNoSpecifiedTests=false verify
