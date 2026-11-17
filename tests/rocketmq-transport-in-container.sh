#!/usr/bin/env bash
set -euo pipefail
mvn -B -s build/settings.xml -DskipTests package
java -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:rocketmq-transport/target/test-classes \
    io.github.rivomesh.transport.rocketmq.RocketTransportAcceptance
