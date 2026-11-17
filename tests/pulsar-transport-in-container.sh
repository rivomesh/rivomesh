#!/usr/bin/env bash
set -euo pipefail
mvn -B -s build/settings.xml -DskipTests package
# The current release JAR contains both core and private SDK; do not shadow it with old classes.
java -cp sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar:pulsar-transport/target/test-classes \
    io.github.rivomesh.transport.pulsar.PulsarTransportAcceptance
