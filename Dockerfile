ARG MAVEN_IMAGE=maven:3.9-eclipse-temurin-8
ARG JAVA_IMAGE=eclipse-temurin:8-jre-jammy
FROM ${MAVEN_IMAGE} AS build
WORKDIR /src
COPY pom.xml ./
COPY build/ ./build/
COPY gaussdb-driver/ ./gaussdb-driver/
COPY gbase8c-driver/ ./gbase8c-driver/
COPY pulsar-transport/ ./pulsar-transport/
COPY rocketmq-transport/ ./rocketmq-transport/
COPY artemis-transport/ ./artemis-transport/
COPY classic-transport/ ./classic-transport/
COPY hive-transport/ ./hive-transport/
COPY sync-app/ ./sync-app/
RUN mvn -B -s build/settings.xml -DskipTests package

FROM ${JAVA_IMAGE}
WORKDIR /app
RUN groupadd --gid 10001 rivomesh && useradd --uid 10001 --gid 10001 --no-create-home rivomesh \
    && mkdir -p /app/data/tasks /app/data/controller /app/config \
    && chown -R 10001:10001 /app
COPY --from=build --chown=10001:10001 /src/sync-app/target/rivomesh-app-0.1.0-SNAPSHOT.jar /app/rivomesh.jar
COPY --chown=10001:10001 deploy/container-platform.yml /app/config/platform.yml
USER 10001:10001
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD ["java", "-cp", "/app/rivomesh.jar", "io.github.rivomesh.platform.Healthcheck"]
ENTRYPOINT ["java", "-jar", "/app/rivomesh.jar"]
CMD ["serve", "/app/config/platform.yml"]
