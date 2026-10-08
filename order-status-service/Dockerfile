# syntax=docker/dockerfile:1
# Build context: repository root (the Avro input schemas come from producers/schemas).

# ---- build: compile, run the tests, assemble the distribution ----
FROM eclipse-temurin:17-jdk-jammy AS build
WORKDIR /src
COPY order-status-service/gradlew order-status-service/settings.gradle.kts order-status-service/build.gradle.kts order-status-service/
COPY order-status-service/gradle order-status-service/gradle
# Resolve dependencies in their own layer so code changes do not re-download them.
RUN cd order-status-service && ./gradlew --no-daemon -q dependencies > /dev/null
COPY producers/schemas producers/schemas
COPY order-status-service/src order-status-service/src
RUN cd order-status-service && ./gradlew --no-daemon installDist -x test

# ---- runtime: JRE only ----
FROM eclipse-temurin:17-jre-jammy
RUN useradd --system --uid 10001 app && mkdir -p /var/lib/kafka-streams && chown app /var/lib/kafka-streams
COPY --from=build /src/order-status-service/build/install/order-status-service /opt/order-status-service
USER app
ENV STATE_DIR=/var/lib/kafka-streams HTTP_PORT=8080
EXPOSE 8080
ENTRYPOINT ["/opt/order-status-service/bin/order-status-service"]
