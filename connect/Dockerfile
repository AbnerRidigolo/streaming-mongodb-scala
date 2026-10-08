# syntax=docker/dockerfile:1
# Kafka Connect worker (Confluent Platform 7.5.0 = Kafka 3.5, Java 11) with the
# MongoDB sink connector. The connector's "-all" JAR bundles its own MongoDB
# driver; its bytecode targets Java 8, so it loads on this JVM.
FROM confluentinc/cp-kafka-connect:7.5.0

ARG MONGO_CONNECTOR_VERSION=3.1.2
# SHA-256 computed from the Maven Central artifact, whose published SHA-1
# (750e2ae6da3807c6a410145d5f56e801c192db2a) was checked when pinning.
ARG MONGO_CONNECTOR_SHA256=c19b187fd9957a3e3f21b55a43ea4a3ed2aa0ca268ffcbf72e21883576d9de84

# /usr/share/confluent-hub-components is on the image's default plugin.path.
RUN set -eu; \
    dir=/usr/share/confluent-hub-components/mongo-kafka-connect; \
    jar="mongo-kafka-connect-${MONGO_CONNECTOR_VERSION}-all.jar"; \
    mkdir -p "$dir"; \
    curl -fsSL -o "$dir/$jar" \
      "https://repo1.maven.org/maven2/org/mongodb/kafka/mongo-kafka-connect/${MONGO_CONNECTOR_VERSION}/$jar"; \
    echo "${MONGO_CONNECTOR_SHA256}  $dir/$jar" | sha256sum -c -
