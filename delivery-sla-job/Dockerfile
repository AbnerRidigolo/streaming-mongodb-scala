# syntax=docker/dockerfile:1
# Build: sbt assembly (Scala 2.12, Java 11). Tests run in CI, not here.
FROM sbtscala/scala-sbt:eclipse-temurin-jammy-11.0.22_7_1.9.9_2.12.18 AS build
WORKDIR /build
COPY project project
COPY build.sbt .
# Resolve dependencies in their own layer so code changes do not re-download them.
RUN sbt -batch update
COPY src/main src/main
RUN sbt -batch assembly

# Run: Spark 3.4.1 (Scala 2.12, Java 11) provides Spark; the assembly JAR
# brings Kafka source, Delta and the MongoDB connector.
FROM apache/spark:3.4.1-scala2.12-java11-ubuntu
# Root, like the other pipeline containers: ./data is a bind mount created by
# root-owned containers, and the checkpoint volume starts root-owned.
USER root
COPY --from=build /build/target/scala-2.12/delivery-sla-job.jar /opt/delivery-sla-job/delivery-sla-job.jar
ENV SPARK_DRIVER_MEMORY=1g
ENTRYPOINT ["sh", "-c", "exec /opt/spark/bin/spark-submit --master \"local[${SPARK_CORES:-2}]\" --driver-memory \"$SPARK_DRIVER_MEMORY\" --class br.com.olist.deliverysla.DeliverySlaJob /opt/delivery-sla-job/delivery-sla-job.jar"]
