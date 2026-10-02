# syntax=docker/dockerfile:1
FROM python:3.11-slim-bookworm

# Spark needs a JVM; procps provides `ps` which the Spark launcher relies on.
# Bookworm is pinned because its default-jre is OpenJDK 17: Spark 3.4 does not
# run on Java 21, the default in newer Debian releases.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir \
    "pyspark==3.4.1" \
    "delta-spark==2.4.0" \
    "confluent-kafka[avro,schemaregistry]==2.3.0" \
    "fastavro==1.9.4" \
    "requests==2.31.0" \
    "structlog==23.2.0" \
    "prometheus-client==0.19.0"

ENV PYTHONPATH=/app
COPY . /app

# Default command is overridden per service in docker-compose.yml.
CMD ["python", "pipeline_runner.py"]
