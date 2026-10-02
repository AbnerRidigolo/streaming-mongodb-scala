# syntax=docker/dockerfile:1
FROM python:3.11-slim-bookworm

# JVM for the embedded SparkSession; curl for the container healthcheck.
# Bookworm is pinned because its default-jre is OpenJDK 17 (Spark 3.4 does not
# run on Java 21).
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir \
    "pyspark==3.4.1" \
    "delta-spark==2.4.0" \
    "streamlit==1.29.0" \
    "pandas==2.1.4" \
    "altair==5.2.0" \
    "confluent-kafka==2.3.0" \
    "structlog==23.2.0"

COPY . /app

EXPOSE 8501

CMD ["streamlit", "run", "app.py", \
     "--server.port=8501", "--server.address=0.0.0.0", \
     "--server.headless=true"]
