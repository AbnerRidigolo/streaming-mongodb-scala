# syntax=docker/dockerfile:1
# Init image: creates topics, registers schemas and prepares the Olist data so
# the stack needs nothing on the host but Docker. Build context: repo root.
FROM python:3.11-slim-bookworm

WORKDIR /app

RUN pip install --no-cache-dir \
    "confluent-kafka[avro,schemaregistry]==2.3.0" \
    "fastavro==1.9.4" \
    "requests==2.31.0" \
    "structlog==23.2.0" \
    "pandas==2.1.4" \
    "pyarrow==14.0.2"

COPY scripts/ /app/scripts/
COPY producers/schemas/ /app/producers/schemas/
COPY spark_jobs/__init__.py /app/spark_jobs/__init__.py
COPY spark_jobs/utils/__init__.py spark_jobs/utils/kafka_utils.py /app/spark_jobs/utils/

# Real Olist CSVs in data/raw are used as-is; without them a synthetic sample
# is generated (seed_data.py exits 1 when the CSVs are missing).
CMD ["sh", "-c", "python scripts/create_topics.py && python scripts/register_schemas.py && (python scripts/seed_data.py || python scripts/seed_data.py --sample)"]
