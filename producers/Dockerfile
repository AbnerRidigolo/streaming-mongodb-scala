# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /app

# Avro + LZ4 + Schema Registry client deps.
RUN pip install --no-cache-dir \
    "confluent-kafka[avro,schemaregistry]==2.3.0" \
    "fastavro==1.9.4" \
    "requests==2.31.0" \
    "structlog==23.2.0" \
    "prometheus-client==0.19.0" \
    "python-dotenv==1.0.0"

COPY . /app

EXPOSE 8000

# Default command is overridden per service in docker-compose.yml.
CMD ["python", "orders_producer.py"]
