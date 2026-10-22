#!/bin/sh
# Entry point of the cloud producer image: synthetic sample, then one pass of
# the orders and delivery producers in parallel. Exits non-zero if either fails,
# so the Container Apps Job execution shows as failed.
set -eu

SAMPLE_ORDERS="${SAMPLE_ORDERS:-2000}"
export DATA_DIR=/tmp/olist/raw
export CUSTOMERS_PATH=/tmp/olist/reference/customers

python /app/scripts/seed_data.py --sample --orders "$SAMPLE_ORDERS" --customers 500

cd /app/producers
PRODUCER_METRICS_PORT=8000 PRODUCER_ID=cloud-orders python orders_producer.py &
orders=$!
PRODUCER_METRICS_PORT=8001 PRODUCER_ID=cloud-delivery python delivery_producer.py &
delivery=$!

status=0
wait "$orders" || status=1
wait "$delivery" || status=1
exit "$status"
