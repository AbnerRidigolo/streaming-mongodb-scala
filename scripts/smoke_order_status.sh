#!/usr/bin/env bash
# Smoke test of order-status-service against the real stack (Docker only).
#
# Brings up Kafka, Schema Registry, init, the three producers and the service,
# then checks that events flow end to end: the interactive-query endpoint
# answers for a sample order, metrics count consumed events, and the
# order-status / order-alerts topics have Avro records. Prints what it saw.
#
# Usage: scripts/smoke_order_status.sh [timeout_seconds]
set -euo pipefail

TIMEOUT="${1:-300}"
SERVICE_URL="http://localhost:8088"
# seed_data.py --sample names orders order_000000, order_000001, ...
ORDER_ID="order_000000"
SERVICES=(zookeeper kafka schema-registry init orders-producer payments-producer
          delivery-producer order-status-service)

wait_for() {
  local what="$1" cmd="$2" deadline=$((SECONDS + TIMEOUT))
  until eval "$cmd" > /dev/null 2>&1; do
    if (( SECONDS > deadline )); then
      echo "Timed out waiting for: $what" >&2
      return 1
    fi
    sleep 5
  done
  echo "ok: $what"
}

consume() {
  # Prints up to $2 Avro records of topic $1 as JSON (key<TAB>value).
  docker compose exec -T schema-registry kafka-avro-console-consumer \
    --bootstrap-server kafka:29092 --topic "$1" --from-beginning \
    --max-messages "$2" --timeout-ms 20000 \
    --property schema.registry.url=http://schema-registry:8081 \
    --property print.key=true \
    --key-deserializer org.apache.kafka.common.serialization.StringDeserializer \
    2>/dev/null
}

docker compose up -d --build "${SERVICES[@]}"

wait_for "order-status-service healthy" "curl -fsS $SERVICE_URL/health"
wait_for "state for $ORDER_ID" "curl -fsS $SERVICE_URL/orders/$ORDER_ID"

echo
echo "== GET /orders/$ORDER_ID"
curl -fsS "$SERVICE_URL/orders/$ORDER_ID" | python3 -m json.tool

echo
echo "== /metrics (order_status_*)"
curl -fsS "$SERVICE_URL/metrics" | grep -E '^order_status_' | sort

echo
echo "== order-status (3 records)"
consume order-status 3

# The delivery producer reports DELIVERED for canceled orders too, so
# DELIVERED_AFTER_CANCELLATION alerts show up within the first minutes.
wait_for "an alert on order-alerts" \
  "[ -n \"\$(consume order-alerts 1)\" ]"
echo
echo "== order-alerts (3 records)"
consume order-alerts 3

STATUS=$(curl -fsS "$SERVICE_URL/orders/$ORDER_ID" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')
case "$STATUS" in
  CREATED|PAID|SHIPPED|DELIVERED|CANCELED) echo; echo "PASS: $ORDER_ID is $STATUS" ;;
  *) echo "FAIL: unexpected status '$STATUS'" >&2; exit 1 ;;
esac
