#!/usr/bin/env bash
# Smoke test of order-status-service and the MongoDB serving layer against the
# real stack. Needs only Docker, curl and bash (Git Bash on Windows).
#
# Brings up Kafka, Schema Registry, init, the three producers, the service,
# MongoDB and Kafka Connect, then checks that events flow end to end:
#   - the interactive-query endpoint answers for a sample order and metrics
#     count consumed events; order-status / order-alerts have Avro records;
#   - both MongoDB sink connectors are RUNNING, the sample order's document in
#     MongoDB has the status the service returns, alerts reach order_alerts,
#     both dead-letter queues are empty and the dashboard user cannot write.
# Prints what it saw.
#
# Usage: scripts/smoke_order_status.sh [timeout_seconds]
set -euo pipefail

TIMEOUT="${1:-300}"
SERVICE_URL="http://localhost:8088"
CONNECT_URL="http://localhost:8083"
# seed_data.py --sample names orders order_000000, order_000001, ...
ORDER_ID="order_000000"
SERVICES=(zookeeper kafka schema-registry init orders-producer payments-producer
          delivery-producer order-status-service mongo connect connect-init)

cd "$(dirname "$0")/.."

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
  # Prints up to $2 Avro records of topic $1 as JSON (key<TAB>value). The
  # consumer may log its config to stdout, indented with a tab: keep only
  # lines with a non-empty key before the tab.
  docker compose exec -T schema-registry kafka-avro-console-consumer \
    --bootstrap-server kafka:29092 --topic "$1" --from-beginning \
    --max-messages "$2" --timeout-ms 20000 \
    --property schema.registry.url=http://schema-registry:8081 \
    --property print.key=true \
    --key-deserializer org.apache.kafka.common.serialization.StringDeserializer \
    2>/dev/null | grep -E $'^[^\t]+\t' || true
}

mongo_eval() {
  # Runs JavaScript $1 in olist_serving as the read-only dashboard user. The
  # password stays inside the container; the script travels in an env var.
  docker compose exec -T -e JS="$1" mongo sh -c \
    'mongosh --quiet "mongodb://dashboard:${MONGO_DASHBOARD_PASSWORD}@localhost:27017/olist_serving?authSource=admin" --eval "$JS"'
}

api_status() {
  curl -fsS "$SERVICE_URL/orders/$ORDER_ID" \
    | grep -oE '"status" *: *"[A-Z_]+"' | head -1 | sed -E 's/.*"([A-Z_]+)"$/\1/'
}

mongo_status() {
  mongo_eval "const d = db.order_status.findOne({order_id: '$ORDER_ID'}); print(d ? d.status : '')"
}

connect_init_done() {
  local state
  state=$(docker inspect -f '{{.State.Status}}:{{.State.ExitCode}}' connect-init)
  case "$state" in
    exited:0) return 0 ;;
    exited:*) echo "connect-init failed ($state):" >&2
              docker compose logs --no-log-prefix connect-init >&2
              exit 1 ;;
    *) return 1 ;;
  esac
}

statuses_match() {
  local api mongo
  api=$(api_status) && mongo=$(mongo_status) && [ -n "$api" ] && [ "$api" = "$mongo" ]
}

# Compose refuses to start without the MongoDB passwords; adds only missing ones.
scripts/gen_env.sh

docker compose up -d --build "${SERVICES[@]}"

wait_for "order-status-service healthy" "curl -fsS $SERVICE_URL/health"
wait_for "state for $ORDER_ID" "curl -fsS $SERVICE_URL/orders/$ORDER_ID"

echo
echo "== GET /orders/$ORDER_ID"
curl -fsS "$SERVICE_URL/orders/$ORDER_ID"
echo

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

STATUS=$(api_status)
case "$STATUS" in
  CREATED|PAID|SHIPPED|DELIVERED|CANCELED) echo; echo "ok: API says $ORDER_ID is $STATUS" ;;
  *) echo "FAIL: unexpected status '$STATUS'" >&2; exit 1 ;;
esac

# ---- MongoDB serving layer -------------------------------------------------
echo
wait_for "connectors registered and RUNNING (connect-init)" connect_init_done
echo "== connector status"
curl -fsS "$CONNECT_URL/connectors?expand=status"
echo

# Producers keep replaying, so the state can change between the two reads:
# retry until the API and MongoDB agree.
wait_for "MongoDB order_status.$ORDER_ID matches the API" statuses_match
wait_for "alerts in MongoDB order_alerts" \
  "[ \"\$(mongo_eval 'print(db.order_alerts.countDocuments())')\" -gt 0 ]"

echo
echo "== MongoDB olist_serving.order_status, $ORDER_ID"
mongo_eval "printjson(db.order_status.findOne({order_id: '$ORDER_ID'}, {_id: 0}))"
echo "== MongoDB counts"
mongo_eval 'print("order_status:", db.order_status.countDocuments(), " order_alerts:", db.order_alerts.countDocuments())'

DENIED=$(mongo_eval 'try { db.order_status.insertOne({order_id: "smoke"}); print("ALLOWED") } catch (e) { print(e.codeName) }')
if [ "$DENIED" != "Unauthorized" ]; then
  echo "FAIL: dashboard user write returned '$DENIED', expected Unauthorized" >&2
  exit 1
fi
echo "ok: dashboard user is read-only (insert -> $DENIED)"

DLQ=$(docker compose exec -T kafka kafka-get-offsets --bootstrap-server kafka:29092 \
        --topic-partitions 'order-status-dlq:0,order-alerts-dlq:0')
echo "== dead-letter queues (topic:partition:end offset)"
echo "$DLQ"
if echo "$DLQ" | grep -qvE ':0$'; then
  echo "FAIL: records in a dead-letter queue" >&2
  exit 1
fi

MONGO_STATUS=$(mongo_status)
echo
echo "PASS: $ORDER_ID is $MONGO_STATUS in the API and in MongoDB"
