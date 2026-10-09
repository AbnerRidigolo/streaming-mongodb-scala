#!/usr/bin/env bash
# End-to-end check of both paths from Kafka into MongoDB, against the real
# stack. Needs only Docker, curl and bash (Git Bash on Windows).
#
# Brings up Kafka, Schema Registry, init, the three producers, MongoDB and
# both paths, then checks that events flow end to end:
#   1. Kafka Streams + Kafka Connect: the interactive-query endpoint answers
#      for a sample order and metrics count consumed events; order-status /
#      order-alerts have Avro records; both MongoDB sink connectors are
#      RUNNING, the sample order's document in MongoDB has the status the
#      service returns, alerts reach order_alerts, both dead-letter queues are
#      empty and the dashboard user cannot write.
#   2. Scala Spark job: the sample order's delivery timeline reaches
#      delivery_timeline with its estimated delivery date and an SLA status,
#      delivery_sla has the country summary and the Delta gold table exists.
# Prints what it saw. The Python Spark pipeline and the dashboards are not
# started (they do not write to MongoDB).
#
# Usage: scripts/smoke_e2e.sh [timeout_seconds]
set -euo pipefail

TIMEOUT="${1:-300}"
SERVICE_URL="http://localhost:8088"
CONNECT_URL="http://localhost:8083"
# seed_data.py --sample names orders order_000000, order_000001, ...
ORDER_ID="order_000000"
SERVICES=(zookeeper kafka schema-registry init orders-producer payments-producer
          delivery-producer order-status-service mongo connect connect-init
          delivery-sla-job)

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

delivery_sla_status() {
  # SLA status of the sample order, only once it carries the estimated date
  # (the business dates travel on order events; UNKNOWN means they did not).
  mongo_eval "const d = db.delivery_timeline.findOne({_id: '$ORDER_ID'}); print(d && d.estimated_delivery_ts ? d.sla_status : '')"
}

delivery_ready() {
  case "$(delivery_sla_status)" in
    ON_TIME|LATE|PENDING|CANCELED) return 0 ;;
    *) return 1 ;;
  esac
}

sla_ready() {
  local delivered
  delivered=$(mongo_eval "const d = db.delivery_sla.findOne({_id: 'all:BR'}); print(d ? Number(d.delivered_orders) : 0)")
  [ "${delivered:-0}" -gt 0 ]
}

dlq_records() {
  # Records ever written to the sinks' dead-letter queues (end offsets summed).
  docker compose exec -T kafka kafka-get-offsets --bootstrap-server kafka:29092 \
    --topic-partitions 'order-status-dlq:0,order-alerts-dlq:0' \
    | tr -d '\r' | awk -F: '{ s += $3 } END { print s + 0 }'
}

# Compose refuses to start without the MongoDB passwords; adds only missing ones.
scripts/gen_env.sh

docker compose up -d --build "${SERVICES[@]}"

wait_for "order-status-service healthy" "curl -fsS $SERVICE_URL/health"
# init has created the topics by now; the check below only counts new
# dead-letter records, so earlier incidents on a reused stack do not count.
DLQ_BEFORE=$(dlq_records)
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

DLQ_AFTER=$(dlq_records)
echo "== dead-letter queues, records (start of run -> now): $DLQ_BEFORE -> $DLQ_AFTER"
if [ "$DLQ_AFTER" -ne "$DLQ_BEFORE" ]; then
  echo "FAIL: records reached a dead-letter queue during this run" >&2
  exit 1
fi
STATUS_JSON=$(curl -fsS "$CONNECT_URL/connectors?expand=status")
if echo "$STATUS_JSON" | grep -q '"state":"FAILED"'; then
  echo "FAIL: a connector task is FAILED: $STATUS_JSON" >&2
  exit 1
fi
echo "ok: both sinks still RUNNING"

MONGO_STATUS=$(mongo_status)
echo "ok: path 1 (Kafka Streams + Kafka Connect): $ORDER_ID is $MONGO_STATUS in the API and in MongoDB"

# ---- Path 2: Scala Spark job ----------------------------------------------
echo
wait_for "delivery_timeline.$ORDER_ID with its estimated date (delivery-sla-job)" delivery_ready
wait_for "delivery_sla all:BR with delivered orders" sla_ready
wait_for "Delta gold table data/gold/delivery_timeline" "[ -d data/gold/delivery_timeline/_delta_log ]"

echo
echo "== MongoDB olist_serving.delivery_timeline, $ORDER_ID"
mongo_eval "printjson(db.delivery_timeline.findOne({_id: '$ORDER_ID'}))"
echo "== MongoDB olist_serving.delivery_sla (country and regions)"
mongo_eval 'db.delivery_sla.find({scope: {$ne: "state"}}).sort({_id: 1}).forEach(d => print(d._id, "delivered:", Number(d.delivered_orders), "late:", Number(d.late_orders), "on_time_rate:", d.on_time_rate, "avg_delay_days_when_late:", d.avg_delay_days_when_late))'

SLA_STATUS=$(delivery_sla_status)
echo
echo "PASS: path 1 (Kafka Streams + Connect): $ORDER_ID is $MONGO_STATUS in the API and in MongoDB;" \
     "path 2 (Scala job): its delivery SLA is $SLA_STATUS"
