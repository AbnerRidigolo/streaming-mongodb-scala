#!/bin/sh
# Registers every connector in $CONNECTORS_DIR (<name>.json holds its config)
# with PUT /connectors/<name>/config, which creates or updates it, so running
# this again is safe. Then waits until each connector and its tasks are
# RUNNING. POSIX sh + curl only (runs in the curlimages/curl image).
set -eu

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
CONNECTORS_DIR="${CONNECTORS_DIR:-/connectors}"
TIMEOUT="${TIMEOUT:-180}"

deadline=$(( $(date +%s) + TIMEOUT ))
past_deadline() { [ "$(date +%s)" -gt "$deadline" ]; }

for file in "$CONNECTORS_DIR"/*.json; do
  name=$(basename "$file" .json)
  # 409 while the worker group rebalances, connection errors while it starts.
  until code=$(curl -sS -o /tmp/response -w '%{http_code}' -X PUT \
                 -H 'Content-Type: application/json' --data @"$file" \
                 "$CONNECT_URL/connectors/$name/config") \
        && { [ "$code" = 200 ] || [ "$code" = 201 ]; }; do
    if past_deadline; then
      echo "failed to register $name (HTTP ${code:-none}):" >&2
      cat /tmp/response >&2 2>/dev/null || true
      exit 1
    fi
    sleep 3
  done
  echo "registered $name (HTTP $code)"
done

for file in "$CONNECTORS_DIR"/*.json; do
  name=$(basename "$file" .json)
  while :; do
    status=$(curl -sS "$CONNECT_URL/connectors/$name/status" || true)
    case "$status" in
      *'"state":"FAILED"'*)
        echo "$name failed: $status" >&2
        exit 1 ;;
    esac
    # Connector plus at least one task, none of them in another state.
    running=$(printf '%s' "$status" | grep -o '"state":"[A-Z]*"' | grep -c RUNNING || true)
    others=$(printf '%s' "$status" | grep -o '"state":"[A-Z]*"' | grep -vc RUNNING || true)
    if [ "$running" -ge 2 ] && [ "$others" -eq 0 ]; then
      echo "$name: connector and $((running - 1)) task(s) RUNNING"
      break
    fi
    if past_deadline; then
      echo "$name not RUNNING in time: $status" >&2
      exit 1
    fi
    sleep 3
  done
done
