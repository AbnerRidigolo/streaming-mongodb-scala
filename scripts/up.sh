#!/usr/bin/env bash
# One command for the whole stack: completes .env with random MongoDB
# passwords when they are missing (never printed), then builds and starts
# every service. Extra arguments go to `docker compose up` (e.g. a service list).
# Runs in Git Bash on Windows, macOS and Linux.
#
# Usage: scripts/up.sh [service ...]
set -euo pipefail
cd "$(dirname "$0")/.."

scripts/gen_env.sh
docker compose up -d --build "$@"

cat <<'EOF'

Stack starting. First data shows up within a few minutes:
  Dashboard (Streamlit)        http://localhost:8501
  Order lookup (MongoDB)       http://localhost:8501/Consulta_de_pedido
  order-status-service API     http://localhost:8088/orders/order_000000
  Kafka Connect                http://localhost:8083/connectors?expand=status
  Grafana (admin / admin)      http://localhost:3000
  Kafka UI                     http://localhost:8080
  Spark UIs                    http://localhost:4040 (Python)  http://localhost:4041 (Scala)
Check both paths into MongoDB end to end: scripts/smoke_e2e.sh
EOF
