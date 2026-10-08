#!/usr/bin/env bash
# Creates or completes .env with random MongoDB passwords; docker compose
# refuses to start without them. Only adds variables that are missing, so it
# is safe to run again and never rotates an existing password. Never prints
# the values. Runs in Git Bash on Windows, macOS and Linux.
#
# Usage: scripts/gen_env.sh [path/to/.env]
set -euo pipefail

ENV_FILE="${1:-$(dirname "$0")/../.env}"
VARS=(MONGO_ROOT_PASSWORD MONGO_CONNECT_PASSWORD MONGO_DASHBOARD_PASSWORD
      MONGO_SPARK_PASSWORD)

# 24 random bytes as hex: URI-safe, so it can go into a connection string as is.
random_password() {
  od -An -tx1 -N24 /dev/urandom | tr -d ' \n'
}

touch "$ENV_FILE"
chmod 600 "$ENV_FILE" 2>/dev/null || true
# A file saved without a final newline would glue the first new line to it.
if [ -s "$ENV_FILE" ] && [ -n "$(tail -c1 "$ENV_FILE")" ]; then
  echo >> "$ENV_FILE"
fi

added=()
for var in "${VARS[@]}"; do
  if grep -q "^${var}=." "$ENV_FILE"; then
    continue
  elif grep -q "^${var}=" "$ENV_FILE"; then
    # Present but empty, as in a copy of .env.example: fill it in place.
    sed -i "s|^${var}=.*\$|${var}=$(random_password)|" "$ENV_FILE"
  else
    echo "${var}=$(random_password)" >> "$ENV_FILE"
  fi
  added+=("$var")
done

if [ ${#added[@]} -eq 0 ]; then
  echo "$ENV_FILE already has every MongoDB password; nothing changed."
else
  echo "Added to $ENV_FILE: ${added[*]}"
fi
