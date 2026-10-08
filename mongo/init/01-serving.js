// Runs once, on the first start of an empty data volume (docker-entrypoint-initdb.d),
// as the root user. Creates the serving database's least-privilege users and
// indexes. Passwords come from the container environment (.env), never from
// this file. Changing them later needs `docker compose down -v` (or updateUser).

const DB_NAME = "olist_serving";

function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is not set; run scripts/gen_env.sh`);
  }
  return value;
}

const admin = db.getSiblingDB("admin");
const users = [
  // Kafka Connect MongoDB sink: upserts order_status / order_alerts.
  { user: "connect", env: "MONGO_CONNECT_PASSWORD", role: "readWrite" },
  // Streamlit order lookup page.
  { user: "dashboard", env: "MONGO_DASHBOARD_PASSWORD", role: "read" },
  // Scala Spark job (phase 3).
  { user: "spark", env: "MONGO_SPARK_PASSWORD", role: "readWrite" },
];
for (const u of users) {
  admin.createUser({
    user: u.user,
    pwd: requireEnv(u.env),
    roles: [{ role: u.role, db: DB_NAME }],
  });
}

const serving = db.getSiblingDB(DB_NAME);

// The sink upserts by order_id (ReplaceOneBusinessKeyStrategy), so order_id
// must be unique; the second index serves "latest orders by status".
serving.order_status.createIndex({ order_id: 1 }, { unique: true });
serving.order_status.createIndex({ status: 1, updated_at: -1 });

// alert_id is deterministic (<type>:<order_id>), so the upsert is idempotent.
serving.order_alerts.createIndex({ alert_id: 1 }, { unique: true });
serving.order_alerts.createIndex({ order_id: 1 });

print(`${DB_NAME}: users ${users.map((u) => u.user).join(", ")} and indexes created`);
