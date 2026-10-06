package br.com.olist.orderstatus;

import java.time.Duration;
import java.util.Map;

/** Service configuration, read from environment variables (see docker-compose.yml). */
record AppConfig(
        String applicationId,
        String bootstrapServers,
        String schemaRegistryUrl,
        String ordersTopic,
        String paymentsTopic,
        String deliveryTopic,
        String statusTopic,
        String alertsTopic,
        int partitions,
        Duration paymentTimeout,
        Duration paymentCheckInterval,
        String stateDir,
        String advertisedHost,
        int httpPort) {

    static AppConfig fromEnv(Map<String, String> env) {
        int httpPort = Integer.parseInt(env.getOrDefault("HTTP_PORT", "8080"));
        return new AppConfig(
                env.getOrDefault("APPLICATION_ID", "order-status-service"),
                env.getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                env.getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
                env.getOrDefault("TOPIC_ORDERS_RAW", "orders-raw"),
                env.getOrDefault("TOPIC_PAYMENTS_RAW", "payments-raw"),
                env.getOrDefault("TOPIC_DELIVERY", "delivery-events"),
                env.getOrDefault("TOPIC_ORDER_STATUS", "order-status"),
                env.getOrDefault("TOPIC_ORDER_ALERTS", "order-alerts"),
                Integer.parseInt(env.getOrDefault("REPARTITION_PARTITIONS", "3")),
                Duration.ofMinutes(Long.parseLong(env.getOrDefault("PAYMENT_TIMEOUT_MINUTES", "15"))),
                Duration.ofSeconds(
                        Long.parseLong(env.getOrDefault("PAYMENT_CHECK_INTERVAL_SECONDS", "30"))),
                env.getOrDefault("STATE_DIR", "/tmp/kafka-streams"),
                env.getOrDefault("ADVERTISED_HOST", "localhost"),
                httpPort);
    }
}
