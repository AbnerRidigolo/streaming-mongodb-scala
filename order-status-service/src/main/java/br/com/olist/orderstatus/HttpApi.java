package br.com.olist.orderstatus;

import br.com.olist.orderstatus.avro.OrderState;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HTTP endpoints of the service.
 *
 * <ul>
 *   <li>{@code GET /orders/{orderId}}: current state, read from the local state store
 *       (Kafka Streams interactive queries). If another instance owns the order's
 *       partition, answers 307 with that instance's URL.
 *   <li>{@code GET /metrics}: Prometheus scrape endpoint.
 *   <li>{@code GET /health}: 200 while Kafka Streams is running or rebalancing.
 * </ul>
 */
final class HttpApi {

    private static final Logger LOG = LoggerFactory.getLogger(HttpApi.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final KafkaStreams streams;
    private final HostInfo self;
    private final PrometheusMeterRegistry metrics;
    private final HttpServer server;

    HttpApi(KafkaStreams streams, HostInfo self, PrometheusMeterRegistry metrics) throws IOException {
        this.streams = streams;
        this.self = self;
        this.metrics = metrics;
        this.server = HttpServer.create(new InetSocketAddress(self.port()), 0);
        server.createContext("/orders/", this::handleOrder);
        server.createContext("/metrics", this::handleMetrics);
        server.createContext("/health", this::handleHealth);
        server.setExecutor(Executors.newFixedThreadPool(4));
    }

    void start() {
        server.start();
        LOG.info("HTTP API listening on :{}", self.port());
    }

    void stop() {
        server.stop(1);
    }

    private void handleOrder(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            send(exchange, 405, Map.of("error", "method not allowed"));
            return;
        }
        String raw = exchange.getRequestURI().getRawPath().substring("/orders/".length());
        String orderId = URLDecoder.decode(raw, StandardCharsets.UTF_8);
        if (orderId.isBlank() || orderId.contains("/")) {
            send(exchange, 400, Map.of("error", "expected /orders/{orderId}"));
            return;
        }
        try {
            KeyQueryMetadata where = streams.queryMetadataForKey(
                    OrderStatusTopology.ORDER_STATE_STORE, orderId, Serdes.String().serializer());
            if (where == null || KeyQueryMetadata.NOT_AVAILABLE.equals(where)) {
                send(exchange, 503, Map.of("error", "state store not available yet"));
                return;
            }
            HostInfo owner = where.activeHost();
            if (!self.equals(owner)) {
                String location = "http://%s:%d/orders/%s".formatted(owner.host(), owner.port(), raw);
                exchange.getResponseHeaders().add("Location", location);
                send(exchange, 307, Map.of("owner", owner.host() + ":" + owner.port()));
                return;
            }
            ReadOnlyKeyValueStore<String, OrderState> store = streams.store(
                    StoreQueryParameters.fromNameAndType(
                                    OrderStatusTopology.ORDER_STATE_STORE,
                                    QueryableStoreTypes.<String, OrderState>keyValueStore())
                            .withPartition(where.partition()));
            OrderState state = store.get(orderId);
            if (state == null) {
                send(exchange, 404, Map.of("error", "order not found", "order_id", orderId));
                return;
            }
            send(exchange, 200, toJsonMap(OrderLifecycle.toStatus(state)));
        } catch (InvalidStateStoreException e) {
            send(exchange, 503, Map.of("error", "state store migrating: " + e.getMessage()));
        }
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        byte[] body = metrics.scrape().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        write(exchange, 200, body);
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        KafkaStreams.State state = streams.state();
        send(exchange, state.isRunningOrRebalancing() ? 200 : 503, Map.of("state", state.name()));
    }

    /** Avro record → JSON-friendly map: timestamps as ISO-8601, enums as names. */
    static Map<String, Object> toJsonMap(SpecificRecord record) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Schema.Field field : record.getSchema().getFields()) {
            out.put(field.name(), toJsonValue(record.get(field.pos())));
        }
        return out;
    }

    private static Object toJsonValue(Object value) {
        if (value instanceof Instant instant) {
            return instant.toString();
        }
        if (value instanceof Enum<?> e) {
            return e.name();
        }
        if (value instanceof CharSequence cs) {
            return cs.toString();
        }
        if (value instanceof List<?> list) {
            return list.stream().map(HttpApi::toJsonValue).toList();
        }
        return value;
    }

    private static void send(HttpExchange exchange, int status, Object body) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        write(exchange, status, JSON.writeValueAsBytes(body));
    }

    private static void write(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }
}
