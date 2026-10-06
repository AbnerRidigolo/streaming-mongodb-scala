package br.com.olist.orderstatus;

import static org.assertj.core.api.Assertions.assertThat;

import br.com.olist.events.DeliveryEvent;
import br.com.olist.events.DeliveryMetadata;
import br.com.olist.events.DeliveryStatus;
import br.com.olist.events.EventMetadata;
import br.com.olist.events.OrderEvent;
import br.com.olist.events.OrderEventType;
import br.com.olist.events.PaymentEvent;
import br.com.olist.events.PaymentMetadata;
import br.com.olist.events.PaymentType;
import br.com.olist.orderstatus.avro.AlertType;
import br.com.olist.orderstatus.avro.LifecycleStatus;
import br.com.olist.orderstatus.avro.OrderAlert;
import br.com.olist.orderstatus.avro.OrderState;
import br.com.olist.orderstatus.avro.OrderStatus;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class OrderStatusTopologyTest {

    private static final Instant T0 = Instant.parse("2024-03-01T12:00:00Z");
    private static final Duration TIMEOUT = Duration.ofMinutes(15);

    private final String scope = "order-status-test-" + UUID.randomUUID();
    private final AppConfig config = new AppConfig(
            "order-status-test", "unused:9092", "mock://" + scope,
            "orders-raw", "payments-raw", "delivery-events", "order-status", "order-alerts",
            3, TIMEOUT, Duration.ofSeconds(30), "unused", "localhost", 8080);

    private SimpleMeterRegistry meters;
    private TopologyTestDriver driver;
    private TestInputTopic<String, OrderEvent> orders;
    private TestInputTopic<String, PaymentEvent> payments;
    private TestInputTopic<String, DeliveryEvent> deliveries;
    private TestOutputTopic<String, OrderStatus> statuses;
    private TestOutputTopic<String, OrderAlert> alerts;

    @BeforeEach
    void setUp() {
        meters = new SimpleMeterRegistry();
        AvroSerdes avro = new AvroSerdes(config.schemaRegistryUrl());
        driver = new TopologyTestDriver(
                OrderStatusTopology.build(config, avro, meters), driverProperties());
        orders = driver.createInputTopic(
                config.ordersTopic(), new StringSerializer(), avro.<OrderEvent>value().serializer());
        payments = driver.createInputTopic(
                config.paymentsTopic(), new StringSerializer(), avro.<PaymentEvent>value().serializer());
        deliveries = driver.createInputTopic(
                config.deliveryTopic(), new StringSerializer(), avro.<DeliveryEvent>value().serializer());
        statuses = driver.createOutputTopic(
                config.statusTopic(), new StringDeserializer(), avro.<OrderStatus>value().deserializer());
        alerts = driver.createOutputTopic(
                config.alertsTopic(), new StringDeserializer(), avro.<OrderAlert>value().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
        MockSchemaRegistry.dropScope(scope);
    }

    // ---------------------------------------------------------------- lifecycle

    @Test
    void followsTheLifecycleCreatedPaidShippedDelivered() {
        orders.pipeInput("o1", order("o1", OrderEventType.ORDER_CREATED, T0), T0);
        send(payments, payment("o1", PaymentType.CREDIT_CARD, 120.0, T0.plusSeconds(60)));
        send(orders, order("o1", OrderEventType.ORDER_SHIPPED, T0.plusSeconds(120)));
        send(deliveries, delivery("o1", DeliveryStatus.PICKED_UP, T0.plusSeconds(130)));
        send(deliveries, delivery("o1", DeliveryStatus.DELIVERED, T0.plusSeconds(600)));

        List<LifecycleStatus> seen = statuses.readValuesToList().stream()
                .map(OrderStatus::getStatus).distinct().toList();
        assertThat(seen).containsExactly(
                LifecycleStatus.CREATED, LifecycleStatus.PAID,
                LifecycleStatus.SHIPPED, LifecycleStatus.DELIVERED);

        OrderStatus last = currentStatus("o1");
        assertThat(last.getCreatedAt()).isEqualTo(T0);
        assertThat(last.getPaidAt()).isEqualTo(T0.plusSeconds(60));
        assertThat(last.getShippedAt()).isEqualTo(T0.plusSeconds(120));
        assertThat(last.getDeliveredAt()).isEqualTo(T0.plusSeconds(600));
        assertThat(last.getLastDeliveryStatus()).isEqualTo("DELIVERED");
        assertThat(last.getPaidAmount()).isEqualTo(120.0);
        assertThat(last.getCustomerState()).isEqualTo("SP");
        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void cancellationIsTerminal() {
        orders.pipeInput("o1", order("o1", OrderEventType.ORDER_CREATED, T0), T0);
        send(payments, payment("o1", PaymentType.BOLETO, 80.0, T0.plusSeconds(30)));
        send(orders, order("o1", OrderEventType.ORDER_CANCELED, T0.plusSeconds(90)));

        assertThat(currentStatus("o1").getStatus()).isEqualTo(LifecycleStatus.CANCELED);
    }

    // ----------------------------------------------------------- out of order

    /** Every arrival order of the same events must converge to the same state. */
    @ParameterizedTest(name = "permutation {index}")
    @MethodSource("lifecyclePermutations")
    void outOfOrderArrivalConvergesToTheSameState(List<Integer> order) {
        List<Consumer<Instant>> steps = List.of(
                at -> orders.pipeInput("o1", order("o1", OrderEventType.ORDER_CREATED, T0), at),
                at -> orders.pipeInput("o1", order("o1", OrderEventType.ORDER_APPROVED, T0.plusSeconds(10)), at),
                at -> payments.pipeInput("o1", payment("o1", PaymentType.CREDIT_CARD, 99.9, T0.plusSeconds(20)), at),
                at -> deliveries.pipeInput("o1", delivery("o1", DeliveryStatus.PICKED_UP, T0.plusSeconds(30)), at),
                at -> deliveries.pipeInput("o1", delivery("o1", DeliveryStatus.DELIVERED, T0.plusSeconds(40)), at));
        Instant arrival = T0;
        for (int i : order) {
            steps.get(i).accept(arrival);
            arrival = arrival.plusMillis(1);
        }

        OrderStatus s = currentStatus("o1");
        assertThat(s.getStatus()).isEqualTo(LifecycleStatus.DELIVERED);
        assertThat(s.getCreatedAt()).isEqualTo(T0);
        assertThat(s.getApprovedAt()).isEqualTo(T0.plusSeconds(10));
        assertThat(s.getPaidAt()).isEqualTo(T0.plusSeconds(20));
        assertThat(s.getShippedAt()).isEqualTo(T0.plusSeconds(30));
        assertThat(s.getDeliveredAt()).isEqualTo(T0.plusSeconds(40));
        assertThat(s.getLastDeliveryStatus()).isEqualTo("DELIVERED");
        assertThat(s.getPaidAmount()).isEqualTo(99.9);
        assertThat(s.getEventCount()).isEqualTo(5);
        assertThat(s.getUpdatedAt()).isEqualTo(T0.plusSeconds(40));
        assertThat(alerts.isEmpty()).isTrue();
    }

    static Stream<List<Integer>> lifecyclePermutations() {
        List<List<Integer>> all = new ArrayList<>();
        permute(new ArrayList<>(List.of(0, 1, 2, 3, 4)), 0, all);
        return all.stream();
    }

    private static void permute(List<Integer> items, int k, List<List<Integer>> out) {
        if (k == items.size()) {
            out.add(List.copyOf(items));
            return;
        }
        for (int i = k; i < items.size(); i++) {
            java.util.Collections.swap(items, k, i);
            permute(items, k + 1, out);
            java.util.Collections.swap(items, k, i);
        }
    }

    // --------------------------------------------------------------- payments

    @Test
    void sumsDistinctPaymentsAndIgnoresResentOnes() {
        orders.pipeInput("o1", order("o1", OrderEventType.ORDER_CREATED, T0), T0);
        send(payments, payment("o1", PaymentType.VOUCHER, 20.0, T0.plusSeconds(5)));
        send(payments, payment("o1", PaymentType.CREDIT_CARD, 100.0, T0.plusSeconds(6)));
        // Same payment again with a new event_id (producer retry / replay loop).
        send(payments, payment("o1", PaymentType.CREDIT_CARD, 100.0, T0.plusSeconds(7)));

        OrderStatus s = currentStatus("o1");
        assertThat(s.getPaidAmount()).isEqualTo(120.0);
        assertThat(s.getPaymentCount()).isEqualTo(2);
        assertThat(s.getPaymentTypes()).containsExactly("CREDIT_CARD", "VOUCHER");
    }

    @Test
    void replayingAWholeLifecycleKeepsMilestonesAndAmounts() {
        for (int pass = 0; pass < 2; pass++) {
            Instant base = T0.plus(Duration.ofHours(pass));
            orders.pipeInput("o1", order("o1", OrderEventType.ORDER_CREATED, base), base);
            send(payments, payment("o1", PaymentType.BOLETO, 55.5, base.plusSeconds(5)));
            send(orders, order("o1", OrderEventType.ORDER_DELIVERED, base.plusSeconds(60)));
        }

        OrderStatus s = currentStatus("o1");
        assertThat(s.getCreatedAt()).isEqualTo(T0);
        assertThat(s.getDeliveredAt()).isEqualTo(T0.plusSeconds(60));
        assertThat(s.getPaidAmount()).isEqualTo(55.5);
        assertThat(s.getPaymentCount()).isEqualTo(1);
        assertThat(alerts.isEmpty()).isTrue();
    }

    // ----------------------------------------------------------------- alerts

    @Test
    void alertsWhenNoPaymentArrivesWithinTheTimeout() {
        orders.pipeInput("o1", order("o1", OrderEventType.ORDER_CREATED, T0), T0);
        advanceStreamTime(T0.plus(TIMEOUT).minusSeconds(60));
        assertThat(alerts.isEmpty()).isTrue();

        advanceStreamTime(T0.plus(TIMEOUT).plusSeconds(60));
        List<OrderAlert> raised = alerts.readValuesToList();
        assertThat(raised).hasSize(1);
        OrderAlert alert = raised.get(0);
        assertThat(alert.getAlertType()).isEqualTo(AlertType.PAYMENT_TIMEOUT);
        assertThat(alert.getOrderId()).isEqualTo("o1");
        assertThat(alert.getAlertId()).isEqualTo("PAYMENT_TIMEOUT:o1");
        assertThat(alert.getCreatedAt()).isEqualTo(T0);
        assertThat(meters.counter("order_status_alerts_total", "type", "PAYMENT_TIMEOUT").count())
                .isEqualTo(1.0);

        // Emitted once, even as stream time keeps advancing or the order changes.
        orders.pipeInput("o1", order("o1", OrderEventType.ORDER_APPROVED, T0.plusSeconds(10)),
                T0.plus(TIMEOUT).plusSeconds(90));
        advanceStreamTime(T0.plus(TIMEOUT.multipliedBy(3)));
        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void noPaymentTimeoutWhenPaidInTime() {
        orders.pipeInput("o1", order("o1", OrderEventType.ORDER_CREATED, T0), T0);
        payments.pipeInput("o1", payment("o1", PaymentType.CREDIT_CARD, 10.0, T0.plusSeconds(300)),
                T0.plusSeconds(300));
        advanceStreamTime(T0.plus(TIMEOUT.multipliedBy(2)));
        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void noPaymentTimeoutWhenPaymentArrivesBeforeTheOrder() {
        payments.pipeInput("o1", payment("o1", PaymentType.CREDIT_CARD, 10.0, T0), T0);
        orders.pipeInput("o1", order("o1", OrderEventType.ORDER_CREATED, T0.plusSeconds(5)),
                T0.plusSeconds(5));
        advanceStreamTime(T0.plus(TIMEOUT.multipliedBy(2)));
        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void noPaymentTimeoutForCanceledOrders() {
        orders.pipeInput("o1", order("o1", OrderEventType.ORDER_CREATED, T0), T0);
        orders.pipeInput("o1", order("o1", OrderEventType.ORDER_CANCELED, T0.plusSeconds(30)),
                T0.plusSeconds(30));
        advanceStreamTime(T0.plus(TIMEOUT.multipliedBy(2)));
        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void alertsWhenACanceledOrderIsDelivered() {
        orders.pipeInput("o1", order("o1", OrderEventType.ORDER_CREATED, T0), T0);
        send(payments, payment("o1", PaymentType.CREDIT_CARD, 10.0, T0.plusSeconds(1)));
        send(orders, order("o1", OrderEventType.ORDER_CANCELED, T0.plusSeconds(30)));
        send(deliveries, delivery("o1", DeliveryStatus.DELIVERED, T0.plusSeconds(600)));
        send(deliveries, delivery("o1", DeliveryStatus.DELIVERED, T0.plusSeconds(700)));

        List<OrderAlert> raised = alerts.readValuesToList();
        assertThat(raised).extracting(OrderAlert::getAlertType)
                .containsExactly(AlertType.DELIVERED_AFTER_CANCELLATION);
        assertThat(currentStatus("o1").getStatus()).isEqualTo(LifecycleStatus.CANCELED);
    }

    @Test
    void alertsOnDeliveredCanceledOrderInEitherArrivalOrder() {
        send(deliveries, delivery("o1", DeliveryStatus.DELIVERED, T0.plusSeconds(600)));
        send(orders, order("o1", OrderEventType.ORDER_CANCELED, T0.plusSeconds(30)));

        assertThat(alerts.readValuesToList()).extracting(OrderAlert::getAlertType)
                .containsExactly(AlertType.DELIVERED_AFTER_CANCELLATION);
    }

    // ------------------------------------------------------------ state store

    @Test
    void keepsTheCurrentStateQueryableByOrderId() {
        orders.pipeInput("o1", order("o1", OrderEventType.ORDER_CREATED, T0), T0);
        orders.pipeInput("o2", order("o2", OrderEventType.ORDER_CREATED, T0), T0);
        send(payments, payment("o2", PaymentType.DEBIT_CARD, 42.0, T0.plusSeconds(3)));

        KeyValueStore<String, OrderState> store =
                driver.getKeyValueStore(OrderStatusTopology.ORDER_STATE_STORE);
        assertThat(OrderLifecycle.status(store.get("o1"))).isEqualTo(LifecycleStatus.CREATED);
        assertThat(OrderLifecycle.status(store.get("o2"))).isEqualTo(LifecycleStatus.PAID);
        assertThat(store.get("missing")).isNull();
    }

    @Test
    void jsonViewUsesIsoTimestampsAndEnumNames() {
        orders.pipeInput("o1", order("o1", OrderEventType.ORDER_CREATED, T0), T0);
        Map<String, Object> json = HttpApi.toJsonMap(currentStatus("o1"));
        assertThat(json).containsEntry("order_id", "o1")
                .containsEntry("status", "CREATED")
                .containsEntry("created_at", "2024-03-01T12:00:00Z");
    }

    @Test
    void streamsConfigUsesExactlyOnceV2() {
        Properties p = OrderStatusApp.streamsProperties(config, new HostInfo("svc", 8080));
        assertThat(p.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)).isEqualTo(StreamsConfig.EXACTLY_ONCE_V2);
        assertThat(p.get(StreamsConfig.APPLICATION_SERVER_CONFIG)).isEqualTo("svc:8080");
    }

    // ---------------------------------------------------------------- helpers

    /** Pipes an event with its event time as the record timestamp, like the producers. */
    private static <V extends org.apache.avro.specific.SpecificRecord> void send(
            TestInputTopic<String, V> topic, V event) {
        Instant ts = (Instant) event.get(event.getSchema().getField("event_timestamp").pos());
        String key = (String) event.get(event.getSchema().getField("order_id").pos());
        topic.pipeInput(key, event, ts);
    }

    private OrderStatus currentStatus(String orderId) {
        return OrderLifecycle.toStatus(
                driver.<String, OrderState>getKeyValueStore(OrderStatusTopology.ORDER_STATE_STORE).get(orderId));
    }

    /** Stream time only moves with records; an unrelated order pushes it forward. */
    private void advanceStreamTime(Instant to) {
        orders.pipeInput("clock", order("clock", OrderEventType.ORDER_CANCELED, to), to);
    }

    private Properties driverProperties() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, config.applicationId());
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        // Forward every KTable update instead of the deduplicated cache flush.
        p.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        return p;
    }

    private static OrderEvent order(String orderId, OrderEventType type, Instant ts) {
        return OrderEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(type)
                .setOrderId(orderId)
                .setCustomerId("cust-" + orderId)
                .setSellerId("seller-1")
                .setPaymentValue(120.0)
                .setProductCategory("esporte_lazer")
                .setCustomerState("SP")
                .setEventTimestamp(ts)
                .setMetadata(new EventMetadata("test", "1.0", "junit"))
                .build();
    }

    private static PaymentEvent payment(String orderId, PaymentType type, double value, Instant ts) {
        return PaymentEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(orderId)
                .setPaymentType(type)
                .setPaymentValue(value)
                .setInstallments(1)
                .setEventTimestamp(ts)
                .setMetadata(new PaymentMetadata("test", "1.0", "junit"))
                .build();
    }

    private static DeliveryEvent delivery(String orderId, DeliveryStatus status, Instant ts) {
        return DeliveryEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(orderId)
                .setDeliveryStatus(status)
                .setCustomerState("SP")
                .setLatitude(-23.5)
                .setLongitude(-46.6)
                .setEventTimestamp(ts)
                .setMetadata(new DeliveryMetadata("test", "1.0", "junit"))
                .build();
    }
}
