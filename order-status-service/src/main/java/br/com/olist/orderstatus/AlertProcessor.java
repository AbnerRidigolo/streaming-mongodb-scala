package br.com.olist.orderstatus;

import br.com.olist.orderstatus.avro.AlertType;
import br.com.olist.orderstatus.avro.OrderAlert;
import br.com.olist.orderstatus.avro.OrderState;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Evaluates the business rules on every order-state change and emits {@link OrderAlert}s.
 *
 * <ul>
 *   <li><b>PAYMENT_TIMEOUT</b>: an order was created and, {@code paymentTimeout} later in
 *       stream time, has neither a payment event nor a cancellation. Unpaid orders wait in
 *       the {@code awaiting-payment} store; a stream-time punctuator checks their deadlines.
 *   <li><b>DELIVERED_AFTER_CANCELLATION</b>: the carrier reports DELIVERED for an order
 *       that was canceled (in either arrival order).
 * </ul>
 *
 * <p>Stream time (record timestamps) rather than wall-clock time keeps the rule correct
 * while catching up on a backlog: an order is not flagged just because its payment is
 * still further back in the payments topic. Each alert is emitted once per order; the
 * {@code emitted-alerts} store remembers which ones went out.
 */
final class AlertProcessor implements Processor<String, OrderState, String, OrderAlert> {

    static final String AWAITING_PAYMENT_STORE = "awaiting-payment";
    static final String EMITTED_ALERTS_STORE = "emitted-alerts";

    private final Duration paymentTimeout;
    private final Duration checkInterval;
    private final MeterRegistry meters;

    private ProcessorContext<String, OrderAlert> context;
    private KeyValueStore<String, OrderState> awaitingPayment;
    private KeyValueStore<String, Long> emittedAlerts;

    AlertProcessor(Duration paymentTimeout, Duration checkInterval, MeterRegistry meters) {
        this.paymentTimeout = paymentTimeout;
        this.checkInterval = checkInterval;
        this.meters = meters;
    }

    @Override
    public void init(ProcessorContext<String, OrderAlert> context) {
        this.context = context;
        this.awaitingPayment = context.getStateStore(AWAITING_PAYMENT_STORE);
        this.emittedAlerts = context.getStateStore(EMITTED_ALERTS_STORE);
        context.schedule(checkInterval, PunctuationType.STREAM_TIME, this::checkPaymentDeadlines);
    }

    @Override
    public void process(Record<String, OrderState> record) {
        String orderId = record.key();
        OrderState state = record.value();
        if (orderId == null || state == null) {
            return;
        }

        boolean awaitingPaymentNow = state.getCreatedAt() != null
                && state.getPaidAt() == null
                && state.getCanceledAt() == null
                && !alreadyEmitted(AlertType.PAYMENT_TIMEOUT, orderId);
        if (awaitingPaymentNow) {
            awaitingPayment.put(orderId, state);
        } else {
            awaitingPayment.delete(orderId);
        }

        if (state.getCanceledAt() != null && state.getDeliveredAt() != null) {
            emitOnce(
                    AlertType.DELIVERED_AFTER_CANCELLATION,
                    state,
                    record.timestamp(),
                    "Delivery reported for a canceled order (canceled at %s, delivered at %s)"
                            .formatted(state.getCanceledAt(), state.getDeliveredAt()));
        }
    }

    private void checkPaymentDeadlines(long streamTime) {
        List<KeyValue<String, OrderState>> due = new ArrayList<>();
        try (KeyValueIterator<String, OrderState> it = awaitingPayment.all()) {
            while (it.hasNext()) {
                KeyValue<String, OrderState> entry = it.next();
                Instant deadline = entry.value.getCreatedAt().plus(paymentTimeout);
                if (!deadline.isAfter(Instant.ofEpochMilli(streamTime))) {
                    due.add(entry);
                }
            }
        }
        for (KeyValue<String, OrderState> entry : due) {
            awaitingPayment.delete(entry.key);
            emitOnce(
                    AlertType.PAYMENT_TIMEOUT,
                    entry.value,
                    streamTime,
                    String.format(
                            Locale.ROOT,
                            "No payment %d min after creation (created at %s)",
                            paymentTimeout.toMinutes(),
                            entry.value.getCreatedAt()));
        }
    }

    private void emitOnce(AlertType type, OrderState state, long timestamp, String message) {
        String alertId = alertId(type, state.getOrderId());
        if (emittedAlerts.get(alertId) != null) {
            return;
        }
        emittedAlerts.put(alertId, timestamp);
        OrderAlert alert = OrderAlert.newBuilder()
                .setAlertId(alertId)
                .setOrderId(state.getOrderId())
                .setAlertType(type)
                .setDetectedAt(Instant.ofEpochMilli(timestamp))
                .setMessage(message)
                .setCustomerState(state.getCustomerState())
                .setOrderValue(state.getOrderValue())
                .setCreatedAt(state.getCreatedAt())
                .build();
        context.forward(new Record<>(state.getOrderId(), alert, timestamp));
        meters.counter("order_status_alerts_total", "type", type.name()).increment();
    }

    private boolean alreadyEmitted(AlertType type, String orderId) {
        return emittedAlerts.get(alertId(type, orderId)) != null;
    }

    static String alertId(AlertType type, String orderId) {
        return type.name() + ":" + orderId;
    }
}
