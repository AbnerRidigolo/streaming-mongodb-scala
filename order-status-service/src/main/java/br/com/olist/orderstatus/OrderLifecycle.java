package br.com.olist.orderstatus;

import br.com.olist.events.DeliveryEvent;
import br.com.olist.events.DeliveryStatus;
import br.com.olist.events.OrderEvent;
import br.com.olist.events.PaymentEvent;
import br.com.olist.orderstatus.avro.LifecycleStatus;
import br.com.olist.orderstatus.avro.OrderState;
import br.com.olist.orderstatus.avro.OrderStatus;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;

/**
 * Folds order, payment and delivery events into the per-order {@link OrderState}.
 *
 * <p>Every operation is commutative and idempotent: milestones keep the earliest event
 * time, the latest carrier status is chosen by event time, and payments are summed once
 * per distinct payment. The final state therefore does not depend on the order in which
 * the three topics deliver their events, and replaying events (producer retries, or the
 * producers' replay loop, which reuses order ids) leaves milestones and payments as they
 * were; only {@code event_count} and {@code updated_at} keep moving.
 */
final class OrderLifecycle {

    /** Payments kept for de-duplication; Olist orders have at most a few dozen. */
    static final int MAX_TRACKED_PAYMENTS = 64;

    private OrderLifecycle() {}

    /** Initial aggregate; the order id is filled in by the first event. */
    static OrderState empty() {
        return OrderState.newBuilder().setOrderId("").build();
    }

    static OrderState applyOrder(String orderId, OrderEvent event, OrderState state) {
        Instant ts = event.getEventTimestamp();
        OrderState.Builder b = touch(OrderState.newBuilder(state).setOrderId(orderId), ts);

        // Order events are authoritative for the order attributes; they carry the same
        // values on every lifecycle event, so overwriting keeps the fold commutative.
        b.setCustomerId(event.getCustomerId());
        b.setCustomerState(event.getCustomerState());
        if (event.getSellerId() != null) {
            b.setSellerId(event.getSellerId());
        }
        if (event.getProductCategory() != null) {
            b.setProductCategory(event.getProductCategory());
        }
        if (event.getPaymentValue() != null) {
            b.setOrderValue(event.getPaymentValue());
        }

        switch (event.getEventType()) {
            case ORDER_CREATED -> b.setCreatedAt(earliest(b.getCreatedAt(), ts));
            case ORDER_APPROVED -> b.setApprovedAt(earliest(b.getApprovedAt(), ts));
            case ORDER_SHIPPED -> b.setShippedAt(earliest(b.getShippedAt(), ts));
            case ORDER_DELIVERED -> b.setDeliveredAt(earliest(b.getDeliveredAt(), ts));
            case ORDER_CANCELED -> b.setCanceledAt(earliest(b.getCanceledAt(), ts));
        }
        return b.build();
    }

    static OrderState applyPayment(String orderId, PaymentEvent event, OrderState state) {
        Instant ts = event.getEventTimestamp();
        OrderState.Builder b = touch(OrderState.newBuilder(state).setOrderId(orderId), ts);
        b.setPaidAt(earliest(b.getPaidAt(), ts));

        // PaymentEvent has no payment sequence number, and the replay loop re-emits the
        // same payments with new event_ids, so a payment is identified by its content.
        String key = paymentKey(event);
        TreeSet<String> seen = new TreeSet<>(b.getSeenPaymentKeys());
        if (!seen.contains(key) && seen.size() < MAX_TRACKED_PAYMENTS) {
            seen.add(key);
            b.setPaidAmount(round2(b.getPaidAmount() + event.getPaymentValue()));
            b.setPaymentCount(b.getPaymentCount() + 1);
            TreeSet<String> types = new TreeSet<>(b.getPaymentTypes());
            types.add(event.getPaymentType().name());
            b.setPaymentTypes(new ArrayList<>(types));
        }
        b.setSeenPaymentKeys(new ArrayList<>(seen));
        return b.build();
    }

    static OrderState applyDelivery(String orderId, DeliveryEvent event, OrderState state) {
        Instant ts = event.getEventTimestamp();
        OrderState.Builder b = touch(OrderState.newBuilder(state).setOrderId(orderId), ts);
        if (b.getCustomerState() == null) {
            b.setCustomerState(event.getCustomerState());
        }

        switch (event.getDeliveryStatus()) {
            case PICKED_UP -> b.setShippedAt(earliest(b.getShippedAt(), ts));
            case DELIVERED -> b.setDeliveredAt(earliest(b.getDeliveredAt(), ts));
            case IN_TRANSIT, OUT_FOR_DELIVERY -> {
                // Only tracked as the latest carrier status below.
            }
        }

        String status = event.getDeliveryStatus().name();
        Instant lastAt = b.getLastDeliveryStatusAt();
        boolean newer = lastAt == null
                || ts.isAfter(lastAt)
                // Same instant: break the tie by progression so the result is order-free.
                || (ts.equals(lastAt)
                        && event.getDeliveryStatus().ordinal()
                                > DeliveryStatus.valueOf(b.getLastDeliveryStatus()).ordinal());
        if (newer) {
            b.setLastDeliveryStatus(status);
            b.setLastDeliveryStatusAt(ts);
        }
        return b.build();
    }

    /** Current lifecycle status; cancellation is terminal and wins over delivery. */
    static LifecycleStatus status(OrderState s) {
        if (s.getCanceledAt() != null) {
            return LifecycleStatus.CANCELED;
        }
        if (s.getDeliveredAt() != null) {
            return LifecycleStatus.DELIVERED;
        }
        if (s.getShippedAt() != null) {
            return LifecycleStatus.SHIPPED;
        }
        if (s.getPaidAt() != null) {
            return LifecycleStatus.PAID;
        }
        if (s.getCreatedAt() != null) {
            return LifecycleStatus.CREATED;
        }
        return LifecycleStatus.UNKNOWN;
    }

    /** Public view of the state: drops the de-duplication bookkeeping, adds the status. */
    static OrderStatus toStatus(OrderState s) {
        return OrderStatus.newBuilder()
                .setOrderId(s.getOrderId())
                .setStatus(status(s))
                .setCustomerId(s.getCustomerId())
                .setCustomerState(s.getCustomerState())
                .setSellerId(s.getSellerId())
                .setProductCategory(s.getProductCategory())
                .setOrderValue(s.getOrderValue())
                .setPaidAmount(s.getPaidAmount())
                .setPaymentCount(s.getPaymentCount())
                .setPaymentTypes(List.copyOf(s.getPaymentTypes()))
                .setCreatedAt(s.getCreatedAt())
                .setApprovedAt(s.getApprovedAt())
                .setPaidAt(s.getPaidAt())
                .setShippedAt(s.getShippedAt())
                .setDeliveredAt(s.getDeliveredAt())
                .setCanceledAt(s.getCanceledAt())
                .setLastDeliveryStatus(s.getLastDeliveryStatus())
                .setLastDeliveryStatusAt(s.getLastDeliveryStatusAt())
                .setEventCount(s.getEventCount())
                .setUpdatedAt(s.getUpdatedAt())
                .build();
    }

    static String paymentKey(PaymentEvent e) {
        return String.format(
                Locale.ROOT, "%s|%.2f|%d", e.getPaymentType(), e.getPaymentValue(), e.getInstallments());
    }

    private static OrderState.Builder touch(OrderState.Builder b, Instant ts) {
        return b.setEventCount(b.getEventCount() + 1).setUpdatedAt(latest(b.getUpdatedAt(), ts));
    }

    private static Instant earliest(Instant current, Instant candidate) {
        return current == null || candidate.isBefore(current) ? candidate : current;
    }

    private static Instant latest(Instant current, Instant candidate) {
        return current == null || candidate.isAfter(current) ? candidate : current;
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
