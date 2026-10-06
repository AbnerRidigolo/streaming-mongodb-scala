package br.com.olist.orderstatus;

import br.com.olist.events.DeliveryEvent;
import br.com.olist.events.OrderEvent;
import br.com.olist.events.PaymentEvent;
import br.com.olist.orderstatus.avro.OrderAlert;
import br.com.olist.orderstatus.avro.OrderState;
import br.com.olist.orderstatus.avro.OrderStatus;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

/**
 * orders-raw + payments-raw + delivery-events → KTable(order_id → OrderState)
 * → order-status (compacted) and order-alerts.
 */
final class OrderStatusTopology {

    /** Queryable store backing the KTable (interactive queries). */
    static final String ORDER_STATE_STORE = "order-state";

    private OrderStatusTopology() {}

    static Topology build(AppConfig config, AvroSerdes avro, MeterRegistry meters) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<String> keys = Serdes.String();
        Serde<OrderState> stateSerde = avro.value();

        KGroupedStream<String, OrderEvent> orders =
                byOrderId(builder, config.ordersTopic(), "orders", avro.value(), config, meters);
        KGroupedStream<String, PaymentEvent> payments =
                byOrderId(builder, config.paymentsTopic(), "payments", avro.value(), config, meters);
        KGroupedStream<String, DeliveryEvent> deliveries =
                byOrderId(builder, config.deliveryTopic(), "deliveries", avro.value(), config, meters);

        KTable<String, OrderState> orderState = orders
                .cogroup(OrderLifecycle::applyOrder)
                .cogroup(payments, OrderLifecycle::applyPayment)
                .cogroup(deliveries, OrderLifecycle::applyDelivery)
                .aggregate(
                        OrderLifecycle::empty,
                        Named.as("order-lifecycle"),
                        Materialized.<String, OrderState, KeyValueStore<Bytes, byte[]>>as(
                                        ORDER_STATE_STORE)
                                .withKeySerde(keys)
                                .withValueSerde(stateSerde));

        orderState
                .toStream(Named.as("order-state-changes"))
                .mapValues(OrderLifecycle::toStatus, Named.as("to-order-status"))
                .to(config.statusTopic(), Produced.<String, OrderStatus>with(keys, avro.value())
                        .withName("order-status-sink"));

        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(AlertProcessor.AWAITING_PAYMENT_STORE), keys, stateSerde));
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(AlertProcessor.EMITTED_ALERTS_STORE), keys, Serdes.Long()));

        orderState
                .toStream(Named.as("order-state-for-rules"))
                .process(
                        () -> new AlertProcessor(
                                config.paymentTimeout(), config.paymentCheckInterval(), meters),
                        Named.as("alert-rules"),
                        AlertProcessor.AWAITING_PAYMENT_STORE,
                        AlertProcessor.EMITTED_ALERTS_STORE)
                .to(config.alertsTopic(), Produced.<String, OrderAlert>with(keys, avro.value())
                        .withName("order-alerts-sink"));

        return builder.build();
    }

    /**
     * Reads one input topic and re-keys it onto a Streams-owned repartition topic.
     *
     * <p>The three inputs cannot be joined in place: delivery-events has 2 partitions while
     * the others have 3, and the Python producers use librdkafka's default partitioner
     * (CRC32), not the Java client's murmur2. Repartitioning all three with the Streams
     * partitioner co-partitions them and lets interactive queries locate an order_id's
     * partition. The input keys are already order_id, so the key itself is unchanged.
     */
    private static <V extends SpecificRecord> KGroupedStream<String, V> byOrderId(
            StreamsBuilder builder,
            String topic,
            String name,
            Serde<V> valueSerde,
            AppConfig config,
            MeterRegistry meters) {
        Serde<String> keys = Serdes.String();
        return builder.stream(topic, Consumed.with(keys, valueSerde).withName(name + "-source"))
                .filter((orderId, event) -> orderId != null && event != null, Named.as(name + "-valid"))
                .peek((orderId, event) ->
                                meters.counter("order_status_input_events_total", "source", name).increment(),
                        Named.as(name + "-count"))
                .repartition(Repartitioned.<String, V>as(name + "-by-order-id")
                        .withKeySerde(keys)
                        .withValueSerde(valueSerde)
                        .withNumberOfPartitions(config.partitions()))
                .groupByKey(Grouped.with(name + "-grouped", keys, valueSerde));
    }
}
