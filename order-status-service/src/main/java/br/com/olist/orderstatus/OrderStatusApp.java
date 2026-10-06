package br.com.olist.orderstatus;

import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Entry point: builds the topology, starts Kafka Streams and the HTTP API. */
public final class OrderStatusApp {

    private static final Logger LOG = LoggerFactory.getLogger(OrderStatusApp.class);

    private OrderStatusApp() {}

    public static void main(String[] args) throws Exception {
        AppConfig config = AppConfig.fromEnv(System.getenv());
        HostInfo self = new HostInfo(config.advertisedHost(), config.httpPort());

        PrometheusMeterRegistry meters = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        new JvmMemoryMetrics().bindTo(meters);
        new JvmGcMetrics().bindTo(meters);

        Topology topology = OrderStatusTopology.build(
                config, new AvroSerdes(config.schemaRegistryUrl()), meters);
        LOG.info("Topology:\n{}", topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, streamsProperties(config, self));
        new KafkaStreamsMetrics(streams).bindTo(meters);
        streams.setUncaughtExceptionHandler(e -> {
            LOG.error("Unrecoverable stream error, shutting down", e);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        streams.setStateListener((newState, oldState) -> {
            LOG.info("Kafka Streams state {} -> {}", oldState, newState);
            if (newState == KafkaStreams.State.ERROR) {
                // Let the container restart policy bring a fresh instance up.
                Runtime.getRuntime().halt(1);
            }
        });

        HttpApi api = new HttpApi(streams, self, meters);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            api.stop();
            streams.close(Duration.ofSeconds(30));
        }, "shutdown"));

        streams.start();
        api.start();
    }

    static Properties streamsProperties(AppConfig config, HostInfo self) {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, config.applicationId());
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        // Transactions across consumed offsets, state changelogs and output topics.
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        // Lets other instances (and this one) route /orders/{id} to the owning host.
        p.put(StreamsConfig.APPLICATION_SERVER_CONFIG, self.host() + ":" + self.port());
        p.put(StreamsConfig.STATE_DIR_CONFIG, config.stateDir());
        // A record that does not deserialize is logged and skipped (and counted in the
        // dropped-records metric) instead of stopping the whole service.
        p.put(
                StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);
        // Single-broker dev cluster.
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        return p;
    }
}
