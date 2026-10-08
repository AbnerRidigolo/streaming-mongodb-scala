package br.com.olist.orderstatus;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

/** Confluent Schema Registry serdes (wire format: magic byte + schema id + Avro). */
final class AvroSerdes {

    private final Map<String, Object> config;

    AvroSerdes(String schemaRegistryUrl) {
        this.config = Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                // Registers the service's own schemas (outputs, changelog, repartition).
                AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    }

    <T extends SpecificRecord> Serde<T> value() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(config, false);
        return serde;
    }
}
