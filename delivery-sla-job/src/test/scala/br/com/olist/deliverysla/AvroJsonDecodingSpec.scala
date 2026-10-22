package br.com.olist.deliverysla

import java.nio.charset.StandardCharsets

import org.scalatest.funsuite.AnyFunSuite

/** VALUE_FORMAT=json: Avro's JSON encoding read with the .avsc bundled in the
  * JAR. The payloads below were produced by the Python producers' encoder
  * (base_producer.encode_avro_json), so this checks the contract across the
  * two languages, not just Avro against itself.
  */
class AvroJsonDecodingSpec extends AnyFunSuite {

  private val orderJson =
    """{"event_id": "e1", "event_type": "ORDER_CREATED", "order_id": "order_000000", "customer_id": "cust_1", """ +
      """"seller_id": {"string": "seller_1"}, "payment_value": {"double": 120.5}, "product_category": {"string": "esporte_lazer"}, """ +
      """"customer_state": "SP", "event_timestamp": 1700000000000, "metadata": {"source": "orders-producer", "version": "1.0", "producer_id": "pytest"}, """ +
      """"purchase_ts": {"long": 1699900000000}, "estimated_delivery_ts": {"long": 1701000000000}, "delivered_customer_ts": null}"""

  private val deliveryJson =
    """{"event_id": "e2", "order_id": "order_000000", "delivery_status": "DELIVERED", "customer_state": "SP", """ +
      """"latitude": -23.5, "longitude": -46.6, "event_timestamp": 1700000600000, """ +
      """"metadata": {"source": "delivery-producer", "version": "1.0", "producer_id": "pytest"}}"""

  private def bytes(s: String): Array[Byte] = s.getBytes(StandardCharsets.UTF_8)

  private val decode =
    EventMapping.decode(new AvroJsonDecoder(BundledSchemas.forTopic("orders-raw")), "orders-raw") _

  test("order event from the Python producer, with business dates") {
    assert(decode("orders-raw", bytes(orderJson)).contains(DeliveryInput("order_000000", DeliveryInput.OrderSource,
      "ORDER_CREATED", 1700000000000L, Some("SP"), Some(1699900000000L), Some(1701000000000L), None)))
  }

  test("carrier event from the Python producer") {
    assert(decode("delivery-events", bytes(deliveryJson)).contains(DeliveryInput("order_000000",
      DeliveryInput.CarrierSource, "DELIVERED", 1700000600000L, Some("SP"))))
  }

  test("a document outside the .avsc contract is skipped, not fatal") {
    val missingRequired = orderJson.replace(""""customer_state": "SP", """, "")
    assert(decode("orders-raw", bytes(missingRequired)).isEmpty)
    assert(decode("orders-raw", bytes("not json")).isEmpty)
  }

  test("KAFKA_CFG_* variables become kafka.* source options") {
    val options = DeliverySlaJob.Config.kafkaOptionsFrom(Map(
      "KAFKA_CFG_SECURITY_PROTOCOL" -> "SASL_SSL",
      "KAFKA_CFG_SASL_MECHANISM" -> "PLAIN",
      "KAFKA_CFG_REQUEST_TIMEOUT_MS" -> "60000",
      "KAFKA_BOOTSTRAP_SERVERS" -> "ignored:9092",
      "KAFKA_CFG_" -> "ignored"
    ))
    assert(options == Map(
      "kafka.security.protocol" -> "SASL_SSL",
      "kafka.sasl.mechanism" -> "PLAIN",
      "kafka.request.timeout.ms" -> "60000"
    ))
  }

  test("VALUE_FORMAT defaults to avro and rejects unknown formats") {
    val base = Map("MONGO_SPARK_URI" -> "mongodb://unused")
    assert(DeliverySlaJob.Config.fromEnv(base).valueFormat == "avro")
    assert(DeliverySlaJob.Config.fromEnv(base + ("VALUE_FORMAT" -> "JSON")).valueFormat == "json")
    intercept[IllegalArgumentException](DeliverySlaJob.Config.fromEnv(base + ("VALUE_FORMAT" -> "protobuf")))
  }
}
