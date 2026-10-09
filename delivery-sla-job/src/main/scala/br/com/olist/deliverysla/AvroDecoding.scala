package br.com.olist.deliverysla

import java.net.{HttpURLConnection, URL}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.io.Source
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory

/** Decodes Confluent wire-format Avro (magic byte 0, 4-byte schema id, body)
  * with the writer's own schema, fetched by id from Schema Registry.
  *
  * Reading with the writer schema means records written before and after
  * the optional business-date fields decode alike: a missing field is None.
  * (Spark's from_avro takes one fixed schema instead.) Not thread-safe: one
  * decoder per partition.
  */
final class ConfluentAvroDecoder(schemaById: Int => Schema) {
  private val readers = mutable.Map.empty[Int, GenericDatumReader[GenericRecord]]

  def decode(bytes: Array[Byte]): GenericRecord = {
    require(bytes != null && bytes.length > 5 && bytes(0) == 0,
      "not Confluent wire-format Avro (magic byte 0 + schema id)")
    val schemaId = ByteBuffer.wrap(bytes, 1, 4).getInt
    val reader = readers.getOrElseUpdate(schemaId, new GenericDatumReader[GenericRecord](schemaById(schemaId)))
    reader.read(null, DecoderFactory.get.binaryDecoder(bytes, 5, bytes.length - 5, null))
  }
}

object SchemaRegistry {
  private val Json = new ObjectMapper()

  /** GET /schemas/ids/{id}; schemas never change for an id, callers cache. */
  def fetch(registryUrl: String)(id: Int): Schema = {
    val conn = new URL(s"${registryUrl.stripSuffix("/")}/schemas/ids/$id")
      .openConnection().asInstanceOf[HttpURLConnection]
    conn.setConnectTimeout(10000)
    conn.setReadTimeout(10000)
    try {
      val body = Source.fromInputStream(conn.getInputStream, StandardCharsets.UTF_8.name).mkString
      new Schema.Parser().parse(Json.readTree(body).get("schema").asText())
    } finally conn.disconnect()
  }
}

/** Maps decoded records of the two input topics to [[DeliveryInput]]. */
object EventMapping {

  def fromOrder(r: GenericRecord): DeliveryInput =
    DeliveryInput(
      order_id = str(r, "order_id").get,
      source = DeliveryInput.OrderSource,
      event_type = str(r, "event_type").get,
      event_ts = long(r, "event_timestamp").get,
      customer_state = str(r, "customer_state"),
      purchase_ts = long(r, "purchase_ts"),
      estimated_delivery_ts = long(r, "estimated_delivery_ts"),
      delivered_customer_ts = long(r, "delivered_customer_ts")
    )

  def fromDelivery(r: GenericRecord): DeliveryInput =
    DeliveryInput(
      order_id = str(r, "order_id").get,
      source = DeliveryInput.CarrierSource,
      event_type = str(r, "delivery_status").get,
      event_ts = long(r, "event_timestamp").get,
      customer_state = str(r, "customer_state")
    )

  /** Decodes one Kafka record; None (and a log line) when it cannot be read,
    * so one corrupt record does not stop the query.
    */
  def decode(decoder: ConfluentAvroDecoder, ordersTopic: String)(
      topic: String,
      value: Array[Byte]
  ): Option[DeliveryInput] =
    try {
      val record = decoder.decode(value)
      Some(if (topic == ordersTopic) fromOrder(record) else fromDelivery(record))
    } catch {
      case NonFatal(e) =>
        System.err.println(s"delivery-sla-job: skipping undecodable record from $topic: $e")
        None
    }

  // Absent from the writer schema, or null: None. Avro strings and enum
  // symbols both print as their value; timestamp-millis is a plain Long here.
  private def field(r: GenericRecord, name: String): Option[AnyRef] =
    if (r.getSchema.getField(name) == null) None else Option(r.get(name))

  private def str(r: GenericRecord, name: String): Option[String] = field(r, name).map(_.toString)

  private def long(r: GenericRecord, name: String): Option[Long] =
    field(r, name).map(_.asInstanceOf[java.lang.Long].longValue)
}
