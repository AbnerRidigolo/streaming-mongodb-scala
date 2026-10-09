package br.com.olist.deliverysla

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.io.Source

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.scalatest.funsuite.AnyFunSuite

/** Encodes records with the producers' .avsc files (test resources come from
  * producers/schemas) in Confluent wire format, then decodes them.
  */
class AvroDecodingSpec extends AnyFunSuite {
  import OrderDeliveryStateSpec.{DeliveredEarly, Estimated, Purchase, T0}

  private def schema(file: String): Schema =
    new Schema.Parser().parse(Source.fromResource(file).mkString)

  private val orderSchema = schema("order_event.avsc")
  private val deliverySchema = schema("delivery_event.avsc")

  /** order_event.avsc as it was before the optional business dates. */
  private val oldOrderSchema: Schema = {
    val dates = Set("purchase_ts", "estimated_delivery_ts", "delivered_customer_ts")
    val fields = orderSchema.getFields.asScala.filterNot(f => dates(f.name)).map(f => new Schema.Field(f, f.schema))
    Schema.createRecord(orderSchema.getName, orderSchema.getDoc, orderSchema.getNamespace, false, fields.asJava)
  }

  private val registry = Map(1 -> oldOrderSchema, 2 -> orderSchema, 3 -> deliverySchema)

  private def wire(schemaId: Int, record: GenericRecord): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    out.write(0)
    out.write(ByteBuffer.allocate(4).putInt(schemaId).array())
    val encoder = EncoderFactory.get.binaryEncoder(out, null)
    new GenericDatumWriter[GenericRecord](record.getSchema).write(record, encoder)
    encoder.flush()
    out.toByteArray
  }

  private def metadata(s: Schema, source: String): GenericRecord = {
    val m = new GenericData.Record(s.getField("metadata").schema)
    m.put("source", source); m.put("version", "1.0"); m.put("producer_id", "test")
    m
  }

  private def orderRecord(s: Schema): GenericData.Record = {
    val r = new GenericData.Record(s)
    r.put("event_id", "e1")
    r.put("event_type", new GenericData.EnumSymbol(s.getField("event_type").schema, "ORDER_CREATED"))
    r.put("order_id", "order_000000")
    r.put("customer_id", "cust_1")
    r.put("customer_state", "SP")
    r.put("event_timestamp", T0)
    r.put("metadata", metadata(s, "orders-producer"))
    r
  }

  private val decode = EventMapping.decode(new ConfluentAvroDecoder(registry), "orders-raw") _

  test("order event with the business dates") {
    val r = orderRecord(orderSchema)
    r.put("purchase_ts", Purchase)
    r.put("estimated_delivery_ts", Estimated)
    r.put("delivered_customer_ts", DeliveredEarly)
    assert(decode("orders-raw", wire(2, r)).contains(DeliveryInput("order_000000", DeliveryInput.OrderSource,
      "ORDER_CREATED", T0, Some("SP"), Some(Purchase), Some(Estimated), Some(DeliveredEarly))))
  }

  test("order event written before the business dates existed") {
    val decoded = decode("orders-raw", wire(1, orderRecord(oldOrderSchema))).get
    assert(decoded.event_type == "ORDER_CREATED")
    assert(decoded.purchase_ts.isEmpty && decoded.estimated_delivery_ts.isEmpty && decoded.delivered_customer_ts.isEmpty)
  }

  test("order event with null business dates (not delivered yet)") {
    val r = orderRecord(orderSchema)
    r.put("estimated_delivery_ts", Estimated)
    val decoded = decode("orders-raw", wire(2, r)).get
    assert(decoded.estimated_delivery_ts.contains(Estimated) && decoded.delivered_customer_ts.isEmpty)
  }

  test("carrier event") {
    val r = new GenericData.Record(deliverySchema)
    r.put("event_id", "d1")
    r.put("order_id", "order_000000")
    r.put("delivery_status", new GenericData.EnumSymbol(deliverySchema.getField("delivery_status").schema, "PICKED_UP"))
    r.put("customer_state", "RJ")
    r.put("latitude", -22.9)
    r.put("longitude", -43.2)
    r.put("event_timestamp", T0 + 1)
    r.put("metadata", metadata(deliverySchema, "delivery-producer"))
    assert(decode("delivery-events", wire(3, r)).contains(
      DeliveryInput("order_000000", DeliveryInput.CarrierSource, "PICKED_UP", T0 + 1, Some("RJ"))))
  }

  test("records that are not Confluent Avro are skipped, not fatal") {
    assert(decode("orders-raw", "not avro".getBytes("UTF-8")).isEmpty)
    assert(decode("orders-raw", Array[Byte](0, 0, 0, 0, 2, 1)).isEmpty) // truncated body
  }
}
