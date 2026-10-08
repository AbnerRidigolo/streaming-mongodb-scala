package br.com.olist.deliverysla

import scala.collection.mutable

import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.funsuite.AnyFunSuite

/** Keeps what would go to MongoDB, by collection and _id (last write wins). */
final class RecordingWriter extends DocumentWriter {
  val docs: mutable.Map[String, mutable.Map[String, Row]] = mutable.Map.empty

  override def upsert(df: DataFrame, collection: String): Unit = {
    val coll = docs.getOrElseUpdate(collection, mutable.Map.empty)
    df.collect().foreach(row => coll(row.getAs[String]("_id")) = row)
  }
}

class TimelineSinkSpec extends AnyFunSuite with SparkSessionFixture {
  import OrderDeliveryStateSpec._

  private def timeline(id: String, state: String, deliveredOffsetDays: Option[Double], canceled: Boolean = false) = {
    val events = Seq(
      DeliveryInput(id, DeliveryInput.OrderSource, "ORDER_CREATED", T0, Some(state), Some(Purchase), Some(Estimated),
        deliveredOffsetDays.map(d => Estimated + (d * Day).toLong))
    ) ++ (if (canceled) Seq(DeliveryInput(id, DeliveryInput.OrderSource, "ORDER_CANCELED", T0 + 1, Some(state))) else Nil)
    events.foldLeft(OrderDeliveryState(id))(_ add _).toTimeline
  }

  test("upserts timelines to Delta and MongoDB and recomputes SLA per state, region and country") {
    import spark.implicits._
    val gold = tempDir("gold").resolve("delivery_timeline").toString
    val writer = new RecordingWriter
    val sink = new TimelineSink(gold, writer)

    sink.write(Seq(
      timeline("o1", "SP", Some(-3)),       // on time
      timeline("o2", "SP", Some(2.5)),      // late by 2.5 days
      timeline("o3", "RJ", Some(0.5)),      // during the estimated day: on time
      timeline("o4", "BA", None),           // not delivered yet
      timeline("o5", "BA", Some(1), canceled = true)
    ).toDS(), 0L)

    // A later batch updates o4 (now late) and replays o1 unchanged.
    val second = Seq(timeline("o4", "BA", Some(4)), timeline("o1", "SP", Some(-3))).toDS()
    sink.write(second, 1L)
    sink.write(second, 1L) // foreachBatch may run a batch again after a failure

    assert(spark.read.format("delta").load(gold).count() == 5)
    val mongoTimeline = writer.docs("delivery_timeline")
    assert(mongoTimeline.keySet == Set("o1", "o2", "o3", "o4", "o5"))
    assert(mongoTimeline("o4").getAs[String]("sla_status") == Sla.Late)

    val sla = writer.docs("delivery_sla")
    def num(id: String, field: String) = sla(id).getAs[Any](field)

    assert(num("state:SP", "delivered_orders") == 2L)
    assert(num("state:SP", "late_orders") == 1L)
    assert(num("state:SP", "on_time_rate") == 0.5)
    assert(num("state:SP", "avg_delay_days_when_late") == 2.5)
    assert(num("state:BA", "late_orders") == 1L)
    assert(num("state:BA", "canceled_orders") == 1L)
    assert(num("region:Sudeste", "delivered_orders") == 3L)
    assert(num("region:Nordeste", "pending_orders") == 0L)
    assert(num("all:BR", "delivered_orders") == 4L)
    assert(num("all:BR", "on_time_rate") == 0.5)
    assert(num("all:BR", "canceled_orders") == 1L)
  }

  test("an empty micro-batch writes nothing") {
    import spark.implicits._
    val gold = tempDir("gold-empty").resolve("delivery_timeline").toString
    val writer = new RecordingWriter
    new TimelineSink(gold, writer).write(spark.emptyDataset[DeliveryTimeline], 0L)
    assert(writer.docs.isEmpty)
    assert(!new java.io.File(gold).exists())
  }
}
