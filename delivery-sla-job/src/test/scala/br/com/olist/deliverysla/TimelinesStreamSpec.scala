package br.com.olist.deliverysla

import org.apache.spark.api.java.Optional

import scala.collection.mutable

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{GroupStateTimeout, TestGroupState}
import org.scalatest.funsuite.AnyFunSuite

class TimelinesStreamSpec extends AnyFunSuite with SparkSessionFixture {
  import OrderDeliveryStateSpec.{Estimated, Purchase, T0}

  private def ev(order: String, source: String, kind: String, at: Long, delivered: Option[Long] = None) =
    DeliveryInput(order, source, kind, at, Some("SP"), Some(Purchase), Some(Estimated), delivered)

  private def order(id: String, kind: String, at: Long, delivered: Option[Long] = None) =
    ev(id, DeliveryInput.OrderSource, kind, at, delivered)

  private def carrier(id: String, kind: String, at: Long) =
    ev(id, DeliveryInput.CarrierSource, kind, at).copy(purchase_ts = None, estimated_delivery_ts = None)

  /** Runs the streaming query batch by batch; returns the rows of each batch. */
  private def run(batches: Seq[Seq[DeliveryInput]]): Seq[Seq[DeliveryTimeline]] = {
    implicit val ctx = spark.sqlContext
    import spark.implicits._

    // Three partitions, like a Kafka topic: events of one order land in
    // different partitions and must still meet in one state.
    val input = MemoryStream[DeliveryInput](3)
    val outputs = mutable.ArrayBuffer.empty[Seq[DeliveryTimeline]]
    val collect: (Dataset[DeliveryTimeline], Long) => Unit =
      (batch, _) => outputs.synchronized(outputs += batch.collect().toSeq.sortBy(_.order_id))

    val query = Timelines
      .build(input.toDS(), "1 hour")
      .writeStream
      .outputMode("update")
      .option("checkpointLocation", tempDir("checkpoint").toString)
      .foreachBatch(collect)
      .start()
    try batches.foreach { events =>
      input.addData(events)
      query.processAllAvailable()
    } finally query.stop()
    outputs.toList
  }

  test("state carries milestones across micro-batches; only touched orders are emitted") {
    val out = run(Seq(
      Seq(order("A", "ORDER_CREATED", T0), carrier("A", "PICKED_UP", T0 + 1000), order("B", "ORDER_CREATED", T0)),
      Seq(carrier("A", "DELIVERED", T0 + 5000), order("A", "ORDER_DELIVERED", T0 + 6000, Some(Estimated + 2 * 86400000L)),
        order("B", "ORDER_CANCELED", T0 + 2000)),
      Seq(carrier("A", "IN_TRANSIT", T0 + 3000))
    ))

    assert(out.map(_.map(_.order_id)) == Seq(Seq("A", "B"), Seq("A", "B"), Seq("A")))

    val (a1, a2, a3) = (out(0).head, out(1).head, out(2).head)
    assert(a1.delivery_status == "SHIPPED" && a1.sla_status == Sla.Pending)
    assert(a2.delivery_status == "DELIVERED" && a2.sla_status == Sla.Late)
    assert(a2.delay_days.contains(2.0))
    assert(a2.created_at.map(_.getTime).contains(T0)) // from the first batch's state
    // A late IN_TRANSIT fills its milestone but does not move the status back.
    assert(a3.in_transit_at.map(_.getTime).contains(T0 + 3000))
    assert(a3.delivery_status == "DELIVERED")
    assert(a3.last_carrier_status.contains("DELIVERED"))

    val b2 = out(1)(1)
    assert(b2.delivery_status == "CANCELED" && b2.sla_status == Sla.Canceled)
  }

  test("events of one order spread over partitions give one row with the merged state") {
    val events = Seq("ORDER_CREATED", "ORDER_APPROVED", "ORDER_SHIPPED").zipWithIndex.map { case (k, i) =>
      order("C", k, T0 + i)
    } ++ Seq("PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY").zipWithIndex.map { case (k, i) =>
      carrier("C", k, T0 + 10 + i)
    }
    val Seq(Seq(row)) = run(Seq(events))
    assert(row.delivery_status == "OUT_FOR_DELIVERY")
    assert(row.shipped_at.map(_.getTime).contains(T0 + 2)) // ORDER_SHIPPED before PICKED_UP
    assert(row.region.contains("Sudeste"))
  }

  test("idle orders are evicted from state after the TTL, without output") {
    val stored = OrderDeliveryState("D", created_at = Some(T0))
    val timedOut = TestGroupState.create[OrderDeliveryState](
      Optional.of(stored), GroupStateTimeout.ProcessingTimeTimeout, 0L, Optional.empty[Long](), true)
    assert(Timelines.update("1 hour")("D", Iterator.empty, timedOut).isEmpty)
    assert(!timedOut.exists)

    val active = TestGroupState.create[OrderDeliveryState](
      Optional.of(stored), GroupStateTimeout.ProcessingTimeTimeout, 1000L, Optional.empty[Long](), false)
    val out = Timelines.update("1 hour")("D", Iterator(order("D", "ORDER_SHIPPED", T0 + 1)), active).toList
    assert(out.map(_.delivery_status) == List("SHIPPED"))
    assert(active.getTimeoutTimestampMs.get == 1000L + 60 * 60 * 1000)
  }
}
