package br.com.olist.deliverysla

import org.scalatest.funsuite.AnyFunSuite

class OrderDeliveryStateSpec extends AnyFunSuite {
  import OrderDeliveryStateSpec._

  test("happy path builds every milestone and the business dates") {
    val s = fold(lifecycle)
    assert(s.customerState.contains("SP"))
    assert(s.created_at.contains(T0))
    assert(s.shipped_at.contains(T0 + 3000)) // PICKED_UP before ORDER_SHIPPED
    assert(s.in_transit_at.contains(T0 + 5000))
    assert(s.delivered_at.contains(T0 + 7000))
    assert(s.last_carrier_status.contains("DELIVERED"))
    assert(s.deliveryStatus == "DELIVERED")
    assert(s.purchase_ts.contains(Purchase))
    assert(s.estimated_delivery_ts.contains(Estimated))
    assert(s.delivered_customer_ts.contains(DeliveredEarly))
  }

  test("any arrival order converges to the same state") {
    val expected = fold(lifecycle)
    lifecycle.permutations.foreach(p => assert(fold(p) == expected, p.map(_.event_type)))
  }

  test("replayed duplicates do not change the state") {
    val once = fold(lifecycle)
    assert(fold(lifecycle ++ lifecycle.reverse) == once)
    assert(once.add(lifecycle.head) == once)
  }

  test("carrier status ties go to the later stage") {
    val s = fold(Seq(carrier("DELIVERED", T0), carrier("IN_TRANSIT", T0)))
    assert(s.last_carrier_status.contains("DELIVERED"))
  }

  test("a canceled order is CANCELED even if the carrier reports DELIVERED") {
    val s = fold(Seq(order("ORDER_CREATED", T0), order("ORDER_CANCELED", T0 + 1), carrier("DELIVERED", T0 + 2)))
    assert(s.deliveryStatus == "CANCELED")
    assert(Sla.classify(s).status == Sla.Canceled)
  }

  test("customer state from order events wins over the carrier's") {
    val s = fold(Seq(carrier("PICKED_UP", T0, state = "RJ"), order("ORDER_CREATED", T0 + 1, state = "SP")))
    assert(s.customerState.contains("SP"))
    assert(fold(Seq(carrier("PICKED_UP", T0, state = "RJ"))).customerState.contains("RJ"))
  }

  test("SLA: on time, late, pending and unknown") {
    def sla(delivered: Option[Long], estimated: Option[Long] = Some(Estimated)) =
      Sla.classify(OrderDeliveryState("o", estimated_delivery_ts = estimated, delivered_customer_ts = delivered))

    val early = sla(Some(DeliveredEarly))
    assert(early.status == Sla.OnTime)
    assert(early.delayDays.contains(-7.11))

    // Any time on the estimated day is still on time (Olist gives a date).
    assert(sla(Some(Estimated + 23 * Hour)).status == Sla.OnTime)
    val late = sla(Some(Estimated + 3 * Day + 12 * Hour))
    assert(late.status == Sla.Late)
    assert(late.delayDays.contains(3.5))

    assert(sla(None) == Sla.Result(Sla.Pending, None))
    assert(sla(Some(DeliveredEarly), estimated = None) == Sla.Result(Sla.Unknown, None))
  }

  test("timeline carries region, SLA and lead times") {
    val t = fold(lifecycle).toTimeline
    assert(t.region.contains("Sudeste"))
    assert(t.sla_status == Sla.OnTime)
    assert(t.promised_days.contains(15.54)) // 15 d 13:03:27 from purchase to the estimated date
    assert(t.actual_days.contains(8.44))    // 8 d 10:28:40 from purchase to delivery
    assert(t.delivered_at.map(_.getTime).contains(T0 + 7000))
  }

  test("regions follow IBGE") {
    assert(Regions.of("sp").contains("Sudeste"))
    assert(Regions.of("AM").contains("Norte"))
    assert(Regions.of("DF").contains("Centro-Oeste"))
    assert(Regions.of("BA").contains("Nordeste"))
    assert(Regions.of("RS").contains("Sul"))
    assert(Regions.of("NA").isEmpty)
  }
}

object OrderDeliveryStateSpec {
  val Hour: Long = 60L * 60 * 1000
  val Day: Long = 24 * Hour
  val T0 = 1791482285000L
  val Purchase = 1506941793000L       // 2017-10-02 10:56:33 UTC
  val Estimated = 1508284800000L      // 2017-10-18 00:00:00 UTC
  val DeliveredEarly = 1507670713000L // 2017-10-10 21:25:13 UTC

  def order(eventType: String, at: Long, state: String = "SP"): DeliveryInput =
    DeliveryInput("order_000000", DeliveryInput.OrderSource, eventType, at, Some(state),
      Some(Purchase), Some(Estimated), Some(DeliveredEarly))

  def carrier(status: String, at: Long, state: String = "SP"): DeliveryInput =
    DeliveryInput("order_000000", DeliveryInput.CarrierSource, status, at, Some(state))

  val lifecycle: Seq[DeliveryInput] = Seq(
    order("ORDER_CREATED", T0),
    order("ORDER_SHIPPED", T0 + 4000),
    carrier("PICKED_UP", T0 + 3000),
    carrier("IN_TRANSIT", T0 + 5000),
    carrier("DELIVERED", T0 + 7000)
  )

  def fold(events: Seq[DeliveryInput]): OrderDeliveryState =
    events.foldLeft(OrderDeliveryState("order_000000"))(_ add _)
}
