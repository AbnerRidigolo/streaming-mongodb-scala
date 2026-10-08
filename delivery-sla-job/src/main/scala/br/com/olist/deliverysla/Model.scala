package br.com.olist.deliverysla

import java.sql.Timestamp

/** One input event, from orders-raw or delivery-events, already decoded.
  *
  * Field names follow the Avro schemas (snake_case). Times are epoch millis.
  *
  * @param source        [[DeliveryInput.OrderSource]] or [[DeliveryInput.CarrierSource]]
  * @param event_type    OrderEventType (ORDER_*) or DeliveryStatus (PICKED_UP, ...)
  * @param event_ts      emission time of the event
  * @param purchase_ts   business dates carried by order events (Olist CSV);
  *                      None on carrier events and on events written before
  *                      these optional fields existed
  */
final case class DeliveryInput(
    order_id: String,
    source: String,
    event_type: String,
    event_ts: Long,
    customer_state: Option[String],
    purchase_ts: Option[Long] = None,
    estimated_delivery_ts: Option[Long] = None,
    delivered_customer_ts: Option[Long] = None
)

object DeliveryInput {
  val OrderSource = "order"
  val CarrierSource = "carrier"
}

/** Per-order state kept by flatMapGroupsWithState.
  *
  * Every field merges commutatively and idempotently (earliest milestone,
  * latest carrier status, earliest business date), so out-of-order arrival
  * across the two topics and replayed duplicates converge to the same state.
  */
final case class OrderDeliveryState(
    order_id: String,
    order_customer_state: Option[String] = None,
    carrier_customer_state: Option[String] = None,
    purchase_ts: Option[Long] = None,
    estimated_delivery_ts: Option[Long] = None,
    delivered_customer_ts: Option[Long] = None,
    created_at: Option[Long] = None,
    shipped_at: Option[Long] = None,
    in_transit_at: Option[Long] = None,
    out_for_delivery_at: Option[Long] = None,
    delivered_at: Option[Long] = None,
    canceled_at: Option[Long] = None,
    last_carrier_status: Option[String] = None,
    last_carrier_status_at: Option[Long] = None,
    last_event_at: Option[Long] = None
) {

  /** Folds one event into the state. */
  def add(e: DeliveryInput): OrderDeliveryState = {
    val t = Some(e.event_ts)
    val base = copy(last_event_at = OrderDeliveryState.max(last_event_at, t))
    if (e.source == DeliveryInput.OrderSource) {
      val withDates = base.copy(
        order_customer_state = OrderDeliveryState.minStr(order_customer_state, e.customer_state),
        purchase_ts = OrderDeliveryState.min(purchase_ts, e.purchase_ts),
        estimated_delivery_ts = OrderDeliveryState.min(estimated_delivery_ts, e.estimated_delivery_ts),
        delivered_customer_ts = OrderDeliveryState.min(delivered_customer_ts, e.delivered_customer_ts)
      )
      e.event_type match {
        case "ORDER_CREATED"   => withDates.copy(created_at = OrderDeliveryState.min(created_at, t))
        case "ORDER_SHIPPED"   => withDates.copy(shipped_at = OrderDeliveryState.min(shipped_at, t))
        case "ORDER_DELIVERED" => withDates.copy(delivered_at = OrderDeliveryState.min(delivered_at, t))
        case "ORDER_CANCELED"  => withDates.copy(canceled_at = OrderDeliveryState.min(canceled_at, t))
        case _                 => withDates // ORDER_APPROVED: no delivery milestone
      }
    } else {
      val withStatus = base.copy(
        carrier_customer_state = OrderDeliveryState.minStr(carrier_customer_state, e.customer_state)
      ).withCarrierStatus(e.event_type, e.event_ts)
      e.event_type match {
        case "PICKED_UP"        => withStatus.copy(shipped_at = OrderDeliveryState.min(shipped_at, t))
        case "IN_TRANSIT"       => withStatus.copy(in_transit_at = OrderDeliveryState.min(in_transit_at, t))
        case "OUT_FOR_DELIVERY" => withStatus.copy(out_for_delivery_at = OrderDeliveryState.min(out_for_delivery_at, t))
        case "DELIVERED"        => withStatus.copy(delivered_at = OrderDeliveryState.min(delivered_at, t))
        case _                  => withStatus
      }
    }
  }

  /** Latest carrier status by event time; ties go to the later stage. */
  private def withCarrierStatus(status: String, at: Long): OrderDeliveryState = {
    val newer = last_carrier_status_at.forall(prev =>
      at > prev || (at == prev && OrderDeliveryState.stageRank(status) >
        last_carrier_status.map(OrderDeliveryState.stageRank).getOrElse(-1)))
    if (newer) copy(last_carrier_status = Some(status), last_carrier_status_at = Some(at)) else this
  }

  def customerState: Option[String] = order_customer_state.orElse(carrier_customer_state)

  /** Where the delivery stands, from the emission-time milestones. */
  def deliveryStatus: String =
    if (canceled_at.isDefined) "CANCELED"
    else if (delivered_at.isDefined) "DELIVERED"
    else if (out_for_delivery_at.isDefined) "OUT_FOR_DELIVERY"
    else if (in_transit_at.isDefined) "IN_TRANSIT"
    else if (shipped_at.isDefined) "SHIPPED"
    else if (created_at.isDefined) "CREATED"
    else "UNKNOWN"

  def toTimeline: DeliveryTimeline = {
    val sla = Sla.classify(this)
    DeliveryTimeline(
      order_id = order_id,
      customer_state = customerState,
      region = customerState.flatMap(Regions.of),
      delivery_status = deliveryStatus,
      sla_status = sla.status,
      delay_days = sla.delayDays,
      promised_days = Sla.days(purchase_ts, estimated_delivery_ts),
      actual_days = Sla.days(purchase_ts, delivered_customer_ts),
      purchase_ts = purchase_ts.map(new Timestamp(_)),
      estimated_delivery_ts = estimated_delivery_ts.map(new Timestamp(_)),
      delivered_customer_ts = delivered_customer_ts.map(new Timestamp(_)),
      created_at = created_at.map(new Timestamp(_)),
      shipped_at = shipped_at.map(new Timestamp(_)),
      in_transit_at = in_transit_at.map(new Timestamp(_)),
      out_for_delivery_at = out_for_delivery_at.map(new Timestamp(_)),
      delivered_at = delivered_at.map(new Timestamp(_)),
      canceled_at = canceled_at.map(new Timestamp(_)),
      last_carrier_status = last_carrier_status,
      last_event_at = last_event_at.map(new Timestamp(_))
    )
  }
}

object OrderDeliveryState {
  private val Stages = Seq("PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED")
  private def stageRank(status: String): Int = Stages.indexOf(status)

  private def min(a: Option[Long], b: Option[Long]): Option[Long] =
    (a ++ b).reduceOption((x: Long, y: Long) => math.min(x, y))
  private def max(a: Option[Long], b: Option[Long]): Option[Long] =
    (a ++ b).reduceOption((x: Long, y: Long) => math.max(x, y))
  private def minStr(a: Option[String], b: Option[String]): Option[String] =
    (a ++ b).reduceOption((x, y) => if (x <= y) x else y)
}

/** The delivery timeline of one order: what is written to MongoDB and Delta.
  *
  * Business dates (purchase / estimated / delivered to the customer) come from
  * the Olist data; the milestone timestamps (created_at ... canceled_at) are
  * event emission times from the replay.
  */
final case class DeliveryTimeline(
    order_id: String,
    customer_state: Option[String],
    region: Option[String],
    delivery_status: String,
    sla_status: String,
    delay_days: Option[Double],
    promised_days: Option[Double],
    actual_days: Option[Double],
    purchase_ts: Option[Timestamp],
    estimated_delivery_ts: Option[Timestamp],
    delivered_customer_ts: Option[Timestamp],
    created_at: Option[Timestamp],
    shipped_at: Option[Timestamp],
    in_transit_at: Option[Timestamp],
    out_for_delivery_at: Option[Timestamp],
    delivered_at: Option[Timestamp],
    canceled_at: Option[Timestamp],
    last_carrier_status: Option[String],
    last_event_at: Option[Timestamp]
)

/** Delivery SLA of one state, one region or the whole country. */
final case class SlaSummary(
    _id: String,
    scope: String,
    key: String,
    delivered_orders: Long,
    on_time_orders: Long,
    late_orders: Long,
    on_time_rate: Option[Double],
    avg_delay_days_when_late: Option[Double],
    avg_promised_days: Option[Double],
    avg_actual_days: Option[Double],
    pending_orders: Long,
    canceled_orders: Long
)
