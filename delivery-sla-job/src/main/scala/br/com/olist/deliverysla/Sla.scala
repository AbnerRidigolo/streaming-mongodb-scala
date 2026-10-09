package br.com.olist.deliverysla

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

/** Delivery SLA against the date promised to the customer (Olist data). */
object Sla {
  val OnTime = "ON_TIME"
  val Late = "LATE"
  val Pending = "PENDING"   // not delivered yet
  val Unknown = "UNKNOWN"   // no estimated date (event written before the field)
  val Canceled = "CANCELED"

  private val DayMs = 24L * 60 * 60 * 1000

  final case class Result(status: String, delayDays: Option[Double])

  /** Classifies one order.
    *
    * Olist's estimated delivery is a date (midnight), so an order is late
    * when it reaches the customer after that day ends. delay_days is
    * delivered minus estimated, in days: negative means early.
    */
  def classify(s: OrderDeliveryState): Result =
    if (s.canceled_at.isDefined) Result(Canceled, None)
    else
      (s.estimated_delivery_ts, s.delivered_customer_ts) match {
        case (None, _) => Result(Unknown, None)
        case (Some(_), None) => Result(Pending, None)
        case (Some(estimated), Some(delivered)) =>
          val status = if (delivered >= estimated + DayMs) Late else OnTime
          Result(status, days(Some(estimated), Some(delivered)))
      }

  /** to - from in days, two decimals. */
  def days(from: Option[Long], to: Option[Long]): Option[Double] =
    for (f <- from; t <- to) yield math.round((t - f).toDouble / DayMs * 100) / 100.0

  /** SLA per state, per region and for the whole country ("all" / "BR"). */
  def summarize(timelines: Dataset[DeliveryTimeline]): Dataset[SlaSummary] = {
    val spark: SparkSession = timelines.sparkSession
    import spark.implicits._

    val rows = timelines.toDF()
    val scoped = rows
      .where($"customer_state".isNotNull)
      .select(lit("state").as("scope"), $"customer_state".as("key"), $"*")
      .unionByName(
        rows.where($"region".isNotNull).select(lit("region").as("scope"), $"region".as("key"), $"*"))
      .unionByName(rows.select(lit("all").as("scope"), lit("BR").as("key"), $"*"))

    def countIf(status: String) = sum(when($"sla_status" === status, 1L).otherwise(0L))
    val delivered = $"sla_status".isin(OnTime, Late)

    scoped
      .groupBy($"scope", $"key")
      .agg(
        countIf(OnTime).as("on_time_orders"),
        countIf(Late).as("late_orders"),
        avg(when($"sla_status" === Late, $"delay_days")).as("avg_delay_days_when_late"),
        avg(when(delivered, $"promised_days")).as("avg_promised_days"),
        avg(when(delivered, $"actual_days")).as("avg_actual_days"),
        countIf(Pending).as("pending_orders"),
        countIf(Canceled).as("canceled_orders")
      )
      .withColumn("delivered_orders", $"on_time_orders" + $"late_orders")
      .withColumn(
        "on_time_rate",
        when($"delivered_orders" > 0, round($"on_time_orders" / $"delivered_orders", 4)))
      .withColumn("avg_delay_days_when_late", round($"avg_delay_days_when_late", 2))
      .withColumn("avg_promised_days", round($"avg_promised_days", 2))
      .withColumn("avg_actual_days", round($"avg_actual_days", 2))
      .withColumn("_id", concat_ws(":", $"scope", $"key"))
      .as[SlaSummary]
  }
}
