package br.com.olist.deliverysla

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

/** Delivery timeline per order: arbitrary state with flatMapGroupsWithState. */
object Timelines {

  /** Folds a micro-batch of events into the order's state and emits the
    * updated timeline. Orders with no new event for `stateTtl` (processing
    * time) are evicted: their timeline is already persisted, and a replayed
    * order rebuilds its state from the new events.
    */
  def update(stateTtl: String)(
      orderId: String,
      events: Iterator[DeliveryInput],
      state: GroupState[OrderDeliveryState]
  ): Iterator[DeliveryTimeline] =
    if (state.hasTimedOut) {
      state.remove()
      Iterator.empty
    } else {
      val current = state.getOption.getOrElse(OrderDeliveryState(orderId))
      val updated = events.foldLeft(current)(_ add _)
      state.update(updated)
      state.setTimeoutDuration(stateTtl)
      Iterator.single(updated.toTimeline)
    }

  /** One output row per order touched in each micro-batch (update mode).
    *
    * groupByKey shuffles by a hash of order_id inside Spark, so events of an
    * order meet in the same state partition whatever Kafka partition they
    * came from. The producers partition by librdkafka's CRC32 and the two
    * topics have 3 and 2 partitions: nothing here relies on co-partitioning.
    */
  def build(input: Dataset[DeliveryInput], stateTtl: String): Dataset[DeliveryTimeline] = {
    import input.sparkSession.implicits._
    input
      .groupByKey(_.order_id)
      .flatMapGroupsWithState(OutputMode.Update, GroupStateTimeout.ProcessingTimeTimeout)(
        update(stateTtl) _)
  }
}
