package br.com.olist.deliverysla

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

/** Writes `df` to a MongoDB collection, replacing documents by `_id`. */
trait DocumentWriter extends Serializable {
  def upsert(df: DataFrame, collection: String): Unit
}

/** MongoDB Spark Connector (batch writer, used inside foreachBatch). */
final class MongoWriter(uri: String, database: String) extends DocumentWriter {
  override def upsert(df: DataFrame, collection: String): Unit =
    df.write
      .format("mongodb")
      .mode("append")
      .option("connection.uri", uri)
      .option("database", database)
      .option("collection", collection)
      .option("operationType", "replace")
      .option("idFieldList", "_id")
      .option("upsertDocument", "true")
      .save()
}

/** Per micro-batch: timelines to the Delta gold table and MongoDB, then the
  * SLA summary recomputed from the whole gold table.
  *
  * Every write is an upsert keyed by order_id (or scope:key), so replaying a
  * micro-batch after a failure (foreachBatch is at-least-once) gives the same
  * result.
  */
final class TimelineSink(
    goldTimelinePath: String,
    writer: DocumentWriter,
    timelineCollection: String = "delivery_timeline",
    slaCollection: String = "delivery_sla"
) extends Serializable {

  def write(batch: Dataset[DeliveryTimeline], batchId: Long): Unit = {
    val spark = batch.sparkSession
    import spark.implicits._

    // One row per order and batch from flatMapGroupsWithState; dedupe anyway
    // so the MERGE never sees two source rows for one target row.
    val rows = batch.dropDuplicates("order_id").withColumn("updated_at", current_timestamp()).cache()
    try if (!rows.isEmpty) {
      if (DeltaTable.isDeltaTable(spark, goldTimelinePath)) {
        DeltaTable
          .forPath(spark, goldTimelinePath)
          .as("t")
          .merge(rows.as("s"), "t.order_id = s.order_id")
          .whenMatched()
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .execute()
      } else {
        rows.write.format("delta").mode("overwrite").save(goldTimelinePath)
      }

      writer.upsert(rows.withColumn("_id", $"order_id"), timelineCollection)

      val gold = spark.read.format("delta").load(goldTimelinePath).drop("updated_at").as[DeliveryTimeline]
      writer.upsert(Sla.summarize(gold).toDF().withColumn("updated_at", current_timestamp()), slaCollection)
    } finally rows.unpersist()
  }
}
