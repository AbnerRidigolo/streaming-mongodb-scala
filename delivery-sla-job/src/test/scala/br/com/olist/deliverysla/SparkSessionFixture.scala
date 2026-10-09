package br.com.olist.deliverysla

import java.nio.file.{Files, Path}

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** One local SparkSession (with Delta) per suite; no Kafka, no MongoDB. */
trait SparkSessionFixture extends BeforeAndAfterAll { self: Suite =>

  lazy val spark: SparkSession = SparkSession.builder
    .master("local[2]")
    .appName(getClass.getSimpleName)
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.ui.enabled", "false")
    // With processing-time timeouts every trigger runs a no-data batch, so
    // processAllAvailable() would never see the stream idle. Production keeps
    // them on (that is how timeouts fire); the TTL is tested with TestGroupState.
    .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
    .config("spark.sql.streaming.metricsEnabled", "false")
    .getOrCreate()

  def tempDir(prefix: String): Path = Files.createTempDirectory(prefix)

  override def afterAll(): Unit = {
    try spark.stop()
    finally super.afterAll()
  }
}
