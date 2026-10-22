package br.com.olist.deliverysla

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.Trigger

/** Kafka (orders-raw + delivery-events) -> delivery timeline per order ->
  * MongoDB (delivery_timeline, delivery_sla) and a Delta gold table.
  *
  * Configured by environment variables; see the README section on this job.
  */
object DeliverySlaJob {

  final case class Config(
      bootstrapServers: String,
      schemaRegistryUrl: String,
      ordersTopic: String,
      deliveryTopic: String,
      mongoUri: String,
      mongoDatabase: String,
      goldTimelinePath: String,
      checkpointLocation: String,
      triggerSeconds: Int,
      maxOffsetsPerTrigger: Long,
      stateTtl: String,
      valueFormat: String,
      kafkaOptions: Map[String, String]
  )

  object Config {
    def fromEnv(env: Map[String, String] = sys.env): Config = {
      def get(name: String, default: String): String = env.getOrElse(name, default)
      Config(
        bootstrapServers = get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        schemaRegistryUrl = get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        ordersTopic = get("TOPIC_ORDERS_RAW", "orders-raw"),
        deliveryTopic = get("TOPIC_DELIVERY", "delivery-events"),
        // Required: the URI carries the spark user's password (from .env).
        mongoUri = env.getOrElse("MONGO_SPARK_URI", sys.error("MONGO_SPARK_URI is not set")),
        mongoDatabase = get("MONGO_DB", "olist_serving"),
        goldTimelinePath = get("GOLD_TIMELINE_PATH", "data/gold/delivery_timeline"),
        checkpointLocation = get("CHECKPOINT_LOCATION", "/tmp/delivery-sla-checkpoints"),
        triggerSeconds = get("TRIGGER_SECONDS", "10").toInt,
        maxOffsetsPerTrigger = get("MAX_OFFSETS_PER_TRIGGER", "10000").toLong,
        stateTtl = get("STATE_TTL", "1 hour"),
        valueFormat = {
          val format = get("VALUE_FORMAT", "avro").toLowerCase
          require(ValueFormats(format), s"VALUE_FORMAT must be one of ${ValueFormats.mkString(", ")}")
          format
        },
        kafkaOptions = kafkaOptionsFrom(env)
      )
    }

    val ValueFormats: Set[String] = Set("avro", "json")

    /** KAFKA_CFG_* variables as Spark Kafka source options:
      * KAFKA_CFG_SECURITY_PROTOCOL -> kafka.security.protocol. Same names as
      * the Python producers use, so one set of variables points both at a
      * managed endpoint such as Azure Event Hubs (SASL_SSL).
      */
    def kafkaOptionsFrom(env: Map[String, String]): Map[String, String] = {
      val prefix = "KAFKA_CFG_"
      env.collect {
        case (name, value) if name.startsWith(prefix) && name.length > prefix.length =>
          "kafka." + name.stripPrefix(prefix).toLowerCase.replace('_', '.') -> value
      }
    }
  }

  /** Both topics, decoded with each record's writer schema (Confluent Avro)
    * or with the bundled .avsc (VALUE_FORMAT=json).
    */
  def readEvents(spark: SparkSession, config: Config): Dataset[DeliveryInput] = {
    import spark.implicits._
    val registryUrl = config.schemaRegistryUrl
    val ordersTopic = config.ordersTopic
    val valueFormat = config.valueFormat
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.bootstrapServers)
      .options(config.kafkaOptions)
      .option("subscribe", s"${config.ordersTopic},${config.deliveryTopic}")
      .option("startingOffsets", "earliest")
      // Topics keep 12-24 h; after a long stop, skip what retention removed.
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", config.maxOffsetsPerTrigger)
      .load()
      .select($"topic", $"value")
      .as[(String, Array[Byte])]
      .mapPartitions { records =>
        val decoder: RecordDecoder =
          if (valueFormat == "json") new AvroJsonDecoder(BundledSchemas.forTopic(ordersTopic))
          else new ConfluentAvroDecoder(SchemaRegistry.fetch(registryUrl))
        val decode = EventMapping.decode(decoder, ordersTopic) _
        records.flatMap { case (topic, value) => decode(topic, value) }
      }
  }

  def main(args: Array[String]): Unit = {
    val config = Config.fromEnv()
    val spark = SparkSession.builder
      .appName("delivery-sla-job")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      // Few, small partitions: one node, a few thousand orders. Fixed for the
      // life of the checkpoint (state store partitions).
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    spark.sparkContext.setLogLevel(sys.env.getOrElse("SPARK_LOG_LEVEL", "WARN"))

    val sink = new TimelineSink(config.goldTimelinePath, new MongoWriter(config.mongoUri, config.mongoDatabase))
    // A typed function value: a lambda here is ambiguous in Scala 2.12
    // (foreachBatch also takes Java's VoidFunction2).
    val writeBatch: (Dataset[DeliveryTimeline], Long) => Unit = sink.write
    val query = Timelines
      .build(readEvents(spark, config), config.stateTtl)
      .writeStream
      .queryName("delivery_timeline")
      .outputMode("update")
      .option("checkpointLocation", config.checkpointLocation)
      .trigger(Trigger.ProcessingTime(s"${config.triggerSeconds} seconds"))
      .foreachBatch(writeBatch)
      .start()

    println(s"delivery-sla-job started (${config.valueFormat}): ${config.ordersTopic} + ${config.deliveryTopic} -> " +
      s"${config.mongoDatabase}.delivery_timeline / delivery_sla, ${config.goldTimelinePath}")
    query.awaitTermination()
  }
}
