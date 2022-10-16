import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object filter extends App{
  val spark = SparkSession.builder().master("local").getOrCreate()
  spark.conf.set("spark.sql.session.timeZone", "UTC")
  import spark.implicits._

  val offset: String = spark.sparkContext.getConf.get("offset")
  val topicName: String = spark.sparkContext.getConf.get("topic_name")
  val outputDirPrefix: String = spark.sparkContext.getConf.get("output_dir_prefix")

  val finalOffset = if (offset.contains("earliest"))
    offset
  else {
    "{\"" + topicName + "\":{\"0\":" + offset + "}}"
  }

  val kafkaParams = Map(
    "kafka.bootstrap.servers" -> "spark-master-1.newprolab.com:6667",
    "subscribe" -> topicName,
    "startingOffsets" -> finalOffset
  )

  val df = spark.read.format("kafka").options(kafkaParams).load

  val jsonString = df.select('value.cast("string")).as[String]

  val parsed = spark.read.json(jsonString)

  val viewDf = parsed.filter($"event_type" === "view")
    .withColumn("date_interim", to_date(from_unixtime($"timestamp" / 1000)))
    .withColumn("date", date_format($"date_interim", "yyyyMMdd"))
    .withColumn("p_date", concat(lit("p"), date_format($"date_interim", "yyyyMMdd")))
    .drop($"date_interim")

  val buyDf = parsed.filter($"event_type" === "buy")
    .withColumn("date_interim", to_date(from_unixtime($"timestamp" / 1000)))
    .withColumn("date", date_format($"date_interim", "yyyyMMdd"))
    .withColumn("p_date", concat(lit("p"), date_format($"date_interim", "yyyyMMdd")))
    .drop($"date_interim")

  buyDf
    .write
    .mode("overwrite")
    .partitionBy("p_date")
    .json(s"$outputDirPrefix/buy")

  viewDf
    .write
    .mode("overwrite")
    .partitionBy("p_date")
    .json(s"$outputDirPrefix/view")
}