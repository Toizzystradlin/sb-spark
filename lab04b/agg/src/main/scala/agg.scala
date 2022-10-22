import org.apache._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object agg extends App{

  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

  val sdf: DataFrame = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "10.0.0.31:6667")
    .option("subscribe", "nikolay_sokolov")
    .load


  val schema: StructType = StructType(
    StructField("event_type", StringType) ::
      StructField("category", StringType) ::
      StructField("item_id", StringType) ::
      StructField("item_price", LongType) ::
      StructField("uid", StringType) ::
      StructField("timestamp", LongType) :: Nil
  )

  val agg = sdf
    .select(
      from_json(col("value").cast(StringType), schema).as("json")
    )
    .select(
      col("json.category").as("category"),
      col("json.event_type").as("event_type"),
      col("json.item_price").as("item_price"),
      col("json.uid").as("uid"),
      to_timestamp(from_unixtime(col("json.timestamp").cast(LongType) / 1000)).as("timestamp")
    )
    .groupBy(window($"timestamp", "1 hour","1 hour"))
    .agg(unix_timestamp(min(col("timestamp"))).as("start_ts"),
      (unix_timestamp(min(col("timestamp"))) + 60 * 60).as("end_ts"),
      sum(when(col("event_type") === "buy", col("item_price").cast(LongType)).otherwise(0L)).as("revenue"),
      count(when(col("uid").isNotNull, "*").otherwise(null)).as("visitors"), //
      count(when(col("event_type") === "buy", "*").otherwise(null)).as("purchases")
    )
    .select(
      col("start_ts"),
      col("end_ts"),
      col("revenue"),
      col("purchases"),
      col("visitors"),
      (col("revenue")/col("purchases")).as("aov")
    )

  agg.select(to_json(struct("*")) as "value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "10.0.0.31:6667")
    .option("topic", "nikolay_sokolov_lab04b_out")
    .option("checkpointLocation", s"log_04b/")
    .outputMode(OutputMode.Update())
    .trigger(Trigger.ProcessingTime(10000))
    .start()
    .awaitTermination()
}
