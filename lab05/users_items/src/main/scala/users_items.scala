import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object users_items extends App {
  val spark = SparkSession.builder().master("local").getOrCreate()
  spark.conf.set("spark.sql.session.timeZone", "UTC")

  import spark.implicits._
  val configurations = spark.conf.getAll
  configurations.foreach(println(_))
  val inputDir: String = spark.conf.get("spark.users_items.input_dir","")
  //"/user/ivan.povkh/visits"
  val outputDir: String = spark.conf.get("spark.users_items.output_dir", "")
  //"/user/ivan.povkh/users-items"
  val update: String = spark.conf.get("spark.users_items.update","0")


  def getFilteredDf(df: DataFrame, dfType: String): DataFrame = {
    val filteredDf = df.filter($"uid".isNotNull).withColumn("item_id", concat(lit(dfType), lower($"item_id")))
      .withColumn("item_id", regexp_replace($"item_id", "-", "_"))

    filteredDf
  }
  val buyDf = spark.read.json(s"$inputDir/buy/*/*.json")

  val viewDf = spark.read.json(s"$inputDir/view/*/*.json")

  val finalBuyDf = getFilteredDf(buyDf,"buy_")

  val finalViewDf= getFilteredDf(viewDf, "view_")

  val userItems = finalBuyDf
    .union(finalViewDf)

  val partitionDate = userItems.select(max("date").cast("string")).map(_.getString(0)).first()

  val userItemsFinal = userItems
    .withColumn("cnt", lit(1))
    .groupBy('uid)
    .pivot('item_id)
    .agg(sum('cnt))
    .na.fill(0)

  update match {
    case "0" => userItemsFinal.write.mode("overwrite").parquet(s"$outputDir"+"/"+partitionDate)
    case _ => userItemsFinal.write.mode("append").parquet(s"$outputDir"+"/"+partitionDate)
  }
}