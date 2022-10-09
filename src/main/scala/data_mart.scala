
import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row


object data_mart extends App{

  // подключение cassandra
//  %AddDeps com.datastax.spark spark-cassandra-connector_2.11 2.4.3
//  %AddDeps org.elasticsearch elasticsearch-spark-20_2.11 6.8.9
//  %AddDeps org.postgresql postgresql 42.2.12
  val spark = SparkSession.builder().master("local").getOrCreate()
  import spark.implicits._

  spark.setCassandraConf(CassandraConnectorConf.KeepAliveMillisParam.option(10000))
  spark.conf.set("spark.cassandra.connection.host", "10.0.0.31")
  spark.conf.set("spark.cassandra.connection.port", "9042")
  spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
  spark.conf.set("spark.cassandra.input.consistency.level", "ONE")

  val tableOpts = Map("table" -> "clients", "keyspace" -> "labdata")

  val users = spark.read.format("org.apache.spark.sql.cassandra").options(tableOpts).load()



  // подклчючение elasticsearch
  val esOptions =
    Map(
      "es.nodes" -> "10.0.0.31:9200",
      "es.batch.write.refresh" -> "false",
      //"es.read.field.as.array.include" -> "visits",
      "inferSchema" -> "true",
      "es.nodes.wan.only" -> "true"
    )


  val df_shop = spark.read.format("org.elasticsearch.spark.sql").options(esOptions).load("/visits")

  val df_log = spark.read.json("/labs/laba03/weblogs.json")

  val df_web = df_log.withColumn("vis", explode(col("visits")))
    .withColumn("timestamp", col("vis").getItem("timestamp"))
    .withColumn("url", col("vis").getItem("url"))
    .drop(col("visits"))
    .drop(col("vis"));

  val domain_cats = spark
    .sqlContext
    .read
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
    .option("user", "nikolay_sokolov")
    .option("password", "KqGfTqq4")
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "domain_cats")
    .load()

  import java.net.URLDecoder
  // форируем спиок из категорий web:
  val df_url = df_web.withColumn("url",regexp_replace(regexp_replace( col("url"),"%(?![0-9a-fA-F]{2})", "%25"),"\\+", "%2B")).selectExpr("uid", "reflect('java.net.URLDecoder','decode', url, 'utf-8') as  url_web")
  val df_web_url = df_url.withColumn("url_web", regexp_replace(regexp_replace(regexp_replace(regexp_replace(col("url_web"), "http://", ""),"https://",""),"/(.*)",""),"^www.", ""))
  val df_cat_web = df_web_url.join(domain_cats, col("url_web") === col("domain"), "left").drop("url_web")


  //формируем 3 датафрейма для джойна:
  users.createOrReplaceTempView("users")

  val df_1 =
    spark.sql("""
select
uid
,case
  when age BETWEEN 18 and 24 then "18-24"
  when age BETWEEN 25 and 34 then "25-34"
  when age BETWEEN 35 and 44 then "35-44"
  when age BETWEEN 45 and 54 then "45-54"
  when age >= 55 then ">=55"
end as age_cat
, gender
from users
""")

  // категория магазинов и uid

  val df_shop_1 = df_shop.select("uid", "category").where("uid is not null")
  val df_shop_2 = df_shop_1.withColumn("category", regexp_replace(regexp_replace(col("category"), " ", "_"),"-","_"))

  val df_2_shop = df_shop_2
    .map(row =>
    { val uid =  row.getString(0)
      val category = ("shop_" + row.getString(1).toLowerCase())
      (uid, category)
    }
    ).toDF("uid", "category")


  // категория web и uid (category, uid)

//  val df_url = df_web.withColumn("url",regexp_replace(regexp_replace( col("url"),"%(?![0-9a-fA-F]{2})", "%25"),"\\+", "%2B")).selectExpr("uid", "reflect('java.net.URLDecoder','decode', url, 'utf-8') as  url_web")
//  val df_web_url = df_url.withColumn("url_web", regexp_replace(regexp_replace(regexp_replace(regexp_replace(col("url_web"), "http://", ""),"https://",""),"/(.*)",""),"^www.", ""))
//  val df_cat_web = df_web_url.join(domain_cats, col("url_web") === col("domain"), "left").drop("url_web")

  val df_3_web = df_cat_web
    .where("category is not null")
    .drop("domain")
    .drop("url_web")
    .map(row =>
    { val uid =  row.getString(0)
      val category = ("web_" + row.getString(1).toLowerCase())
      (uid, category)
    }
    )
    .toDF("uid", "category")
  df_1.createOrReplaceTempView("df_1")
  df_2_shop.createOrReplaceTempView("df_2_shop")
  df_3_web.createOrReplaceTempView("df_3_web")

  // делаем общий датафрейм, который потом надо будет pivot разворачивать

  // df_2_shop.count() // 88 066
  // df_3_web.count() // 2 097 074

  val df_union_category = spark.sql("""
select uid as u, category from df_2_shop union all
select uid as u, category
from df_3_web""")

//  df_union_category.count() // 2 185 140

  val df_result = df_1.join(df_union_category, df_1("uid") === df_union_category("u"), "left")
    //.where("category is not null")
    .drop("u")


  val df_result_2 =
    df_result.select("uid", "age_cat","category", "gender").groupBy("uid", "age_cat","category", "gender").count().withColumnRenamed("count", "count")
  //.show()


  val df =
    df_result_2
      .groupBy("uid","age_cat","gender").pivot("category").sum("count")
      .drop("null")

  df.printSchema()

  // пишем в постгрес:

  df.write
    .mode("append")
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.0.31:5432/nikolay_sokolov")
    .option("dbtable", "clients")
    .option("user", "nikolay_sokolov")
    .option("password", "KqGfTqq4")
    .option("driver", "org.postgresql.Driver")
    .save()
}
