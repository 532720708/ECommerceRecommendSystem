package cn.downey.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

object StatisticsRecommender {
  //MongoDB中存取的表名
  val MONGODB_RATING_COLLECTION = "Rating"

  //统计的表的名称
  val RATE_MORE_PRODUCTS = "RateMoreProducts" //评分更多的商品
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts" //最近评分更多的商品
  val AVERAGE_PRODUCTS = "AverageProducts" //平均评分统计


  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //创建spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")

    //创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //加载数据
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    //创建一张叫ratings的临时表
    ratingDF.createOrReplaceTempView("ratings")

    // TODO:用spark sql去做不同的统计推荐

    // 1.历史热门商品，按照评分个数统计, productId, count
    val rateMoreProductsDF = spark
      .sql("select productId, count(productId) as count from ratings group by productId order by count desc")
    storeDFInMongoDB(rateMoreProductsDF, RATE_MORE_PRODUCTS)

    // 2.近期热门商品，把时间戳转换成yyyyMM格式进行评分个数统计,最终得到productId，count，yearmonth
    // 创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // 注册UDF，将timestamp转换为年月格式yyyyMM
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

    //把原始rating数据转换成productId,score,yearmonth
    val ratingOfYearMonthDF = spark
      .sql("select productId,score, changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")
    val rateMoreRecentlyProductsDF = spark
      .sql("select productId,count(productId) as count, yearmonth from ratingOfMonth " +
        "group by yearmonth, productId order by yearmonth desc,count desc")
    //把df保存到mongodb
    storeDFInMongoDB(rateMoreProductsDF, RATE_MORE_RECENTLY_PRODUCTS)

    // 3.优质商品统计，商品的平均评分,productId,avgscore
    val averageProductsDF = spark
      .sql("select productId, avg(score) as avg from ratings group by productId order by avg desc")
    storeDFInMongoDB(averageProductsDF, AVERAGE_PRODUCTS)

    spark.stop()
  }

  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

}
