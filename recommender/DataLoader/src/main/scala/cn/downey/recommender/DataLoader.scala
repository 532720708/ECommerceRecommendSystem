package cn.downey.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Products数据集
 * 商品ID：         3982
 * 商品名称：       Fuhlen 富勒 M8眩光舞者时尚节能无线鼠标(草绿)(眩光.悦动.时尚炫舞鼠标 12个月免换电池 高精度光学寻迹引擎 超细微接收器10米传输距离)
 * 商品分类ID：     1057,439,736
 * 亚马逊ID：       B009EJN4T2
 * 商品图片的URL：  https://images-cn-4.ssl-images-amazon.com/images/I/31QPvUDNavL._SY300_QL70_.jpg
 * 商品分类：       外设产品|鼠标|电脑/办公
 * 商品UGC标签:     富勒|鼠标|电子产品|好用|外观漂亮
 */

case class Product(productId: Int, name: String, imageUrl: String, categories: String, tags: String)

/**
 * Ratings数据集
 * 用户ID：      4867
 * 商品ID：      457976
 * 评分数据：    5.0
 * 时间戳:       1395676800
 */

case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

/**
 * MongoDB连接配置
 *
 * @param uri
 * @param db
 */
case class MongoConfig(uri: String, db: String)

object DataLoader {

  //数据文件路径
  val PRODUCT_DATA_PATH = "G:\\bigdata\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
  val RATING_DATA_PATH = "G:\\bigdata\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"

  //MongoDB中存取的表名
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    //创建spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")

    //创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    //加载数据
    val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    val productDF = productRDD.map(item => {
      val attr = item.split("\\^")
      Product(attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim)
    }).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()


    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    storeDataInMongoDB(productDF, ratingDF)

    //    spark.stop()
  }

  def storeDataInMongoDB(productDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    //新建MongoDB连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    //定义操作表, db.product
    val productCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
    val ratingCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)

    //若表已经存在，则删除
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    //将当前数据存入对应的表中
    productDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对表创建索引
    productCollection.createIndex(MongoDBObject("productId" -> -1))
    ratingCollection.createIndex(MongoDBObject("productId" -> -1))
    ratingCollection.createIndex(MongoDBObject("userId" -> -1))

    mongoClient.close()

  }


}
