package cn.downey.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

case class ProductRating(userId: Int, productId: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

// 定义标准推荐对象, productId, score
case class Recommendation(productId: Int, score: Double)

// 定义用户推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])

// 定义商品相似度列表（商品推荐）
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

/**
 * 1.	userId和productId做笛卡尔积，产生（userId，productId）的元组
 * 2.	通过模型预测（userId，productId）对应的评分。
 * 3.	将预测结果通过预测分值进行排序。
 * 4.	返回分值最大的K个商品，作为当前用户的推荐列表。
 */
object OfflineRecommender {

  // 定义常量
  val MONGODB_RATING_COLLECTION = "Rating"

  // 推荐表的名称
  val USER_RECS = "UserRecs"
  val PRODUCT_RECS = "ProductRecs"

  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // 创建spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")

    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载数据
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(
        rating => (rating.userId, rating.productId, rating.score)
      ).cache()

    // 提取出所有用户和商品的数据集
    var userRDD = ratingRDD.map(_._1).distinct()
    var productRDD = ratingRDD.map(_._2).distinct()

    //TODO:核心计算过程

    // 1.训练隐语义模型
    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))
    // TODO 定义模型训练的参数，rank隐特征个数，iterations迭代次数，lambda正则化系数
    val (rank, iterations, lambda) = (5, 10, 0.01)
    val model = ALS.train(trainData, rank, iterations, lambda)

    // 2.获得预测评分矩阵，得到用户的推荐列表
    // 用userRDD和productRDD做笛卡尔积，得到空的UserProductRDD
    var userProducts = userRDD.cartesian(productRDD)
    val preRating = model.predict(userProducts)

    // 从预测评分矩阵中提取得到用户推荐列表
    val userRecs = preRating.filter(_.rating > 0)
      .map(
        rating => (rating.user, (rating.product, rating.rating))
      )
      .groupByKey()
      .map {
        case (userId, recs) =>
          UserRecs(userId, recs.toList.sortWith(_._2 > _._2)
            .take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2))
          )
      }
      .toDF()
    storeDFInMongoDB(userRecs, USER_RECS)

    // 3.利用商品的特征向量，计算商品的相似度列表
    val productFeatures = model.productFeatures.map {
      case (productId, features) => (productId, new DoubleMatrix(features))
    }
    //两两配对商品，计算余弦相似度
    val productRecs = productFeatures.cartesian(productFeatures)
      .filter {
        case (a, b) => a._1 != b._1
      }
      //计算余弦相似度
      .map {
        case (a, b) =>
          val simScore = consineSim(a._2, b._2)
          (a._1, (b._1, simScore))
      }
      //TODO 相似度＞0.4
      .filter(_._2._2 > 0.4)
      .groupByKey()
      .map {
        case (productId, recs) =>
          ProductRecs(productId, recs.toList.sortWith(_._2 > _._2)
            .map(x => Recommendation(x._1, x._2))
          )
      }
      .toDF()
    storeDFInMongoDB(productRecs, PRODUCT_RECS)

    spark.stop()
  }

  def consineSim(product1: DoubleMatrix, product2: DoubleMatrix): Double = {
    product1.dot(product2) / (product1.norm2() * product2.norm2())
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
