package cn.downey.online

import com.mongodb.MongoClientURI
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 *
 */
//连接助手对象, 建立到redis和mongodb的连接
object ConnHelper extends Serializable {
  // 懒变量，使用的时候才初始化
  lazy val jedis = new Jedis("hadoop100", 6379)
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))
}

case class MongoConfig(uri: String, db: String)

// 定义标准推荐对象, productId, score
case class Recommendation(productId: Int, score: Double)

// 定义用户推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])

// 定义商品相似度列表（商品推荐）
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

object OnlineRecommender {

  // 定义常量和表明
  val STREAM_RECS = "StreamRecs"
  val PRODUCT_RECS = "ProductRecs"

  val MAX_USER_RATING_NUM = 20
  val MAX_SIM_PRODUCTS_NUM = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    //创建spark conf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OnlineRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //加载数据, 相似度矩阵, 广播出去
    val simProductsMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", PRODUCT_RECS)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRecs]
      .rdd
      //为了后续查询相似度方便, 把数据转换成Map形式
      .map { item =>
        (item.productId, item.recs.map(x => (x.productId, x.score)).toMap)
      }
      .collectAsMap()

    //定义广播变量
    val simProductsMatrixBC = sc.broadcast(simProductsMatrix)
  }
}
