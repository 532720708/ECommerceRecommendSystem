package cn.downey.online

import com.mongodb.MongoClientURI
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
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

  // 定义常量和表名
  val MONGODB_RATING_COLLECTION = "Rating"
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

    //创建kafka配置参数
    val kafkaPara = Map(
      "bootstrap.servers" -> "hadoop100:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    //创建一个DStream
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara)
    )

    //对kafkaStream进行处理, 产生评分流, userId|productId|score|timestamp
    val ratingStream = kafkaStream.map { msg =>
      var attr = msg.value().split("\\|")
      (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }

    //核心算法部分，定义评分流的处理流程
    ratingStream.foreachRDD(
      rdds => rdds.foreach {
        case (userId, productId, score, timestamp) =>
          println("Rating data coming>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

          //TODO：核心算法流程
          //1.从redis里取出当前用户的最近评分, 保存成一个数组Array[(productId, score)]
          var userRecentlyRatings = getUserRecentlyRatings(MAX_USER_RATING_NUM, userId, ConnHelper.jedis)

          //2.从相似度矩阵中获取当前商品最相似的商品列表, 作为备选列表, 保存成一个数组Array[productId]
          val candidateProducts = getTopSimProducts(MAX_SIM_PRODUCTS_NUM, productId, userId, simProductsMatrixBC.value)

          //3.计算每个备选商品的推荐优先级，得到当前用户的实时推荐列表, 保存成一个数组Array[productId, score]
          val streamRecs = computeProductScore(candidateProducts, userRecentlyRatings, simProductsMatrixBC.value)


          //4.把推荐列表保存到mongodb
          saveDataInMongoDB(userId, streamRecs)
      }
    )

    //启动streaming
    ssc.start()
    println("Streaming started>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    ssc.awaitTermination()
  }

  /**
   * 从redis里获取最近num次评分
   */

  import scala.collection.JavaConversions._

  def getUserRecentlyRatings(num: Int, userId: Int, jedis: Jedis): Array[(Int, Double)] = {
    //从redis中用户的评分队列里获取评分数据, List键名为uid:USERID, 值为:PRODUCTID:SCORE
    jedis.lrange("userId:" + userId.toString, 0, num)
      .map { item =>
        val attr = item.split("\\:")
        (attr(0).trim.toInt, attr(1).trim.toDouble)
      }
      .toArray
  }

  /**
   * 获取当前商品的相似列表，并过滤掉用户已经评分过的，作为备选列表
   *
   * @param num
   * @param productId
   * @param userId
   * @param simProducts
   * @param mongoConfig
   * @return
   */
  def getTopSimProducts(num: Int,
                        productId: Int,
                        userId: Int,
                        simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                       (implicit mongoConfig: MongoConfig): Array[Int] = {
    //从广播变量相似度矩阵中拿到当前商品的相似度列表
    val allSimProducts = simProducts(productId).toArray

    //获得用户已经评分过的商品，过滤掉，排序输出
    val ratingCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
    val ratignExist = ratingCollection.find(MongoDBObject("userId" -> userId))
      .toArray
      .map { item => //只需要productId
        item.get("productId").toString.toInt
      }

    //从所有的相似商品中进行过滤, 如果productId不在exist里面
    allSimProducts.filter(x => !ratignExist.contains(x._1))
      .sortWith(_._2 > _._2)
      .take(num)
      .map(x => x._1)
  }

  def computeProductScore(candidateProducts: Unit, userRecentlyRatings: Unit, value: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]) = {

  }

  def saveDataInMongoDB(userId: Int, streamRecs: Unit): Unit = {

  }
}
