package cn.downey.online

import com.mongodb.MongoClientURI
import com.mongodb.casbah.{MongoClient, MongoClientURI}
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

}
