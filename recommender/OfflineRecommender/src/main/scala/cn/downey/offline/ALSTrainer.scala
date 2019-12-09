package cn.downey.offline

import breeze.numerics.sqrt
import cn.downey.offline.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // 创建spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")

    //创建Spark上下文对象
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("/tmp")
    println("================================" + sc.getCheckpointDir)

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
        rating => Rating(rating.userId, rating.productId, rating.score)
      ).cache()

    // 数据集的切分，切分成训练集和测试集
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD = splits(0)
    val testingRDD = splits(1)

    //核心实现：输出最优参数
    adjustALSParams(trainingRDD, testingRDD)

    spark.stop()

  }


  //TODO
  // iterations迭代次数没有加入参数列表
  // RMSE输出需要改成遍历
  def adjustALSParams(trainData: RDD[Rating], testData: RDD[Rating]) = {
    //遍历数组中定义的参数取值
    val result = for (rank <- Array(5, 10, 20, 50); iterations <- Array(50); lambda <- Array(1, 0.1, 0.01))
      yield {
        val model = ALS.train(trainData, rank, iterations, lambda)
        val rmse = getRMSE(model, testData)
        (rank, iterations, lambda, rmse)
      }

    //按照RMSE排序并输出最优参数
    println(result.minBy(_._4))
  }

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    //构建userProducts，得到预测评分矩阵
    val userProducts = data.map(item => (item.user, item.product))
    val predictRating = model.predict(userProducts)

    //按照公式计算RMSE
    //首先把预测评分和实际评分表按照(userId, productId)做一个连接
    val observed = data.map(item => ((item.user, item.product), item.rating))
    val predict = predictRating.map(item => ((item.user, item.product), item.rating))

    sqrt(
      observed.join(predict).map {
        case ((userId, productId), (actual, pre)) =>
          val err = actual - pre
          err * err
      }.mean()
    )
  }
}
