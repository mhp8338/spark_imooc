package streaming.spark

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import sort.ProductClass
import streaming.utils.{ParamsConf, RedisPool}

/**
  * Author: Michael PK   QQ: 1990218038
  * Spark Streaming处理Kafka的数据
  *
  * 统计每天付费的订单数&订单总额
  *
  * 统计每个小时付费的订单数&订单总额
  *
  * 统计每分钟付费的订单数&订单总额
  */
object StreamingV2App {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]")
      .setAppName("StreamingV2App")
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  // 序列化开关设置为kryo
      .registerKryoClasses(Array(classOf[ProductClass]))  // 需要手工注册自己的类，使得该类使用kryo进行序列化

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val stream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](ParamsConf.topic, ParamsConf.kafkaParams)
    )

    stream.foreachRDD(rdd => {

      // flag fee time
      val data = rdd.map(x => JSON.parseObject(x.value()))
        .map(x => {
          val flag = x.getString("flag")
          val fee = x.getLong("fee")
          val time = x.getString("time")

          val day = time.substring(0, 8)
          val hour = time.substring(8, 10)
          val minute = time.substring(10, 12)

          val success:(Long,Long) = if(flag == "1") (1, fee) else (0, 0)

          /**
            * 把上面的数据规整成一个数据结构
            * day, hour, minute: 分别是粒度
            *
            *
            */
          (day, hour, minute, List[Long](1, success._1, success._2))
        })


      // 天
      data.map(x => (x._1, x._4))
        .reduceByKey((a,b) => {
          a.zip(b).map(x => x._1 + x._2)
        }).foreachPartition(partition => {
        val jedis = RedisPool.getJedis()
        partition.foreach(x => {
          jedis.hincrBy("Imooc-"+x._1, "total", x._2(0))
          jedis.hincrBy("Imooc-"+x._1, "success", x._2(1))
          jedis.hincrBy("Imooc-"+x._1, "fee", x._2(2))
        })
      })


      // 小时
      data.map(x => ((x._1,x._2), x._4))
        .reduceByKey((a,b) => {
          a.zip(b).map(x => x._1 + x._2)
        }).foreachPartition(partition => {
        val jedis = RedisPool.getJedis()
        partition.foreach(x => {
          jedis.hincrBy("Imooc-"+x._1._1, "total"+x._1._2, x._2(0))
          jedis.hincrBy("Imooc-"+x._1._1, "success"+x._1._2, x._2(1))
          jedis.hincrBy("Imooc-"+x._1._1, "fee"+x._1._2, x._2(2))
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
