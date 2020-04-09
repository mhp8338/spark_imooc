package streaming.spark

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import streaming.utils.{ParamsConf, RedisPool}

/**
  * Author: Michael PK   QQ: 1990218038
  * Spark Streaming处理Kafka的数据
  */
object StreamingApp {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StreamingApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val stream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](ParamsConf.topic, ParamsConf.kafkaParams)
    )

//    stream.map(x => x.value()).print()
    /**
      * 统计每天付费成功的总订单数
      * 统计每天付费成功的总订单金额
      */
    stream.foreachRDD(rdd => {

      // flag fee time
      val data = rdd.map(x => JSON.parseObject(x.value()))
      data.cache()

      /**
        * wc  rdd.flatMap(_.split(",)).map((_,1)).reduceByKey(_+_)
        */
      // 每天付费成功的总订单数：day flag=1
      data.map(x => {
        val time = x.getString("time")
        val day = time.substring(0,8)
        val flag = x.getString("flag")
        val flagResult = if(flag == "1") 1 else 0
        (day, flagResult)
      }).reduceByKey(_+_).foreachPartition(partition => {
        val jedis = RedisPool.getJedis()
        partition.foreach(x => {
          jedis.incrBy("ImoocCount-"+x._1, x._2)
        })
      })

      // 每天付费成功的总订单金额
      data.map(x => {
        val time = x.getString("time")
        val day = time.substring(0,8)
        val flag = x.getString("flag")
        val fee = if(flag == "1") x.getString("fee").toLong else 0
        (day, fee)
      }).reduceByKey(_+_).foreachPartition(partition => {
        val jedis = RedisPool.getJedis()
        partition.foreach(x => {
          jedis.incrBy("ImoocFee-"+x._1, x._2)
        })
      })

      data.unpersist(true)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
