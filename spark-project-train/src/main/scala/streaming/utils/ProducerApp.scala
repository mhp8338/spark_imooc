package streaming.utils

import java.util
import java.util.{Date, Properties, UUID}

import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

/**
  * Author: Michael PK   QQ: 1990218038
  *
  * Kafka数据生产者
  */
object ProducerApp {

  def main(args: Array[String]): Unit = {

    val props = new Properties
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("bootstrap.servers", ParamsConf.brokers)
    props.put("request.required.acks","1")

    val topic = ParamsConf.topic
    val producer = new KafkaProducer[String,String](props)

    val random = new Random()
    val dateFormat = FastDateFormat.getInstance("yyyyMMddHHmmss")

    for(i <- 1 to 100){
      val time = dateFormat.format(new Date())+""
      val userid = random.nextInt(1000)+""
      val courseid = random.nextInt(500)+""
      val fee = random.nextInt(400)+""
      val result = Array("0","1") // 0未成功支付，1成功支付
      val flag = result(random.nextInt(2))
      var orderid = UUID.randomUUID().toString

      val map = new util.HashMap[String, Object]()
      map.put("time", time)
      map.put("userid",userid)
      map.put("courseid",courseid)
      map.put("fee", fee)
      map.put("flag", flag)
      map.put("orderid",orderid)

      val json = new JSONObject(map)

      producer.send(new ProducerRecord[String,String](topic(0),json+""))
    }


    println("PK Kafka生产者生产数据完毕...")
  }

}
