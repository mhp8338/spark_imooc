package batch

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.SparkSession

/**
  * Author: Michael PK 
  *
  * 测试HBase和Spark整合使用的兼容性
  */
object TestApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("TestApp").master("local[2]").getOrCreate()

//    val rdd = spark.sparkContext.parallelize(List(1,2,3,4))
//    rdd.collect().foreach(println)

    var logDF = spark.read.format("com.imooc.bigdata.spark.pk").option("path","/Users/rocky/IdeaProjects/imooc-workspace/spark-project-train/src/data/test-access.log")
      .load()

    logDF.show(false)

//    /**
//      * Spark SQL自定义函数的使用
//      */
//    import org.apache.spark.sql.functions._
//    def formatTime() = udf((time:String) =>{
//      FastDateFormat.getInstance("yyyyMMddHHmm").format(
//        new Date(FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
//        .parse(time.substring(time.indexOf("[")+1, time.lastIndexOf("]"))).getTime
//      ))
//    })
//
//    // 在已有的DF之上添加或者修改字段
//    logDF = logDF.withColumn("formattime", formatTime()(logDF("time")))
//
//    logDF.show(false)

    spark.stop()
  }

}
