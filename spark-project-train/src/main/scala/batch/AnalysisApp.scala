package batch

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * Author: Michael PK 
  * 使用Spark对HBase中的数据做统计分析操作
  *
  * 1） 统计每个国家每个省份的访问量
  * 2） 统计不同浏览器的访问量
  */
object AnalysisApp extends Logging {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("AnalysisApp").master("local[2]").getOrCreate()

    // 获取要进行统计分析的日期
    val day = "20190130"

    // 连接HBase
    val conf = new Configuration()
    conf.set("hbase.rootdir", "hdfs://hadoop000:8020/hbase")
    conf.set("hbase.zookeeper.quorum", "hadoop000:2181")

    val tableName = "access_" + day
    conf.set(TableInputFormat.INPUT_TABLE, tableName) // 要从哪个表里面去读取数据

    val scan = new Scan()

    // 设置要查询的cf
    scan.addFamily(Bytes.toBytes("o"))

    // 设置要查询的列
    scan.addColumn(Bytes.toBytes("o"), Bytes.toBytes("country"))
    scan.addColumn(Bytes.toBytes("o"), Bytes.toBytes("province"))

    scan.addColumn(Bytes.toBytes("o"), Bytes.toBytes("browsername"))

    // 设置Scan
    conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

    // 通过Spark的newAPIHadoopRDD读取数据
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    //    hbaseRDD.take(10).foreach(x => {
    //      val rowKey = Bytes.toString(x._1.get())
    //
    //      for(cell <- x._2.rawCells()) {
    //        val cf = Bytes.toString(CellUtil.cloneFamily(cell))
    //        val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
    //        val value = Bytes.toString(CellUtil.cloneValue(cell))
    //
    //        println(s"$rowKey : $cf : $qualifier : $value")
    //      }
    //    })

    /**
      * Spark优化中最常用的一个点：Cache
      */

    hbaseRDD.cache()

    //TODO...  统计每个国家每个省份的访问量  ==> TOP10
//    hbaseRDD.map(x => {
//      val country = Bytes.toString(x._2.getValue("o".getBytes, "country".getBytes))
//      val province = Bytes.toString(x._2.getValue("o".getBytes, "province".getBytes))
//
//      ((country, province), 1)
//    }).reduceByKey(_ + _)
//      .map(x => (x._2, x._1)).sortByKey(false) // (hello,3)=>(3,hello)=>(hello,3)
//      .map(x => (x._2, x._1)).take(10).foreach(println)
    logError("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    import spark.implicits._
    hbaseRDD.map(x => {
      val country = Bytes.toString(x._2.getValue("o".getBytes, "country".getBytes))
      val province = Bytes.toString(x._2.getValue("o".getBytes, "province".getBytes))

      CountryProvince(country, province)
    }).toDF.select("country","province")
      .groupBy("country","province").count().show(10,false)

    logError("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    hbaseRDD.map(x => {
      val browsername = Bytes.toString(x._2.getValue("o".getBytes, "browsername".getBytes))
      (browsername, 1)
    }).reduceByKey(_ + _)
      .map(x => (x._2, x._1)).sortByKey(false)
      .map(x => (x._2, x._1)).foreach(println)
    logError("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

//    hbaseRDD.map(x => {
//      val browsername = Bytes.toString(x._2.getValue("o".getBytes, "browsername".getBytes))
//      Browser(browsername)
//    }).toDF().select("browsername").groupBy("browsername").count().show(false)
//
//    logError("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    hbaseRDD.map(x => {
      val browsername = Bytes.toString(x._2.getValue("o".getBytes, "browsername".getBytes))
      Browser(browsername)
    }).toDF().createOrReplaceTempView("tmp")

    spark.sql("select browsername,count(1) cnt from tmp group by browsername order by cnt desc").show(false)

    hbaseRDD.unpersist(true)

    spark.stop()
  }

  case class CountryProvince(country: String, province: String)

  case class Browser(browsername: String)

}
