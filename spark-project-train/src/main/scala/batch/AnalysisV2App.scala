package batch

import java.sql.DriverManager

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

/**
  * Author: Michael PK 
  * 使用Spark对HBase中的数据做统计分析操作
  *
  * 1） 统计每个国家每个省份的访问量
  * 2） 统计不同浏览器的访问量
  */
object AnalysisV2App extends Logging {

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

    val resultRDD = hbaseRDD.map(x => {
      val browsername = Bytes.toString(x._2.getValue("o".getBytes, "browsername".getBytes))
      (browsername, 1)
    }).reduceByKey(_ + _)

    resultRDD.collect().foreach(println)


    resultRDD.coalesce(1).foreachPartition(part => {
      Try{
        // TODO... 将统计结果写入到MySQL
        val connection = {
          Class.forName("com.mysql.jdbc.Driver")
          val url = "jdbc:mysql://hadoop000:3306/spark?characterEncoding=UTF-8"
          val user = "root"
          val password = "root"
          DriverManager.getConnection(url, user, password)
        }

        val preAutoCommit = connection.getAutoCommit
        connection.setAutoCommit(false)

        val sql = "insert into browser_stat (day,browser,cnt) values(?,?,?)"
        val pstmt = connection.prepareStatement(sql)
        pstmt.addBatch(s"delete from browser_stat where day=$day")

        part.foreach(x => {
          pstmt.setString(1, day)
          pstmt.setString(2, x._1)
          pstmt.setInt(3,  x._2)

          pstmt.addBatch()
        })

        pstmt.executeBatch()
        connection.commit()

        (connection, preAutoCommit)
      } match {
        case Success((connection, preAutoCommit)) => {
          connection.setAutoCommit(preAutoCommit)
          if(null != connection) connection.close()
        }
        case Failure(e) => throw e
      }



    })

    spark.stop()
  }

  case class CountryProvince(country: String, province: String)

  case class Browser(browsername: String)

}
