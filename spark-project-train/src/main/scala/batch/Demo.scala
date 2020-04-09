package batch

import org.apache.spark.sql.SparkSession

object Demo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ImoocLogApp").master("local[2]").getOrCreate()

    //System.setProperty("icode","xxxxxx")
    var logDF = spark.read.format("com.imooc.bigdata.spark.pk").option("path","/Users/rocky/IdeaProjects/imooc-workspace/spark-project-train/src/data/test-access.log")
      .load()

    logDF.show()

    spark.stop()
  }
}
