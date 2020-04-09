package sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val data = sc.parallelize(List("米家激光投影电视4K套装 17999 1000",
      "小米充电宝 200 10",
      "小米云台监控 200 1000",
      "米兔 9 2000"), 1)

    // TODO... 先按照商品的价格降序排列，如果价格相同，按照库存降序排序

    // 自定义排序的第一种方式：通过元组来实现
//    data.map(x => {
//      val splits = x.split(" ")
//      val name = splits(0)
//      val price = splits(1).toInt
//      val store = splits(2).toInt
//      (name, price, store)
//    }).sortBy(x => (-x._2, -x._3)).foreach(println)


    val products: RDD[ProductClass] = data.map(x => {
      val splits = x.split(" ")
      val name = splits(0)
      val price = splits(1).toInt
      val store = splits(2).toInt
      new ProductClass(name, price, store)
      //ProductCaseClass(name, price, store)
    })


    implicit def productClass2Ordered(productClass: ProductClass):Ordered[ProductClass] = new Ordered[ProductClass]{
      override def compare(that: ProductClass): Int = {
        that.price - productClass.price
      }
    }

    products.sortBy(x => x).foreach(println)


    /**
      * 什么隐式转换：偷偷摸摸的给某个类的方法增强
      */
    sc.stop()
  }
}

case class ProductCaseClass (val name:String, val price:Int, val store:Int)
  extends Ordered[ProductCaseClass] {
  override def compare(that: ProductCaseClass): Int = {
    that.price - this.price
  }
}

// 普通的不能再普通的一个class，不带比较规则，如何进行排序
class ProductClass(val name:String, val price:Int, val store:Int)
 extends Serializable{

  override def toString = s"ProductClass($name, $price, $store)"
}
