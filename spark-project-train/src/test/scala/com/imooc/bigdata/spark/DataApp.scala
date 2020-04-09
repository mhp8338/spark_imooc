package com.imooc.bigdata.spark

import batch.ImoocLogApp

/**
  * Author: Michael PK 
  */
object DataApp {

  def main(args: Array[String]): Unit = {

    val url = "GET /course/list?c=cb HTTP/1.1"
    val referer = "https://www.imooc.com/course/list?c=data"
    val ip = "110.85.18.234"
    val rowkey = ImoocLogApp.getRowKey("20190130",url+referer+ip)

    println(rowkey)

  }

}
