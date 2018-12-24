package com.abner.sparksql

import com.abner.sparksql.utils.DateUtils
import org.apache.spark.sql.SparkSession

/**
  * @author peiwenrui
  * @since 2018-12-14 10:04
  */
object SparkStatFormatJob {

  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("abner.spark.sql").master("local[3]").getOrCreate()

    val df = sc.sparkContext.textFile("src/access.log")

    df.map(line => {
      val splits = line.split(" ")
      val ip = splits(0)
      val time = DateUtils.parse(splits(3) + " " + splits(4))
      val url = splits(11).replace("\"", "")
      val traffic = splits(9)
      //      (ip, time,url,traffic)
      time + "\t" + url + "\t" + traffic + "\t" + ip
    }).saveAsTextFile("output/")


    sc.stop()
  }

}
