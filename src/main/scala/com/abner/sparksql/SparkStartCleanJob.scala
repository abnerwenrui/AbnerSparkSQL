package com.abner.sparksql

import com.abner.sparksql.utils.AccessConvertUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author peiwenrui
  * @since 2018-12-14 12:18
  */
object SparkStartCleanJob {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("sparkStartCleanJob").master("local[2]").getOrCreate()

    val cleanDataRDD = sc.sparkContext.textFile("src/access2.log")

    val dataFrame = sc.createDataFrame(cleanDataRDD.map(x => AccessConvertUtil.parseLog(x)), AccessConvertUtil.struct)

//    dataFrame.printSchema()
//
//    dataFrame.show()
    dataFrame.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).save("/Users/peiwenrui/jd/AbnerSpark/AbnerSparkSQL/result")

    sc.stop()
  }
}
