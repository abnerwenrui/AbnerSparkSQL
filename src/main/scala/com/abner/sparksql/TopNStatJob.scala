package com.abner.sparksql

import com.abner.sparksql.dao.StatDao
import com.abner.sparksql.domain.DayVideoAccessStat
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._

/**
  * @author peiwenrui
  * @since 2018-12-17 17:12
  */
object TopNStatJob {


  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[2]").appName("abner")
      //        .config("spark.sql.sources.partitionColumnTypeInference.enabled","true")
      .getOrCreate()

    //"part-00000-0adb19df-516d-4866-82e8-89ac68f0aa74.snappy.parquet"

    val accessDF = sparkSession.read.format("parquet").load("result/part-00000-0adb19df-516d-4866-82e8-89ac68f0aa74.snappy.parquet")

    accessDF.show()

//    videoAccessTopNStat(sparkSession, accessDF)
    cityAccessTopNStat(sparkSession, accessDF)
    sparkSession.stop()

  }


  def videoAccessTopNStat(sparkSession: SparkSession, accessDF: DataFrame) = {
    //    import sparkSession.implicits._
    //    val df = accessDF.filter($"day" === "20171110" && $"cmsType" === "code").groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)
    //    df.show()
    try {
      accessDF.createOrReplaceTempView("data_table")

      val videoAccessTopNDF = sparkSession.sql("select day, cmsId, count(1) as times  from data_table where day = '20171110' and cmsType = 'code' " +
        "group by day,cmsId order by times desc")


      videoAccessTopNDF.show()

      videoAccessTopNDF.foreachPartition(partitionOfRecords => {

        val list = new ListBuffer[DayVideoAccessStat]

        partitionOfRecords.foreach(item => {
          list.append(DayVideoAccessStat(item.getAs[String]("day"), item.getAs[Long]("cmsId"), item.getAs[Long]("times")))
        })
        if (list.nonEmpty) {
          StatDao.insertDayVideoAccessTopN(list)
        }
      })
    } catch {
      case e: Exception => println(e.printStackTrace())
    }
  }


  def cityAccessTopNStat(sparkSession: SparkSession, accessDF: DataFrame) = {
    try {
      import sparkSession.implicits._
      accessDF.filter($"day" === "20171110" && $"cmsType" === "code").groupBy("day","city","cmsId").agg(count("cmsId").as("times")).show()

      accessDF.createOrReplaceTempView("data")

      sparkSession.sql("select day,city,cmsId,count(1) as times from data where day='20171110' and cmsType = 'code' group by day,city,cmsId").show()

    } catch {
      case e: Exception => println(e.printStackTrace())
    }
  }
}
