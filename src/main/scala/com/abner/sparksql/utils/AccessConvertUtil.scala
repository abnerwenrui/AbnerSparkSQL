package com.abner.sparksql.utils


import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * @author peiwenrui
  * @since 2018-12-14 16:03
  */
object AccessConvertUtil {
  val struct = StructType(
    Array(
      StructField("url",StringType),
      StructField("cmsType",StringType),
      StructField("cmsId",LongType),
      StructField("traffic",LongType),
      StructField("ip",StringType),
      StructField("city",StringType),
      StructField("time",StringType),
      StructField("day",StringType)
    )
  )

  def parseLog(log: String) = {
    try {
      val splits = log.split("\t")
      val url = splits(1)
      val traffic = splits(2).toLong
      val ip = splits(3)

      val domain = "http://www.abner.com/"
      var cms = ""
      if (url.length > domain.length) {
        cms = url.substring(url.indexOf(domain) + domain.length)
      }
      var cmsType = ""
      var cmsId = 0L
      val cmsTypeId = cms.split("/")

      if (cmsTypeId.length > 1) {
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }

      val city = IpUtils.getCity(ip)
      val time = splits(0)
      val day = time.substring(0, 10).replaceAll("-", "")

      Row(url, cmsType, cmsId, traffic, ip, city, time, day)
    } catch {
      case e: Exception => {
        println(e)
        Row(0)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val unit = parseLog("2016-11-10 00:01:02\t-\t0\t10.100.0.1")

    println(parseLog("2017-11-10 10:44:22\thttp://www.abner.com/code/1852\t633\t117.35.88.11"))
    println(unit)
  }

}
