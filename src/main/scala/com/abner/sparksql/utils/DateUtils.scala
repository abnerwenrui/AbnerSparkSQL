package com.abner.sparksql.utils

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期格式化类
  *
  * @author peiwenrui
  * @since 2018-12-14 11:03
  */
object DateUtils {


  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")


  def parse(time: String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def getTime(time: String) = {
    try {
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
    }catch {
      case e:Exception =>{
        println("解析错误:%s",e)
        0L
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val l = parse("[10/Nov/2016:00:01:02 +0800]")
  println(l)
  }

}
