package com.abner.sparksql.utils

import com.ggstar.util.ip.IpHelper

/**
  * @author peiwenrui
  * @since 2018-12-17 11:30
  */
object IpUtils {

  def getCity(ip:String)={
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {

    println(getCity("10.100.0.1"))
    println(getCity("117.35.88.11"))
  }

}
