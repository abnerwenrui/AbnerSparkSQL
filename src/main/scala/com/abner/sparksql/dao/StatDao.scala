package com.abner.sparksql.dao

import java.sql.{Connection, PreparedStatement}

import com.abner.sparksql.domain.DayVideoAccessStat
import com.abner.sparksql.utils.MySQLUtils

import scala.collection.mutable.ListBuffer

/**
  * @author peiwenrui
  * @since 2018-12-18 15:27
  */
object StatDao {

  /**
    * 批量保存
    *
    * @param list
    */
  def insertDayVideoAccessTopN(list: ListBuffer[DayVideoAccessStat]): Unit = {
    var conn: Connection = null
    var pstmt: PreparedStatement = null

    try {
      conn = MySQLUtils.getConnection()

      conn.setAutoCommit(false)
      val sql = "insert into day_video_access_topn_stat (day,cms_id,times) values (?,?,?)"
      pstmt = conn.prepareStatement(sql)


      for(ele <- list){
        println(ele.times+"--"+ele.cmsId+"--"+ele.day)
        pstmt.setObject(1,ele.day)
        pstmt.setObject(2,ele.cmsId)
        pstmt.setObject(3,ele.times)
        pstmt.addBatch()
      }

      pstmt.executeBatch()
      conn.commit()
    }catch {
      case e:Exception => println(e.printStackTrace())
    }finally {
      MySQLUtils.releaseConn(conn,pstmt)
    }

  }

}
