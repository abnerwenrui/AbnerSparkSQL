package com.abner.sparksql.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * @author peiwenrui
  * @since 2018-12-17 17:49
  */
object MySQLUtils {

  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/abnerProject?user=root&password=rootroot&useSSL=false")
  }

  def releaseConn(connection: Connection, preparedStatement: PreparedStatement): Unit = {
    try {
      if (preparedStatement != null) {
        preparedStatement.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      try {
        if (connection != null) {
          connection.close()
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }

  }

  def main(args: Array[String]): Unit = {
    var conn:Connection = null
    var pstmt: PreparedStatement = null

    try {
      conn = MySQLUtils.getConnection()

//      conn.setAutoCommit(false)
    val sql = "insert into day_video_access_topn_stat (day,cms_id,times) values (?,?,?)"

//      for(ele <- list){
//        println(ele.times+"--"+ele.cmsId+"--"+ele.day)

//      }

      pstmt.execute("insert into day_video_access_topn_stat (day,cms_id,times) values (12,12,12)")
    }catch {
      case e:Exception => println(e.printStackTrace())
    }finally {
      MySQLUtils.releaseConn(conn,pstmt)
    }
  }
}
