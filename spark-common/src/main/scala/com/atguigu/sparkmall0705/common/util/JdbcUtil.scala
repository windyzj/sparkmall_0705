package com.atguigu.sparkmall0705.common.util

import java.sql.PreparedStatement
import java.util.Properties
import javax.sql.DataSource

import com.alibaba.druid.pool.DruidDataSourceFactory
import com.atguigu.sparkmall0705.common.ConfigUtil


object JdbcUtil {

  var dataSource:DataSource=init()

  def init()={
    val properties = new Properties()
    val config = ConfigUtil("config.properties").config

    properties.setProperty("driverClassName","com.mysql.jdbc.Driver")
    properties.setProperty("url",config.getString("jdbc.url"))
    properties.setProperty("username",config.getString("jdbc.user"))
    properties.setProperty("password",config.getString("jdbc.password"))
    properties.setProperty("maxActive","20")

    DruidDataSourceFactory.createDataSource(properties)

  }

  def executeUpdate(sql: String, params: Array[Any]): Int = {  // "insert into xxx values (?,?,?)"
    var rtn = 0
    var pstmt: PreparedStatement = null
    val connection = dataSource.getConnection
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)

      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      rtn = pstmt.executeUpdate()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace
    }
    rtn
  }

  def executeBatchUpdate(sql: String, paramsList: Iterable[Array[Any]]): Array[Int] = {
        var rtn:Array[Int]=null
        var pstmt: PreparedStatement = null
        val connection = dataSource.getConnection
        try {
          connection.setAutoCommit(false)
          pstmt = connection.prepareStatement(sql)
          for (params <- paramsList ) {
            if (params != null && params.length > 0) {
              for (i <- 0 until params.length) {
                pstmt.setObject(i + 1, params(i))
              }
              pstmt.addBatch()
            }
          }
      rtn = pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace
    }
    rtn
  }


  def main(args: Array[String]): Unit = {
    JdbcUtil.executeBatchUpdate("insert into page_convert_rate values(?,?,?)",Array(Array("1","1_2",1.2),Array("2","2_3",0.8)))
  }


}
