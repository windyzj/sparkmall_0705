package com.atguigu.sparkmall0705.offline

import java.text.SimpleDateFormat
import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall0705.common.ConfigUtil
import com.atguigu.sparkmall0705.common.model.UserVisitAction
import com.atguigu.sparkmall0705.common.util.JdbcUtil
import com.atguigu.sparkmall0705.offline.utils.SessionAccumulator
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object OfflineApp {

  def main(args: Array[String]): Unit = {
       val sparkconf: SparkConf = new SparkConf().setAppName("offline").setMaster("local[*]")
       val sparkSession: SparkSession = SparkSession.builder().config(sparkconf).enableHiveSupport().getOrCreate()
    val accumulator = new SessionAccumulator()
      sparkSession.sparkContext.register(accumulator)
      val taskId: String = UUID.randomUUID().toString
    val conditionConfig: FileBasedConfiguration = ConfigUtil("conditions.properties").config
    val conditionJsonString: String = conditionConfig.getString("condition.params.json")

//    1 \ 筛选  要关联用户  sql   join user_info  where  contidition  =>DF=>RDD[UserVisitAction]
    val userActionRDD: RDD[UserVisitAction] = readUserVisitActionRDD(sparkSession,conditionJsonString)
//      2  rdd=>  RDD[(sessionId,UserVisitAction)] => groupbykey => RDD[(sessionId,iterable[UserVisitAction])]
    val userSessionRDD: RDD[(String, Iterable[UserVisitAction])] = userActionRDD.map(userAction=>(userAction.session_id,userAction)).groupByKey()
    userSessionRDD.cache()

    //      3 求 session总数量，
    val userSessionCount: Long = userSessionRDD.count()  //总数

    //    遍历一下全部session，对每个session的类型进行判断 来进行分类的累加  （累加器）
    //    4  分类 ：时长 ，把session里面的每个action进行遍历 ，取出最大时间和最小事件 ，求差得到时长 ，再判断时长是否大于10秒
    //    步长： 计算下session中有多少个action, 判断个数是否大于5
    //
    userSessionRDD.foreach{  case (sessionId,actions)  =>
        var maxActionTime= -1L
        var minActionTime= Long.MaxValue
        for( action<- actions){
               val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
               val actionTimeMillSec: Long = format.parse(action.action_time).getTime
               maxActionTime=Math.max(maxActionTime,actionTimeMillSec)
               minActionTime=Math.min(minActionTime,actionTimeMillSec)
        }
         val visitTime: Long = maxActionTime-minActionTime
         if(visitTime>10000){
           accumulator.add("session_visitLength_gt_10_count")
           //累加
         }else {
           //累加
           accumulator.add("session_visitLength_le_10_count")
         }
         if(actions.size>5){
           //累加
           accumulator.add("session_stepLength_gt_5_count")
         }else {
           //累加
           accumulator.add("session_stepLength_le_5_count")
         }
    }

    userSessionRDD

    //5 提取累加器中的值
    val sessionCountMap: mutable.HashMap[String, Long] = accumulator.value

    println(sessionCountMap.mkString(","))
    //6 把累计值计算为比例
    val session_visitLength_gt_10_ratio=  Math.round(  1000.0*sessionCountMap("session_visitLength_gt_10_count")/userSessionCount)/10.0
    val session_visitLength_le_10_ratio=  Math.round(1000.0* sessionCountMap("session_visitLength_le_10_count")/userSessionCount)/10.0
    val session_stepLength_gt_5_ratio=  Math.round(1000.0* sessionCountMap("session_stepLength_gt_5_count")/userSessionCount)/10.0
    val session_stepLength_le_5_ratio=  Math.round(1000.0* sessionCountMap("session_stepLength_le_5_count")/userSessionCount)/10.0

     val resultArray=  Array(taskId,conditionJsonString,userSessionCount,session_visitLength_gt_10_ratio,session_visitLength_le_10_ratio,session_stepLength_gt_5_ratio,session_stepLength_le_5_ratio)
    //7 保存到mysql中
    JdbcUtil.executeUpdate("insert into session_stat_info values (?,?,?,?,?,?,?) " ,resultArray)


    //需求 按比例抽取session
    val sessionExtractRDD: RDD[SessionInfo] = SessionExtractApp.sessionExtract(userSessionCount ,taskId,userSessionRDD)

    import sparkSession.implicits._
    val  config: FileBasedConfiguration = ConfigUtil("config.properties").config
    sessionExtractRDD.toDF.write.format("jdbc")
        .option("url",config.getString("jdbc.url"))
        .option("user",config.getString("jdbc.user"))
        .option("password",config.getString("jdbc.password"))
          .option("dbtable","random_session_info")
      .mode(SaveMode.Append).save()

  }


  def readUserVisitActionRDD(sparkSession: SparkSession,conditionJsonString:String):RDD[UserVisitAction]={
    //1 查库  2 条件
    val config: FileBasedConfiguration = ConfigUtil("config.properties").config
    val databaseName: String = config.getString("hive.database")
    sparkSession.sql("use "+databaseName)

    val jSONObject: JSONObject = JSON.parseObject(conditionJsonString)
    jSONObject.getString("startDate")

    //sql    //left join ->翻译   inner join 过滤
    var sql =   new StringBuilder("select v.* from user_visit_action v join user_info u on v.user_id=u.user_id  where 1=1")

    if(jSONObject.getString("startDate")!=null){
         sql.append(" and  date>='"+jSONObject.getString("startDate")+"'")
    }
    if(jSONObject.getString("endDate")!=null){
      sql.append(" and  date<='"+jSONObject.getString("endDate")+"'")
    }
    if(jSONObject.getString("startAge")!=null){
      sql.append(" and  u.age>="+jSONObject.getString("startAge"))
    }
    if(jSONObject.getString("endAge")!=null){
      sql.append(" and  u.age<="+jSONObject.getString("endAge"))
    }
    if(!jSONObject.getString("professionals").isEmpty){
      sql.append(" and  u.professional in ("+jSONObject.getString("professionals") +")")
    }
    println(sql)
    import sparkSession.implicits._
    val rdd: RDD[UserVisitAction] = sparkSession.sql(sql.toString()).as[UserVisitAction].rdd

    rdd
  }

}
