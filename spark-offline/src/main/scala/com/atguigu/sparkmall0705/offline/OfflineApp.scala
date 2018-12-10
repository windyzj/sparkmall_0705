package com.atguigu.sparkmall0705.offline

import java.text.SimpleDateFormat
import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall0705.common.ConfigUtil
import com.atguigu.sparkmall0705.common.model.UserVisitAction
import com.atguigu.sparkmall0705.common.util.JdbcUtil
import com.atguigu.sparkmall0705.offline.bean.{CategoryTopN, SessionInfo}
import com.atguigu.sparkmall0705.offline.utils.{CategoryActionCountAccumulator, SessionAccumulator}
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
    println(s"userSessionCount = ${userSessionCount}")
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


    //需求二 按比例抽取session
    val sessionExtractRDD: RDD[SessionInfo] = SessionExtractApp.sessionExtract(userSessionCount ,taskId,userSessionRDD)

    import sparkSession.implicits._
    val  config: FileBasedConfiguration = ConfigUtil("config.properties").config
    sessionExtractRDD.toDF.write.format("jdbc")
        .option("url",config.getString("jdbc.url"))
        .option("user",config.getString("jdbc.user"))
        .option("password",config.getString("jdbc.password"))
          .option("dbtable","random_session_info")
      .mode(SaveMode.Append).save()

 ///////////////////////////////////////////////////////////////////////////
///////////////////////////需求三/////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////
    //需求三  根据点击、下单、支付进行排序，取前十名
    //    1 、遍历所有的访问日志
    //    map
    //    2 按照cid+操作类型进行分别累加
    //    累加器 hashMap[String ,Long]
    val categoryActionCountAccumulator = new CategoryActionCountAccumulator()
    sparkSession.sparkContext.register(categoryActionCountAccumulator)
    userActionRDD.foreach{ userAction=>
        if(userAction.click_category_id!= -1L){
          categoryActionCountAccumulator.add(userAction.click_category_id+ "_click")
          //给当前品类的点击(商品浏览)项进行加1
        }else if(userAction.order_category_ids!=null&&userAction.order_category_ids.size!=0){
          //由于订单涉及多个品类，所以用逗号切分 ，循环进行累加
          val orderCidArr: Array[String] = userAction.order_category_ids.split(",")

          for(orderCid<-orderCidArr){
             categoryActionCountAccumulator.add(orderCid+ "_order")
           }
          //
        }else if(userAction.pay_category_ids!=null&&userAction.pay_category_ids.size!=0) {
          //由于支付涉及多个品类，所以用逗号切分 ，循环进行累加
          val payCidArr: Array[String] = userAction.pay_category_ids.split(",")
          for(payCid<-payCidArr){
            categoryActionCountAccumulator.add(payCid+ "_pay")
          }
        }
    }
    //4 把结果转 cid,clickcount,ordercount,paycount
    //    CategoryTopN（click ,  hashMap.get(cid+“_click”), hashMap.get(cid+“_order”), hashMap.get(cid+“_pay”) ）

    //      3  得到累加的结果  map[cid_actiontype,count]
    //    累加器 hashMap[String ,Long]
    println(categoryActionCountAccumulator.value.mkString("\n"))
    val actionCountByCidMap: Map[String, mutable.HashMap[String, Long]] = categoryActionCountAccumulator.value.groupBy { case (cidAction, count) => //定义用什么来分组
      val cid: String = cidAction.split("_")(0)
      cid //用cid进行分组
    }
    //      4 把结果转 cid,clickcount,ordercount,paycount
    //    CategoryTopN（click ,  hashMap.get(cid+“_click”), hashMap.get(cid+“_order”), hashMap.get(cid+“_pay”) ）

    val categoryTopNList: List[CategoryTopN] = actionCountByCidMap.map { case (cid, actionMap) =>
      CategoryTopN(taskId, cid, actionMap.getOrElse(cid + "_click", 0L), actionMap.getOrElse(cid + "_order", 0L), actionMap.getOrElse(cid + "_pay", 0L))
    }.toList


    val categoryTop10: List[CategoryTopN] = categoryTopNList.sortWith((ctn1, ctn2) =>
      if (ctn1.click_count > ctn2.click_count) {
        true
      } else if (ctn1.click_count == ctn2.click_count) {
        if (ctn1.order_count > ctn2.order_count) {
          true
        } else {
          false
        }
      } else {
        false
      }
    ).take(10)
    println(categoryTop10.mkString("\n"))
   //组合成jdbcUtil插入需要的结构
    val categoryList = new ListBuffer[Array[Any]]()
    for ( categoryTopN<- categoryTop10 ) {
      val paramArray = Array(categoryTopN.taskId,categoryTopN.category_id,categoryTopN.click_count,categoryTopN.order_count,categoryTopN.pay_count)
      categoryList.append(paramArray)
    }
//保存到数据库
    JdbcUtil.executeBatchUpdate("insert into category_top10 values (?,?,?,?,?) " ,categoryList)


//    1 、遍历所有的访问日志
//    map
//    2 按照cid+操作类型进行分别累加
//    累加器 hashMap[String ,Long]
//      3  得到累加的结果  map[cid_actiontype,count]
//    累加器 hashMap[String ,Long]
//      4 把结果转 cid,clickcount,ordercount,paycount
//    CategoryTopN（click ,  hashMap.get(cid+“_click”), hashMap.get(cid+“_order”), hashMap.get(cid+“_pay”) ）
//    5 按照点击，下单，支付的次序进行排序   List[CatagoryTopN] .sortWith( function)
//    6 截取前十  .take(10)
//    7 保存到数据库中




    /////////////////需求四

    CategorySessionTopApp.statCategorySession(config,sparkSession,taskId,userActionRDD,categoryTop10)

    println("需求四保存完成！")
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
