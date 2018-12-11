package com.atguigu.sparkmall0705.offline

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall0705.common.model.UserVisitAction
import com.atguigu.sparkmall0705.common.util.JdbcUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PageConvertRatioApp {

  def main(args: Array[String]): Unit = {
    println(Array(1, 2, 3).slice(0, 2).mkString(","))
  }

  def calcPageConvertRatio(sparkSession: SparkSession,conditionJsonString:String, taskId:String ,userActionRDD:RDD[UserVisitAction]): Unit ={
//    1 转化率的公式：  两个页面跳转的次数 /  前一个页面的访问次数
//    2   1，2, 3,4,5,6,7  ->        1-2,2-3,3-4,4-5,5-6,6-7次数  /  1,2,3,4,5,6次数
    //取得要计算的跳转页面
    //
    val pageVisitArray: Array[String] = JSON.parseObject(conditionJsonString).getString("targetPageFlow").split(",")  //1,2,3,4,5,6,7
    //      3  可以用zip 1-6 zip 2-7 ->  (1,2),(2,3),…….-> 1-2,2-3,3-4,4-5,5-6,6-7
     val pageJumpTupleArray: Array[(String, String)] = pageVisitArray.slice(0,pageVisitArray.length-1).zip(pageVisitArray.slice(1,pageVisitArray.length)) //(1,2),(2,3),(3,4)....
    val targetVisitPageBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(pageVisitArray.slice(0,pageVisitArray.length-1))


    val targetPageJumpArray: Array[String] =pageJumpTupleArray.map{case (page1,page2) =>page1+"-"+page2}////1-2,2-3,3-4,4-5,5-6,6-7
    val targetPageJumpsBC: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(targetPageJumpArray)


//    4 前一个页面的访问次数  1-6页面的访问次数  ->取pageid为1-6的访问次数
    val filteredUserActionRDD: RDD[UserVisitAction] = userActionRDD.filter { userAction =>
      targetVisitPageBC.value.contains(userAction.page_id.toString)
    }
   //  pageid为1-6的访问记录 -> 按照pageid进行count-> countbykey  Map[key,count]
    val pageVisitCountMap: collection.Map[Long, Long] = filteredUserActionRDD.map(userAction=>(userAction.page_id,1L)).countByKey()

//
//    5  两个页面跳转的次数
//      根据sessionId进行聚合   RDD[sessionId,iterable[UserVisitAction]]
    val userActionsBySessionRDD: RDD[(String, Iterable[UserVisitAction])] = userActionRDD.map(userAction=>(userAction.session_id,userAction)).groupByKey()

    //    =>针对每个session的访问记录按时间排序 sortwith=>List[pageId]// 1,2,4,5,6,7,8,12,13,16
    //    1-13 zip 2-16 -> 1-2,2-4,4-5,5-6……->(1-2,1L)-> 进行一次过滤 -> reducebykey  (1-2,300L),(2-3,120L)….
    //    =》   1-2,2-3,3-4,4-5,5-6,6-7次数  Map[key,count]
    val pageJumpsRDD: RDD[String] = userActionsBySessionRDD.flatMap { case (sessionId, itrActions) =>
      //按照时间从小到大排序
      val userActionSortedList: List[UserVisitAction] = itrActions.toList.sortWith((action1, action2) => action1.action_time < action2.action_time)
      //把访问转换成页面的跳转： 1-2,2-3，....

      val pageVisitList: List[Long] = userActionSortedList.map(userAction => userAction.page_id)
      val pageJumpList: List[String] = pageVisitList.slice(0, pageVisitList.length - 1).zip(pageVisitList.slice(1, pageVisitList.length)).map { case (pageId1, pageId2) => pageId1 + "-" + pageId2 }
      pageJumpList //1-2,2-3,3-8,8-10
    }
    val filteredPageJumpRDD: RDD[String] = pageJumpsRDD.filter(pageJumps =>
      targetPageJumpsBC.value.contains(pageJumps)
    )
    val pageJumpCountMap: collection.Map[String, Long] = filteredPageJumpRDD.map{pageJump=>(pageJump,1L)}.countByKey()  //得到的是每个跳转的 计数


    println(pageVisitCountMap.mkString("\n"))
    println(pageJumpCountMap.mkString("\n"))


//    6 两个map 分别进行除法得到 转化率

    val pageConvertRatios: Iterable[Array[Any]] = pageJumpCountMap.map { case (pageJumps, count) =>
      val prefixPageId: String = pageJumps.split("-")(0)
      val prefixPageCount: Long = pageVisitCountMap.getOrElse(prefixPageId.toLong, 0L)
      val ratio: Double = Math.round(count / prefixPageCount.toDouble * 1000) / 10.0
      Array(taskId, pageJumps, ratio)
    }



//      7 存库
    JdbcUtil.executeBatchUpdate("insert into page_convert_rate values(?,?,?)",pageConvertRatios)




  }


}
