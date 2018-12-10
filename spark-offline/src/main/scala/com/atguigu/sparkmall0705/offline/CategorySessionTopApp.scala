package com.atguigu.sparkmall0705.offline

import com.atguigu.sparkmall0705.common.model.UserVisitAction
import com.atguigu.sparkmall0705.offline.bean.{CategorySessionTop, CategoryTopN}
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object CategorySessionTopApp {

  def  statCategorySession(config:FileBasedConfiguration, sparkSession: SparkSession,taskId:String ,userActionRDD:RDD[UserVisitAction],categoryTop10List: List[CategoryTopN]): Unit ={

    val categoryTop10BC: Broadcast[List[CategoryTopN]] = sparkSession.sparkContext.broadcast(categoryTop10List)

//    1、	过滤: 过滤出所有排名前十品类的action=>RDD[UserVisitAction]
    val filteredUserActionRDD: RDD[UserVisitAction] = userActionRDD.filter { userAction =>
      var matchflag = false
      for (categoryTop <- categoryTop10BC.value) {
        if (userAction.click_category_id.toString == categoryTop.category_id) matchflag = true
      }
      matchflag
    }

//      2 相同的cid+sessionId进行累加计数
    val cidSessionIdCountRDD: RDD[(String, Long)] = filteredUserActionRDD.map { userAction =>
      (userAction.click_category_id + "_" + userAction.session_id, 1L)
    }.reduceByKey(_ + _)



//      RDD[userAction.clickcid+userAction.sessionId,1L]
//    reducebykey(_+_)  =>RDD[cid_sessionId,count]
//    3、根据cid进行聚合
//    RDD[cid_sessionId,count]
//    =>RDD[cid,(sessionId,count)]
//    => groupbykey => RDD[cid,Iterable[(sessionId,clickCount)]]
    val sessionCountByCidRDD: RDD[(String, Iterable[(String, Long)])] = cidSessionIdCountRDD.map { case (cid_sessionId, count) =>
      val cidSessionArray: Array[String] = cid_sessionId.split("_")
      val cid: String = cidSessionArray(0)
      val sessionId: String = cidSessionArray(1)
      (cid, (sessionId, count))
    }.groupByKey()

//    4 、聚合后进行排序、截取
//    =>RDD[cid,Iterable[(sessionId,clickCount)]]
//    =>把iterable 进行排序截取前十=>RDD[cid,Iterable[(sessionId,clickCount)]]
//    5、 结果转换成对象然后 存储到数据库中
    val categorpSessionTopRDD: RDD[CategorySessionTop] = sessionCountByCidRDD.flatMap { case (cid, itrSessionCount) =>
      val sessionCountTop10: List[(String, Long)] = itrSessionCount.toList.sortWith { (sessionCount1, sessionCount2) =>
        sessionCount1._2 > sessionCount2._2
      }.take(10)
      val categorySessionTopList: List[CategorySessionTop] = sessionCountTop10.map { case (sessionId, count) =>
        CategorySessionTop(taskId, cid, sessionId, count)
      }
      categorySessionTopList
    }
    println(categorpSessionTopRDD.collect().mkString("\n"))

    import sparkSession.implicits._
    categorpSessionTopRDD.toDF.write.format("jdbc")
      .option("url",config.getString("jdbc.url"))
      .option("user",config.getString("jdbc.user"))
      .option("password",config.getString("jdbc.password"))
      .option("dbtable","category_top10_session_count")
      .mode(SaveMode.Append).save()

//    =>RDD[CategorySessionTop]=>存数据库

  }

}
