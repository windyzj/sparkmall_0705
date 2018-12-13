package com.atguigu.sparkmall0705.realtime.app

import com.atguigu.sparkmall0705.common.util.RedisUtil
import com.atguigu.sparkmall0705.realtime.bean.RealtimeAdslog
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import redis.clients.jedis.Jedis

object LastHourCountPerAds {

  def calcLastHourCountPerAds(filteredRealtimelogDStream:DStream[RealtimeAdslog]): Unit ={
//    RDD[Realtimelog]=> window=>map=>  利用滑动窗口去最近一小时的日志
    val lastHourLogDStream: DStream[RealtimeAdslog] = filteredRealtimelogDStream.window(Minutes(60),Seconds(10))

//      RDD[(adsid_hour_min,1L)] -> reducebykey  //按照每个广告+小时分钟进行统计个数
    val lastHourAdsCountDstream: DStream[(String, Long)] = lastHourLogDStream.map { realtimelog =>
      val hourminuStr: String = realtimelog.getHourMinuString()
      val adsId: String = realtimelog.adsId
      val key: String = adsId +"_"+ hourminuStr
      (key, 1L)
    }.reduceByKey(_ + _)

    //    =>RDD[adsid_hour_min,count]     按广告id进行聚合，把本小时内相同广告id的计数聚到一起
    //    =>RDD[(adsId,(hour_min,count)] .groupbykey
    //    => RDD[adsId,Iterable[(hour_min,count)]]
    val hourMinuCountGroupAdsDStream: DStream[(String, Iterable[(String, Long)])] = lastHourAdsCountDstream.map { case (adsId_hourMin, count) =>
      val adsIdHourMinArr: Array[String] = adsId_hourMin.split("_")
      val adsId: String = adsIdHourMinArr(0)
      val hourMinu: String = adsIdHourMinArr(1)
      (adsId, (hourMinu, count))
    }.groupByKey()

    //RDD[adsId,iterable[(hourMinu,count)]]  //把小时分钟的计数变成json
    val hourminuJsonPerAdsDStream: DStream[(String, String)] = hourMinuCountGroupAdsDStream.map { case (adsId, hourMinuItr) =>
      val hourMinuList: List[(String, Long)] = hourMinuItr.toList
      val hourMinuJson: String = compact(render(hourMinuList))
      (adsId, hourMinuJson)
    }
    //保存到redis中
    val jedis: Jedis = RedisUtil.getJedisClient
    hourminuJsonPerAdsDStream.foreachRDD(rdd=> {
      val hourMinuJsonPerAds: Array[(String, String)] = rdd.collect()
      import collection.JavaConversions._
      jedis.hmset("last_hour_ads_click",hourMinuJsonPerAds.toMap)
    }

    )





//    RDD[adsId,hourminCountJson]
//    map(adsid,json)

  }

}
