package com.atguigu.sparkmall0705.realtime.app

import com.atguigu.sparkmall0705.common.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import redis.clients.jedis.Jedis
object AreaTop3AdsPerdayApp {

  def calcTop3Ads(areaCityAdsTotalCountDstream:DStream[(String, Long)]): Unit ={
//    RDD[day_area_city_adsId,count]

//    RDD[(day_area_adsId,count)]=>不同城市但是地区一样的进行聚合（reducebykey）
    val areaAdsTotalCountDstream: DStream[(String, Long)] = areaCityAdsTotalCountDstream.map { case (area_city_adsId_day, count) =>
      val keyArr: Array[String] = area_city_adsId_day.split(":")
      val area: String = keyArr(0)
      val adsId: String = keyArr(2)
      val day: String = keyArr(3)
      val area_adsId_day: String = area + ":" + adsId + ":" + day
      (area_adsId_day, count)
    }.reduceByKey(_ + _)



//    => RDD[(day_area_adsId,count)]
//    => RDD[(day,(area,(adsId,count)))]  =>groupbykey
    val areaAdsIdGroupbyDayDStream: DStream[(String, Iterable[(String, (String, Long))])] = areaAdsTotalCountDstream.map { case (area_adsId_day, count) =>
      val keyArr: Array[String] = area_adsId_day.split(":")
      val area: String = keyArr(0)
      val adsId: String = keyArr(1)
      val day: String = keyArr(2)
      (day, (area, (adsId, count)))
    }.groupByKey()
    //=> RDD[(day,iterable(area,(adsId,count)))]  要聚合成以area为key的广告计数集合
    val areaTop3adsPerDayDStream: DStream[(String, Map[String, String])] = areaAdsIdGroupbyDayDStream.map { case (daykey, areaItr) =>
      //    =>  iterable(area,(adsId,count))->groupby  ->  Map[area,iterable[area,(adsId,count)]]
      val adsIdCountGroupbyArea: Map[String, Iterable[(String, (String, Long))]] = areaItr.groupBy { case (area, (adsId, count)) => area }
      // iterable[area,(adsId,count)]-> iterable[(adsId,count)]->sort ->take(3)-> Json
      val areaTop3AdsJsonMap: Map[String, String] = adsIdCountGroupbyArea.map { case (area, areaAdsIterable) =>
        //adsIterable.toMap.mapValues() //待尝试
        //把里边的（adsid,count)取出来 ，把多余的area去掉
        val adsCountItr: Iterable[(String, Long)] = areaAdsIterable.map { case (area, (adsId, count)) => (adsId, count) }
        val top3AdsCountList: List[(String, Long)] = adsCountItr.toList.sortWith { (adscount1, adscount2) => adscount1._2 > adscount2._2 }.take(3)
        //转json  利用json4s
        val top3AdsCountJsonString: String = compact(render(top3AdsCountList))
        (area, top3AdsCountJsonString)
      }
      (daykey, areaTop3AdsJsonMap)

    }
    areaTop3adsPerDayDStream
    //可以存盘

    areaTop3adsPerDayDStream.foreachRDD{rdd=>
      rdd.foreachPartition{areaTop3adsPerDayItr=>
        val jedis: Jedis = RedisUtil.getJedisClient
        for ((daykey,areaMap) <- areaTop3adsPerDayItr ) {
         import   collection.JavaConversions._
          jedis.hmset("top3_ads_per_day:"+daykey,areaMap)
        }
        jedis.close()
      }


    }




//
//    =>
//    => RDD[(day,Map[area,map[adsId,count]])]
//    =>RDD[(day,Map[area,json])]
//    Map[day,Map[area,jsonString]]




  }

}
