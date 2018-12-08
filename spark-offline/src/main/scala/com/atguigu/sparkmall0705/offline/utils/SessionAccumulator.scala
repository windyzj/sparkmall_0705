package com.atguigu.sparkmall0705.offline.utils

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class SessionAccumulator extends  AccumulatorV2[String,mutable.HashMap[String,Long]]{

  var sessionMap=new mutable.HashMap[String,Long]()

  //判断是否是初始值
  override def isZero: Boolean = {
    sessionMap.isEmpty
  }

  //制作拷贝
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
     val accumulator = new SessionAccumulator()
     accumulator.sessionMap++=sessionMap
     accumulator
  }

  //
  override def reset(): Unit = {
    sessionMap=new mutable.HashMap[String,Long]()
  }

  //累加
  override def add(key: String): Unit = {
    sessionMap(key)= sessionMap.getOrElse(key,0L)+1L
  }

  //合并
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
          val otherMap: mutable.HashMap[String, Long] = other.value
    sessionMap = sessionMap.foldLeft(otherMap) { case (otherMap, (key, count)) =>
      otherMap(key) = otherMap.getOrElse(key, 0L) + count
      otherMap
    }

  }

  //得到当前值
  override def value: mutable.HashMap[String, Long] = {
    sessionMap
  }
}
