package com.atguigu.sparkmall0705.offline.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap

class CityRemarkUDAF  extends  UserDefinedAggregateFunction{

  //定义输入的结构
  override def inputSchema: StructType = StructType(Array(StructField("city_name",StringType)))
  //存放累计值   //两个累计值  一个map[city_name,count]  另一个 total_count
  override def bufferSchema: StructType = StructType(Array(StructField("city_count",MapType(StringType,LongType)),StructField("total_count",LongType)))
  // 定义谁出值结构    //打印结果的类型
  override def dataType: DataType = StringType
  //校验 一致性   相同的输入有相同的输出 true
  override def deterministic: Boolean = true
  // 初始化buffer
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=new HashMap[String,Long]
    buffer(1)=0L
  }
  // 更新  每进入一条数据，进行累加
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val city_name: String = input.getString(0)
    if(city_name==null ||city_name.isEmpty){
      return
    }
    val cityCountMap: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
    val totalCount: Long = buffer.getLong(1)
   // "abc"+"d" = "abcd""
    // 用原有的map 和 新的键值对组合  产生新的map
    val cityCountMapNew: Map[String, Long] = cityCountMap+ (  city_name -> (cityCountMap.getOrElse(city_name,0L)+1L))
    buffer(0)=cityCountMapNew
    buffer(1)= totalCount+1L
  }
  //  合并， 把不同分区的结果进行合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val cityCountMap1: Map[String, Long] = buffer1.getAs[Map[String,Long]](0)
    val totalCount1: Long = buffer1.getLong(1)

    val cityCountMap2: Map[String, Long] = buffer2.getAs[Map[String,Long]](0)
    val totalCount2: Long = buffer2.getLong(1)

    //合并map
    val cityCountMapNew: Map[String, Long] = cityCountMap1.foldLeft(cityCountMap2) { case (cityCountMap2, (city_name, count)) =>

      cityCountMap2 + (city_name -> (cityCountMap2.getOrElse(city_name, 0L) + count))
    }
    buffer1(0)=cityCountMapNew
    //合并总数
    buffer1(1)=totalCount1+totalCount2

  }
  //  输出 展示出结果
  override def evaluate(buffer: Row): Any = {
    //把累计结果取出
    val cityCountMap: Map[String, Long] = buffer.getAs[Map[String,Long]](0)
    val totalCount: Long = buffer.getLong(1)
     // 取出所有城市的集合  =》 List(CityCountInfo)
    val cityInfoList: List[CityCountInfo] = cityCountMap.map{case (city_name,count)=>CityCountInfo(city_name,count, Math.round(count/totalCount.toDouble*1000)/10)}.toList
    //排序//截取前2
    var top2CityCountList: List[CityCountInfo] = cityInfoList.sortWith((cityInfo1,cityInfo2)=> cityInfo1.cityCount>cityInfo2.cityCount).take(2)

    //计算出其他
    if(cityInfoList.size>2){
      var otherRatio =100D
      top2CityCountList.foreach(cityInfo=> otherRatio -= cityInfo.cityRatio)
       val cityInfoWithOtherList: List[CityCountInfo] = top2CityCountList:+CityCountInfo("其他",0L,otherRatio)
      cityInfoWithOtherList.mkString(",")
    }else {
      top2CityCountList.mkString(",")
    }

  }

  case class CityCountInfo(cityName:String,cityCount:Long ,cityRatio:Double){
    override def toString: String = {
      cityName+":"+cityRatio+"%"
    }
  }
}
