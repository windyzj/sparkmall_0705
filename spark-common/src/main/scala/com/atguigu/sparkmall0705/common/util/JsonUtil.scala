package com.atguigu.sparkmall0705.common.util

import com.alibaba.fastjson.JSON
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object JsonUtil {
  def main(args: Array[String]): Unit = {
    val arr:List[String] = List("12","32")
    //println(JSON.toJSONString(arr))


   // println(compact(render(arr)))   //Jvalue  相当于 java工具JsonObject

    //把json串 变成scala对象
     val json: String = compact(render(arr))
    implicit val formats = DefaultFormats
    val list: List[String] = parse(json).extract[List[String]]

    println(list. mkString("-"))
  }

 /* def toJSONString(any:List[()]): Unit ={
       compact(render(any))
   }*/

}
