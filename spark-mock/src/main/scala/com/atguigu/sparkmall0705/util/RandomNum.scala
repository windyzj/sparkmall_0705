package com.atguigu.sparkmall0705.util

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object RandomNum {

  def apply(fromNum:Int,toNum:Int): Int =  {10
    fromNum+ new Random().nextInt(toNum-fromNum+1)
  }
  def multi(fromNum:Int,toNum:Int,amount:Int,delimiter:String,canRepeat:Boolean) ={
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
    //用delimiter分割  canRepeat为false则不允许重复

    //List.mkString(",")
    if(canRepeat){
      val valueList = new ListBuffer[Int]()
      for( i <- 1 to amount){
            val randomNum = apply(fromNum ,toNum )
            valueList+=randomNum
      }
      valueList.mkString(",")
    }else {
        val valueSet = new mutable.HashSet[Int]()
        while(valueSet.size<amount){
          val randomNum = apply(fromNum ,toNum )
          valueSet+=randomNum
        }
        valueSet.mkString(",")
    }

   }


  def main(args: Array[String]): Unit = {
    println(multi(1, 5, 3, ",", false))
  }

}

