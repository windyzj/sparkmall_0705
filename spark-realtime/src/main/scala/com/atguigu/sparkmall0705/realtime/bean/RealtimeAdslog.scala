package com.atguigu.sparkmall0705.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

case class RealtimeAdslog (logdate:Date,area:String,city:String,userId:String,adsId:String){
    def  getDateTimeString() ={
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      format.format(logdate)
    }
  def  getDateString()={
    val format = new SimpleDateFormat("yyyy-MM-dd")
    format.format(logdate)
  }
}
