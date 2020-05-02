package com.ecommerce

case class Entity()

//埋点日志
case class UserBehavior(userId:Long,itemId:Long,category:Int,behavior:String,timestamp:Long)

//apache日志
case class ApacheLogEvent(ip:String,userId:String,eventTime:Long,method:String,url:String)


//937166,1715,beijing,beijing,1511661711
case class AdClickLog(userId:Long,adId:Long,province:String,city:String,timestamp:Long)
