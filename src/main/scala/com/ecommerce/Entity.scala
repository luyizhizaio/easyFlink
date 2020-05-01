package com.ecommerce

case class Entity()

//埋点日志
case class UserBehavior(userId:Long,itemId:Long,category:Int,behavior:String,timestamp:Long)

//apache日志
case class ApacheLogEvent(ip:String,userId:String,eventTime:Long,method:String,url:String)
