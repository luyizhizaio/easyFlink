package com.ecommerce

case class Entity()

//埋点日志
case class UserBehavior(userId:Long,itemId:Long,category:Int,behavior:String,timestamp:Long)

//apache日志
case class ApacheLogEvent(ip:String,userId:String,eventTime:Long,method:String,url:String)


//937166,1715,beijing,beijing,1511661711
case class AdClickLog(userId:Long,adId:Long,province:String,city:String,timestamp:Long)

//23064,66.249.3.15,fail,1558430826
case class LoginLog(userId:Long,ip:String,state:String,timestamp:Long)

//34731,pay,35jue34we,1558430849
case class OrderItem(orderId:Long,operType:String,txId:String, eventTime:Long)

//ewr342as4,wechat,1558430845
case class ReceiptLog(txId:String,channel:String,eventTime:Long)
