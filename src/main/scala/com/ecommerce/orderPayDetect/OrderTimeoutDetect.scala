package com.ecommerce.orderPayDetect

import java.util

import com.ecommerce.OrderItem
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 检测订单是否在15分钟内支付
 */

case class PayResult(orderId:Long,msg:String)

object OrderTimeoutDetect {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("data/ecom/OrderLog.csv")
      .map{line =>
        val arr = line.split(",")
        OrderItem(arr(0).trim.toLong,arr(1).trim,arr(2).trim,arr(3).trim.toLong)
      }
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId)


    //cep
    //1.定义pattern
    val orderPayPattern = Pattern.begin[OrderItem]("begin").where(_.operType =="create")
      .followedBy("follow").where(_.operType =="pay")
      .within(Time.minutes(15))

    //2.找出pattern stream
    val patternStream = CEP.pattern(dataStream,orderPayPattern)

    //定义side out 标签 接收超时定点
    val sideOutputTag = new OutputTag[PayResult]("payTimeout")

    val payStream = patternStream.select(sideOutputTag,
    new PayTimeOutSelect(), //timeout select function 处理timeout的订单
    new OrderPaySelect())  //处理正常支付订单


    payStream.print("payed")

    //获取超时订单
    val timeOutPayStream = payStream.getSideOutput(sideOutputTag)
    timeOutPayStream.print("timeout")

    env.execute()

  }



}
//处理超时15分钟没有支付的订单
class PayTimeOutSelect extends PatternTimeoutFunction[OrderItem,PayResult]{
  override def timeout(map: util.Map[String, util.List[OrderItem]], timeoutTimestamp: Long): PayResult = {
    //map 中保存超时订单
    val timeOutOrder = map.get("begin").iterator().next()

    PayResult(timeOutOrder.orderId,"payed timeout")
  }
}

class OrderPaySelect extends PatternSelectFunction[OrderItem,PayResult]{
  override def select(map: util.Map[String, util.List[OrderItem]]): PayResult = {

    val order = map.get("follow").iterator().next()

    PayResult(order.orderId,"payed successfully")
  }
}
